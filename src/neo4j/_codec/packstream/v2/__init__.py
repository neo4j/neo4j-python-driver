# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# ruff: noqa: RUF067 - might clean up in the future


from codecs import decode
from struct import (
    pack as struct_pack,
    unpack as struct_unpack,
)
from uuid import UUID

from ..._types import (
    BYTES_TYPES,
    FALSE_VALUES,
    FLOAT_TYPES,
    INT_TYPES,
    MAPPING_TYPES,
    NONE_VALUES,
    SEQUENCE_TYPES,
    TRUE_VALUES,
)
from .. import v1
from .._common import Structure


try:
    from ...._rust.codec.packstream.v2 import (
        pack as _rust_pack,
        unpack as _rust_unpack,
    )
except ImportError:
    _rust_pack = None
    _rust_unpack = None


PACKED_UINT_8 = v1.PACKED_UINT_8
PACKED_UINT_16 = v1.PACKED_UINT_16

UNPACKED_UINT_8 = v1.UNPACKED_UINT_8
UNPACKED_UINT_16 = v1.UNPACKED_UINT_16

INT64_MIN = v1.INT64_MIN
INT64_MAX = v1.INT64_MAX


class Packer(v1.Packer):
    if _rust_pack:

        def _pack(self, data, dehydration_hooks=None):
            data = _rust_pack(data, dehydration_hooks)
            self._write(data)
    else:

        def _pack(self, data, dehydration_hooks=None):
            self._py_pack(data, dehydration_hooks)

    def _py_pack(self, value, dehydration_hooks=None):
        write = self._write

        # None
        if any(value is v for v in NONE_VALUES):
            write(b"\xc0")  # NULL

        # Boolean
        elif any(value is v for v in TRUE_VALUES):
            write(b"\xc3")
        elif any(value is v for v in FALSE_VALUES):
            write(b"\xc2")

        # Float (only double precision is supported)
        elif isinstance(value, FLOAT_TYPES):
            write(b"\xc1")
            write(struct_pack(">d", value))

        # Integer
        elif isinstance(value, INT_TYPES):
            value = int(value)
            if -0x10 <= value < 0x80:
                write(PACKED_UINT_8[value % 0x100])
            elif -0x80 <= value < -0x10:
                write(b"\xc8")
                write(PACKED_UINT_8[value % 0x100])
            elif -0x8000 <= value < 0x8000:
                write(b"\xc9")
                write(PACKED_UINT_16[value % 0x10000])
            elif -0x80000000 <= value < 0x80000000:
                write(b"\xca")
                write(struct_pack(">i", value))
            elif INT64_MIN <= value < INT64_MAX:
                write(b"\xcb")
                write(struct_pack(">q", value))
            else:
                raise OverflowError(f"Integer {value} out of range")

        # String
        elif isinstance(value, str):
            encoded = value.encode("utf-8")
            self._pack_string_header(len(encoded))
            self._write(encoded)

        # Bytes
        elif isinstance(value, BYTES_TYPES):
            self._pack_bytes_header(len(value))
            self._write(value)

        # List
        elif isinstance(value, SEQUENCE_TYPES):
            self._pack_list_header(len(value))
            for item in value:
                self._py_pack(item, dehydration_hooks)

        # Map
        elif isinstance(value, MAPPING_TYPES):
            self._pack_map_header(len(value.keys()))
            for key, item in value.items():
                if not isinstance(key, str):
                    raise TypeError(
                        f"Map keys must be strings, not {type(key)}"
                    )
                self._py_pack(key, dehydration_hooks)
                self._py_pack(item, dehydration_hooks)

        # UUID
        elif isinstance(value, UUID):
            write(b"\xe0")
            write(value.bytes)

        # Structure
        elif isinstance(value, Structure):
            self.pack_struct(value.tag, value.fields)

        # Other if in dehydration hooks
        else:
            if dehydration_hooks:
                transformer = dehydration_hooks.get_transformer(value)
                if transformer is not None:
                    self._py_pack(transformer(value), dehydration_hooks)
                    return

            raise ValueError(f"Values of type {type(value)} are not supported")


class Unpacker(v1.Unpacker):
    if _rust_unpack:

        def unpack(self, hydration_hooks=None):
            value, i = _rust_unpack(
                self.unpackable.data, self.unpackable.p, hydration_hooks
            )
            self.unpackable.p = i
            return value
    else:

        def unpack(self, hydration_hooks=None):
            return self._unpack(hydration_hooks=hydration_hooks)

    def _unpack(self, hydration_hooks=None):
        marker = self.read_u8()

        if marker == -1:
            raise ValueError("Nothing to unpack")

        # Tiny Integer
        if 0x00 <= marker <= 0x7F:
            return marker
        elif 0xF0 <= marker <= 0xFF:
            return marker - 0x100

        # Null
        elif marker == 0xC0:
            return None

        # Float
        elif marker == 0xC1:
            (value,) = struct_unpack(">d", self.read(8))
            return value

        # Boolean
        elif marker == 0xC2:
            return False
        elif marker == 0xC3:
            return True

        # Integer
        elif marker == 0xC8:
            return struct_unpack(">b", self.read(1))[0]
        elif marker == 0xC9:
            return struct_unpack(">h", self.read(2))[0]
        elif marker == 0xCA:
            return struct_unpack(">i", self.read(4))[0]
        elif marker == 0xCB:
            return struct_unpack(">q", self.read(8))[0]

        # Bytes
        elif marker == 0xCC:
            (size,) = struct_unpack(">B", self.read(1))
            return self.read(size).tobytes()
        elif marker == 0xCD:
            (size,) = struct_unpack(">H", self.read(2))
            return self.read(size).tobytes()
        elif marker == 0xCE:
            (size,) = struct_unpack(">I", self.read(4))
            return self.read(size).tobytes()

        # UUID
        elif marker == 0xE0:
            return UUID(
                int=int.from_bytes(
                    self.read(16), byteorder="big", signed=False
                )
            )

        else:
            marker_high = marker & 0xF0
            # String
            if marker_high == 0x80:  # TINY_STRING
                return decode(self.read(marker & 0x0F), "utf-8")
            elif marker == 0xD0:  # STRING_8:
                (size,) = struct_unpack(">B", self.read(1))
                return decode(self.read(size), "utf-8")
            elif marker == 0xD1:  # STRING_16:
                (size,) = struct_unpack(">H", self.read(2))
                return decode(self.read(size), "utf-8")
            elif marker == 0xD2:  # STRING_32:
                (size,) = struct_unpack(">I", self.read(4))
                return decode(self.read(size), "utf-8")

            # List
            elif 0x90 <= marker <= 0x9F or 0xD4 <= marker <= 0xD6:
                return list(
                    self._unpack_list_items(
                        marker, hydration_hooks=hydration_hooks
                    )
                )

            # Map
            elif 0xA0 <= marker <= 0xAF or 0xD8 <= marker <= 0xDA:
                return self._unpack_map(
                    marker, hydration_hooks=hydration_hooks
                )

            # Structure
            elif 0xB0 <= marker <= 0xBF:
                size, tag = self._unpack_structure_header(marker)
                value = Structure(tag, *([None] * size))
                for i in range(len(value)):
                    value[i] = self._unpack(hydration_hooks=hydration_hooks)
                if not hydration_hooks:
                    return value
                hydration_hook = hydration_hooks.get(type(value))
                if not hydration_hook:
                    return value
                return hydration_hook(value)

            else:
                raise ValueError(f"Unknown PackStream marker {marker:02X}")
