#!/usr/bin/env python
#! -*- encoding: UTF-8 -*-

# Copyright (c) 2002-2015 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from io import BytesIO
from struct import pack as struct_pack, unpack as struct_unpack
import sys

if sys.version_info >= (3,):
    BYTES_TYPE = bytes
    TEXT_TYPE = str
    INTEGER_TYPE = int
else:
    INTEGER_TYPE = (int, long)
    BYTES_TYPE = str
    TEXT_TYPE = unicode

__all__ = ["Packer", "pack", "packb", "Unpacker", "unpack", "unpackb"]

PLUS_2_TO_THE_63 = 2 ** 63
PLUS_2_TO_THE_32 = 4294967296
PLUS_2_TO_THE_31 = 2147483648
PLUS_2_TO_THE_16 = 65536
PLUS_2_TO_THE_15 = 32768
PLUS_2_TO_THE_8 = 256
PLUS_2_TO_THE_7 = 128
PLUS_2_TO_THE_4 = 16
MINUS_2_TO_THE_4 = -16
MINUS_2_TO_THE_7 = -128
MINUS_2_TO_THE_15 = -32768
MINUS_2_TO_THE_31 = -2147483648
MINUS_2_TO_THE_63 = -(2 ** 63)

ENCODING = "UTF-8"

DOUBLE_STRUCT = ">d"
UINT_8_STRUCT = ">B"
UINT_16_STRUCT = ">H"
UINT_32_STRUCT = ">I"
INT_8_STRUCT = ">b"
INT_16_STRUCT = ">h"
INT_32_STRUCT = ">i"
INT_64_STRUCT = ">q"

TINY_TEXT = [bytes(bytearray([x])) for x in range(0x80, 0x90)]
TINY_LIST = [bytes(bytearray([x])) for x in range(0x90, 0xA0)]
TINY_MAP = [bytes(bytearray([x])) for x in range(0xA0, 0xB0)]
TINY_STRUCT = [bytes(bytearray([x])) for x in range(0xB0, 0xC0)]

NULL = b"\xC0"
FLOAT_64 = b"\xC1"
FALSE = b"\xC2"
TRUE = b"\xC3"
INT_8 = b"\xC8"
INT_16 = b"\xC9"
INT_32 = b"\xCA"
INT_64 = b"\xCB"
BYTES_8 = b"\xCC"
BYTES_16 = b"\xCD"
BYTES_32 = b"\xCE"
TEXT_8 = b"\xD0"
TEXT_16 = b"\xD1"
TEXT_32 = b"\xD2"
LIST_8 = b"\xD4"
LIST_16 = b"\xD5"
LIST_32 = b"\xD6"
MAP_8 = b"\xD8"
MAP_16 = b"\xD9"
MAP_32 = b"\xDA"
STRUCT_8 = b"\xDC"
STRUCT_16 = b"\xDD"

PACKED_UINT_8 = [struct_pack(UINT_8_STRUCT, value) for value in range(PLUS_2_TO_THE_8)]
PACKED_UINT_16 = [struct_pack(UINT_16_STRUCT, value) for value in range(PLUS_2_TO_THE_16)]
PACKED_INT_8 = {value: struct_pack(INT_8_STRUCT, value) 
                for value in range(MINUS_2_TO_THE_7, PLUS_2_TO_THE_7)}
PACKED_INT_16 = {value: struct_pack(INT_16_STRUCT, value)
                 for value in range(MINUS_2_TO_THE_15, PLUS_2_TO_THE_15)}

UNPACKED_UINT_8 = {bytes(bytearray([x])): x for x in range(PLUS_2_TO_THE_8)}
UNPACKED_UINT_16 = {struct_pack(UINT_16_STRUCT, x): x for x in range(PLUS_2_TO_THE_16)}
UNPACKED_INT_8 = {value: key for key, value in PACKED_INT_8.items()}
UNPACKED_INT_16 = {value: key for key, value in PACKED_INT_16.items()}

UNPACKED_MARKERS = {NULL: None, TRUE: True, FALSE: False}
UNPACKED_MARKERS.update({bytes(bytearray([z])): z for z in range(0, PLUS_2_TO_THE_7)})
UNPACKED_MARKERS.update({bytes(bytearray([z + 256])): z for z in range(MINUS_2_TO_THE_4, 0)})


class UnlimitedList(list):
    capacity = 1e309  # infinity


class List(list):

    def __init__(self, capacity):
        self.capacity = capacity


class Map(dict):

    def __init__(self, capacity):
        self.capacity = capacity
        self.__key = NotImplemented

    def append(self, item):
        key = self.__key
        if key is NotImplemented:
            self.__key = item
        else:
            self[key] = item
            self.__key = NotImplemented
        return key


class Structure(list):

    def __init__(self, capacity, signature):
        self.capacity = capacity
        self.signature = signature

    def __repr__(self):
        return repr(tuple(iter(self)))

    def __eq__(self, other):
        return list(self) == list(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __iter__(self):
        yield self.signature
        yield tuple(super(Structure, self).__iter__())


class Packer(object):

    def __init__(self, stream):
        self.stream = stream

    def pack_raw(self, data):
        self.stream.write(data)

    def pack(self, value):
        stream = self.stream

        # None
        if value is None:
            stream.write(NULL)

        # Boolean
        elif value is True:
            stream.write(TRUE)
        elif value is False:
            stream.write(FALSE)

        # Float (only double precision is supported)
        elif isinstance(value, float):
            stream.write(FLOAT_64)
            stream.write(struct_pack(DOUBLE_STRUCT, value))

        # Integer
        elif isinstance(value, INTEGER_TYPE):
            if MINUS_2_TO_THE_4 <= value < PLUS_2_TO_THE_7:
                stream.write(PACKED_INT_8[value])
            elif MINUS_2_TO_THE_7 <= value < MINUS_2_TO_THE_4:
                stream.write(INT_8)
                stream.write(PACKED_INT_8[value])
            elif MINUS_2_TO_THE_15 <= value < PLUS_2_TO_THE_15:
                stream.write(INT_16)
                stream.write(PACKED_INT_16[value])
            elif MINUS_2_TO_THE_31 <= value < PLUS_2_TO_THE_31:
                stream.write(INT_32)
                stream.write(struct_pack(INT_32_STRUCT, value))
            elif MINUS_2_TO_THE_63 <= value < PLUS_2_TO_THE_63:
                stream.write(INT_64)
                stream.write(struct_pack(INT_64_STRUCT, value))
            else:
                raise OverflowError("Integer %s out of range" % value)

        # Bytes
        elif isinstance(value, BYTES_TYPE):
            self.pack_bytes_header(len(value))
            self.pack_raw(value)

        # Text
        elif isinstance(value, TEXT_TYPE):
            value_bytes = value.encode(ENCODING)
            self.pack_text_header(len(value_bytes))
            self.pack_raw(value_bytes)

        # List
        elif isinstance(value, list):
            self.pack_list_header(len(value))
            for item in value:
                self.pack(item)

        # Map
        elif isinstance(value, dict):
            self.pack_map_header(len(value))
            for key, item in value.items():
                self.pack(key)
                self.pack(item)

        # Structure
        elif isinstance(value, (Structure, tuple)):
            try:
                signature, fields = value
            except ValueError:
                raise ValueError("Structures require a 2-tuple of (signature, fields)")
            else:
                self.pack_struct_header(len(fields), signature)
                for field in fields:
                    self.pack(field)

        # Other
        else:
            raise ValueError("Values of type %s are not supported" % type(value))

    def pack_bytes_header(self, size):
        stream = self.stream
        if size < PLUS_2_TO_THE_8:
            stream.write(BYTES_8)
            stream.write(PACKED_UINT_8[size])
        elif size < PLUS_2_TO_THE_16:
            stream.write(BYTES_16)
            stream.write(PACKED_UINT_16[size])
        elif size < PLUS_2_TO_THE_32:
            stream.write(BYTES_32)
            stream.write(struct_pack(UINT_32_STRUCT, size))
        else:
            raise OverflowError("Bytes header size out of range")

    def pack_text_header(self, size):
        stream = self.stream
        if size < PLUS_2_TO_THE_4:
            stream.write(TINY_TEXT[size])
        elif size < PLUS_2_TO_THE_8:
            stream.write(TEXT_8)
            stream.write(PACKED_UINT_8[size])
        elif size < PLUS_2_TO_THE_16:
            stream.write(TEXT_16)
            stream.write(PACKED_UINT_16[size])
        elif size < PLUS_2_TO_THE_32:
            stream.write(TEXT_32)
            stream.write(struct_pack(UINT_32_STRUCT, size))
        else:
            raise OverflowError("Text header size out of range")

    def pack_list_header(self, size):
        stream = self.stream
        if size < PLUS_2_TO_THE_4:
            stream.write(TINY_LIST[size])
        elif size < PLUS_2_TO_THE_8:
            stream.write(LIST_8)
            stream.write(PACKED_UINT_8[size])
        elif size < PLUS_2_TO_THE_16:
            stream.write(LIST_16)
            stream.write(PACKED_UINT_16[size])
        elif size < PLUS_2_TO_THE_32:
            stream.write(LIST_32)
            stream.write(struct_pack(UINT_32_STRUCT, size))
        else:
            raise OverflowError("List header size out of range")

    def pack_map_header(self, size):
        stream = self.stream
        if size < PLUS_2_TO_THE_4:
            stream.write(TINY_MAP[size])
        elif size < PLUS_2_TO_THE_8:
            stream.write(MAP_8)
            stream.write(PACKED_UINT_8[size])
        elif size < PLUS_2_TO_THE_16:
            stream.write(MAP_16)
            stream.write(PACKED_UINT_16[size])
        elif size < PLUS_2_TO_THE_32:
            stream.write(MAP_32)
            stream.write(struct_pack(UINT_32_STRUCT, size))
        else:
            raise OverflowError("Map header size out of range")

    def pack_struct_header(self, size, signature):
        if isinstance(signature, bytes) and len(signature) == 1:
            stream = self.stream
            if size < PLUS_2_TO_THE_4:
                stream.write(TINY_STRUCT[size])
                stream.write(signature)
            elif size < PLUS_2_TO_THE_8:
                stream.write(STRUCT_8)
                stream.write(PACKED_UINT_8[size])
                stream.write(signature)
            elif size < PLUS_2_TO_THE_16:
                stream.write(STRUCT_16)
                stream.write(PACKED_UINT_16[size])
                stream.write(signature)
            else:
                raise OverflowError("Structure header size out of range")
        else:
            raise ValueError("Structure signature must be a single byte value")


def pack(stream, *values):
    for value in values:
        Packer(stream).pack(value)


def packb(*values):
    stream = BytesIO()
    pack(stream, *values)
    return stream.getvalue()


class Unpacker(object):

    def __init__(self, stream):
        self.stream = stream

    def unpack(self):
        current_collection = UnlimitedList()
        current_capacity = current_collection.capacity
        current_size = len(current_collection)
        push_item = current_collection.append

        collection_stack = []
        push_collection = collection_stack.append
        pop_collection = collection_stack.pop

        stream_read = self.stream.read
        while True:
            marker_byte = stream_read(1)

            if marker_byte == b"":
                break

            is_collection = False

            try:
                value = UNPACKED_MARKERS[marker_byte]  # NULL, TRUE, FALSE and TINY_INT

            except KeyError:
                marker = UNPACKED_UINT_8[marker_byte]
                marker_high = marker & 0xF0

                # Float
                if marker_byte == FLOAT_64:
                    value = struct_unpack(DOUBLE_STRUCT, stream_read(8))[0]

                # Integer
                elif marker_byte == INT_8:
                    value = UNPACKED_INT_8[stream_read(1)]
                elif marker_byte == INT_16:
                    value = UNPACKED_INT_16[stream_read(2)]
                elif marker_byte == INT_32:
                    value = struct_unpack(INT_32_STRUCT, stream_read(4))[0]
                elif marker_byte == INT_64:
                    value = struct_unpack(INT_64_STRUCT, stream_read(8))[0]

                # Bytes
                elif marker_byte == BYTES_8:
                    byte_size = UNPACKED_UINT_8[stream_read(1)]
                    value = stream_read(byte_size)
                elif marker_byte == BYTES_16:
                    byte_size = UNPACKED_UINT_16[stream_read(2)]
                    value = stream_read(byte_size)
                elif marker_byte == BYTES_32:
                    byte_size = struct_unpack(UINT_32_STRUCT, stream_read(4))[0]
                    value = stream_read(byte_size)

                # Text
                elif marker_high == 0x80:
                    value = stream_read(marker & 0x0F).decode(ENCODING)
                elif marker_byte == TEXT_8:
                    byte_size = UNPACKED_UINT_8[stream_read(1)]
                    value = stream_read(byte_size).decode(ENCODING)
                elif marker_byte == TEXT_16:
                    byte_size = UNPACKED_UINT_16[stream_read(2)]
                    value = stream_read(byte_size).decode(ENCODING)
                elif marker_byte == TEXT_32:
                    byte_size = struct_unpack(UINT_32_STRUCT, stream_read(4))[0]
                    value = stream_read(byte_size).decode(ENCODING)

                # List
                elif marker_high == 0x90:
                    value = List(marker & 0x0F)
                    is_collection = True
                elif marker_byte == LIST_8:
                    size = UNPACKED_UINT_8[stream_read(1)]
                    value = List(size)
                    is_collection = True
                elif marker_byte == LIST_16:
                    size = UNPACKED_UINT_16[stream_read(2)]
                    value = List(size)
                    is_collection = True
                elif marker_byte == LIST_32:
                    size = struct_unpack(UINT_32_STRUCT, stream_read(4))[0]
                    value = List(size)
                    is_collection = True

                # Map
                elif marker_high == 0xA0:
                    value = Map(marker & 0x0F)
                    is_collection = True
                elif marker_byte == MAP_8:
                    size = UNPACKED_UINT_8[stream_read(1)]
                    value = Map(size)
                    is_collection = True
                elif marker_byte == MAP_16:
                    size = UNPACKED_UINT_16[stream_read(2)]
                    value = Map(size)
                    is_collection = True
                elif marker_byte == MAP_32:
                    size = struct_unpack(UINT_32_STRUCT, stream_read(4))[0]
                    value = Map(size)
                    is_collection = True

                # Structure
                elif marker_high == 0xB0:
                    signature = stream_read(1)
                    value = Structure(marker & 0x0F, signature)
                    is_collection = True
                elif marker_byte == STRUCT_8:
                    size, signature = stream_read(2)
                    value = Structure(UNPACKED_UINT_8[size], signature)
                    is_collection = True
                elif marker_byte == STRUCT_16:
                    data = stream_read(3)
                    value = Structure(UNPACKED_UINT_16[data[0:2]], data[2])
                    is_collection = True

            appended = False
            while not appended:
                if current_size >= current_capacity:
                    current_collection = pop_collection()
                    current_capacity = current_collection.capacity
                    current_size = len(current_collection)
                    push_item = current_collection.append
                else:
                    if push_item(value) is not NotImplemented:
                        current_size += 1
                    if is_collection:
                        push_collection(current_collection)
                        current_collection = value
                        current_capacity = current_collection.capacity
                        current_size = len(current_collection)
                        push_item = current_collection.append
                    appended = True

        if collection_stack:
            return iter(collection_stack[0])
        else:
            return iter(current_collection)


def unpack(stream):
    unpacker = Unpacker(stream)
    for value in unpacker.unpack():
        yield value


def unpackb(b):
    return unpack(BytesIO(b))
