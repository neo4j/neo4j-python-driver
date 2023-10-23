import json
from timeit import default_timer as timer
from fast_packstream import read as fpsr
from struct import (
    unpack as struct_unpack,
)
from codecs import decode


class Unpacker:

    def __init__(self, unpackable):
        self.unpackable = unpackable

    def reset(self):
        self.unpackable.reset()

    def read(self, n=1):
        return self.unpackable.read(n)

    def read_u8(self):
        return self.unpackable.read_u8()

    def unpack(self, hydration_hooks=None):
        value = self._unpack(hydration_hooks=hydration_hooks)
        if hydration_hooks and type(value) in hydration_hooks:
            return hydration_hooks[type(value)](value)
        return value

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
            value, = struct_unpack(">d", self.read(8))
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
            size, = struct_unpack(">B", self.read(1))
            return self.read(size).tobytes()
        elif marker == 0xCD:
            size, = struct_unpack(">H", self.read(2))
            return self.read(size).tobytes()
        elif marker == 0xCE:
            size, = struct_unpack(">I", self.read(4))
            return self.read(size).tobytes()

        else:
            marker_high = marker & 0xF0
            # String
            if marker_high == 0x80:  # TINY_STRING
                return decode(self.read(marker & 0x0F), "utf-8")
            elif marker == 0xD0:  # STRING_8:
                size, = struct_unpack(">B", self.read(1))
                return decode(self.read(size), "utf-8")
            elif marker == 0xD1:  # STRING_16:
                size, = struct_unpack(">H", self.read(2))
                return decode(self.read(size), "utf-8")
            elif marker == 0xD2:  # STRING_32:
                size, = struct_unpack(">I", self.read(4))
                return decode(self.read(size), "utf-8")

            # List
            elif 0x90 <= marker <= 0x9F or 0xD4 <= marker <= 0xD6:
                return list(self._unpack_list_items(
                    marker, hydration_hooks=hydration_hooks)
                )

            # Map
            elif 0xA0 <= marker <= 0xAF or 0xD8 <= marker <= 0xDA:
                return self._unpack_map(
                    marker, hydration_hooks=hydration_hooks
                )

            else:
                raise ValueError("Unknown PackStream marker %02X" % marker)

    def _unpack_list_items(self, marker, hydration_hooks=None):
        marker_high = marker & 0xF0
        if marker_high == 0x90:
            size = marker & 0x0F
            if size == 0:
                return
            elif size == 1:
                yield self.unpack(hydration_hooks=hydration_hooks)
            else:
                for _ in range(size):
                    yield self.unpack(hydration_hooks=hydration_hooks)
        elif marker == 0xD4:  # LIST_8:
            size, = struct_unpack(">B", self.read(1))
            for _ in range(size):
                yield self.unpack(hydration_hooks=hydration_hooks)
        elif marker == 0xD5:  # LIST_16:
            size, = struct_unpack(">H", self.read(2))
            for _ in range(size):
                yield self.unpack(hydration_hooks=hydration_hooks)
        elif marker == 0xD6:  # LIST_32:
            size, = struct_unpack(">I", self.read(4))
            for _ in range(size):
                yield self.unpack(hydration_hooks=hydration_hooks)
        else:
            return

    def unpack_map(self, hydration_hooks=None):
        marker = self.read_u8()
        return self._unpack_map(marker, hydration_hooks=hydration_hooks)

    def _unpack_map(self, marker, hydration_hooks=None):
        marker_high = marker & 0xF0
        if marker_high == 0xA0:
            size = marker & 0x0F
            value = {}
            for _ in range(size):
                key = self.unpack(hydration_hooks=hydration_hooks)
                value[key] = self.unpack(hydration_hooks=hydration_hooks)
            return value
        elif marker == 0xD8:  # MAP_8:
            size, = struct_unpack(">B", self.read(1))
            value = {}
            for _ in range(size):
                key = self.unpack(hydration_hooks=hydration_hooks)
                value[key] = self.unpack(hydration_hooks=hydration_hooks)
            return value
        elif marker == 0xD9:  # MAP_16:
            size, = struct_unpack(">H", self.read(2))
            value = {}
            for _ in range(size):
                key = self.unpack(hydration_hooks=hydration_hooks)
                value[key] = self.unpack(hydration_hooks=hydration_hooks)
            return value
        elif marker == 0xDA:  # MAP_32:
            size, = struct_unpack(">I", self.read(4))
            value = {}
            for _ in range(size):
                key = self.unpack(hydration_hooks=hydration_hooks)
                value[key] = self.unpack(hydration_hooks=hydration_hooks)
            return value
        else:
            return None

    def unpack_structure_header(self):
        marker = self.read_u8()
        if marker == -1:
            return None, None
        else:
            return self._unpack_structure_header(marker)

    def _unpack_structure_header(self, marker):
        marker_high = marker & 0xF0
        if marker_high == 0xB0:  # TINY_STRUCT
            signature = self.read(1).tobytes()
            return marker & 0x0F, signature
        else:
            raise ValueError("Expected structure, found marker %02X" % marker)

    @staticmethod
    def new_unpackable_buffer():
        return UnpackableBuffer()


class UnpackableBuffer:

    initial_capacity = 8192

    def __init__(self, data=None):
        if data is None:
            self.data = bytearray(self.initial_capacity)
            self.used = 0
        else:
            self.data = bytearray(data)
            self.used = len(self.data)
        self.p = 0

    def reset(self):
        self.used = 0
        self.p = 0

    def read(self, n=1):
        view = memoryview(self.data)
        q = self.p + n
        subview = view[self.p:q]
        self.p = q
        return subview

    def read_u8(self):
        if self.used - self.p >= 1:
            value = self.data[self.p]
            self.p += 1
            return value
        else:
            return -1

    def pop_u16(self):
        """ Remove the last two bytes of data, returning them as a big-endian
        16-bit unsigned integer.
        """
        if self.used >= 2:
            value = 0x100 * self.data[self.used - 2] + self.data[self.used - 1]
            self.used -= 2
            return value
        else:
            return -1


def main():
    with open("C:\\tmp\\test.bin", "rb") as file:
        data = bytearray(file.read())
    print(f"decoding:{len(data)} bytes.")
    print("Python.")
    start = timer()
    u = Unpacker(UnpackableBuffer(data)).unpack()
    end = timer()
    print_time(start, end)
    start = timer()
    (v, i) = fpsr(data, 0, None)
    end = timer()
    print("PyO3.")
    print_time(start, end)

    a = json.dumps(u)
    b = json.dumps(v)
    print(f"output matched: {a == b}.")


def print_time(start, end):
    ms = (end - start)
    print('Time elapsed: {}s'.format(ms))


if __name__ == '__main__':
    main()
