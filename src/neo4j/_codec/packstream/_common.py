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


from contextlib import contextmanager


try:
    from ..._rust.codec.packstream import Structure

    RUST_AVAILABLE = True
except ImportError:
    from ._python import Structure

    RUST_AVAILABLE = False


class PackableBuffer:
    def __init__(self):
        self.data = bytearray()
        # export write method for packer; "inline" for performance
        self.write = self.data.extend
        self.clear = self.data.clear
        self._tmp_buffering = 0

    @contextmanager
    def tmp_buffer(self):
        self._tmp_buffering += 1
        old_len = len(self.data)
        try:
            yield
        except Exception:
            del self.data[old_len:]
            raise
        finally:
            self._tmp_buffering -= 1

    def is_tmp_buffering(self):
        return bool(self._tmp_buffering)


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
        subview = view[self.p : q]
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
        """Pop last two bytes as a big-endian 16-bit unsigned integer."""
        if self.used >= 2:
            value = 0x100 * self.data[self.used - 2] + self.data[self.used - 1]
            self.used -= 2
            return value
        else:
            return -1


__all__ = [
    "RUST_AVAILABLE",
    "PackableBuffer",
    "Structure",
    "UnpackableBuffer",
]
