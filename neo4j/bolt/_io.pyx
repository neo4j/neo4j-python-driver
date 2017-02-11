#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2017 "Neo Technology,"
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


from struct import pack as struct_pack, unpack as struct_unpack

from neo4j.compat import memoryview_at


cdef _empty_view = memoryview(b"")


cdef class MessageFrame(object):

    cdef _view
    cdef list _panes
    cdef int _current_pane
    cdef int _current_offset

    def __cinit__(self, view, list panes):
        self._view = view
        self._panes = panes
        if panes:
            self._current_pane = 0
            self._current_offset = 0
        else:
            self._current_pane = -1
            self._current_offset = -1

    cdef _next_pane(self):
        self._current_pane += 1
        if self._current_pane < len(self._panes):
            self._current_offset = 0
        else:
            self._current_pane = -1
            self._current_offset = -1

    cpdef panes(self):
        return self._panes

    cpdef read_int(self):
        cdef int p
        cdef int q
        cdef int size
        cdef int value

        if self._current_pane == -1:
            return -1
        p, q = self._panes[self._current_pane]
        size = q - p
        value = memoryview_at(self._view, p + self._current_offset)
        self._current_offset += 1
        if self._current_offset == size:
            self._next_pane()
        return value

    cpdef read(self, int n):
        cdef int p
        cdef int q
        cdef int size
        cdef int start
        cdef int end
        cdef int remaining
        cdef bytearray value

        if n == 0 or self._current_pane == -1:
            return _empty_view
        p, q = self._panes[self._current_pane]
        size = q - p
        remaining = size - self._current_offset
        if n <= remaining:
            start = p + self._current_offset
            end = start + n
            if n < remaining:
                self._current_offset += n
            else:
                self._next_pane()
            return memoryview(self._view[start:end])
        start = p + self._current_offset
        end = q
        value = bytearray(self._view[start:end])
        self._next_pane()
        if len(value) < n and self._current_pane >= 0:
            value.extend(self.read(n - (end - start)))
        return value


cdef class ChunkedInputBuffer(object):

    cdef bytearray _data
    cdef _view
    cdef int _extent
    cdef int _origin
    cdef int _limit
    cdef MessageFrame _frame

    def __cinit__(self, capacity=524288):
        self._data = bytearray(capacity)
        self._view = memoryview(self._data)
        self._extent = 0    # end position of all loaded data
        self._origin = 0    # start position of current frame
        self._limit = -1    # end position of current frame
        self._frame = None  # frame object

    def __repr__(self):
        return repr(self.view().tobytes())

    cpdef capacity(self):
        return len(self._view)

    cpdef view(self):
        return memoryview(self._view[:self._extent])

    cpdef load(self, b):
        """

        Note: may modify buffer size
        """
        n = len(b)
        new_extent = self._extent + n
        overflow = new_extent - len(self._data)
        if overflow > 0:
            if self._recycle():
                return self.load(b)
            self._view = None
            new_extent = self._extent + n
            self._data[self._extent:new_extent] = b
            self._view = memoryview(self._data)
        else:
            self._view[self._extent:new_extent] = b
        self._extent = new_extent

    cpdef int receive(self, socket, int n):
        """

        Note: may modify buffer size, should error if frame exists
        """
        cdef int new_extent
        cdef int overflow
        cdef bytes data
        cdef int data_size

        new_extent = self._extent + n
        overflow = new_extent - len(self._data)
        if overflow > 0:
            if self._recycle():
                return self.receive(socket, n)
            self._view = None
            data = socket.recv(n)
            data_size = len(data)
            new_extent = self._extent + data_size
            self._data[self._extent:new_extent] = data
            self._view = memoryview(self._data)
        else:
            data_size = socket.recv_into(self._view[self._extent:new_extent])
            new_extent = self._extent + data_size
        self._extent = new_extent
        return data_size

    cpdef bint receive_message(self, socket, int n):
        """

        :param socket:
        :param n:
        :return:
        """
        cdef int received

        frame_message = self.frame_message
        receive = self.receive
        while not frame_message():
            received = receive(socket, n)
            if received == 0:
                return False
        return True

    cdef _recycle(self):
        """ Reclaim buffer space before the origin.

        Note: modifies buffer size
        """
        cdef int origin
        cdef int available

        origin = self._origin
        if origin == 0:
            return False
        available = self._extent - origin
        self._data[:available] = self._data[origin:self._extent]
        self._extent = available
        self._origin = 0
        #log_debug("Recycled %d bytes" % origin)
        return True

    cpdef frame(self):
        return self._frame

    cpdef bint frame_message(self):
        """ Construct a frame around the first complete message in the buffer.
        """
        cdef list panes
        cdef int origin
        cdef int p
        cdef int extent
        cdef int available
        cdef int chunk_size
        cdef int q

        if self._frame is not None:
            self.discard_message()
        panes = []
        p = origin = self._origin
        extent = self._extent
        while p < extent:
            available = extent - p
            if available < 2:
                break
            chunk_size, = struct_unpack(">H", self._view[p:(p + 2)])
            p += 2
            if chunk_size == 0:
                self._limit = p
                self._frame = MessageFrame(memoryview(self._view[origin:self._limit]), panes)
                return True
            q = p + chunk_size
            panes.append((p - origin, q - origin))
            p = q
        return False

    cpdef discard_message(self):
        if self._frame is not None:
            self._origin = self._limit
            self._limit = -1
            self._frame = None


cdef class ChunkedOutputBuffer(object):

    cdef int _max_chunk_size
    cdef bytearray _data
    cdef int _header
    cdef int _start
    cdef int _end

    def __cinit__(self, int capacity=1048576, int max_chunk_size=16384):
        self._max_chunk_size = max_chunk_size
        self._header = 0
        self._start = 2
        self._end = 2
        self._data = bytearray(capacity)

    cpdef int max_chunk_size(self):
        return self._max_chunk_size

    cpdef clear(self):
        self._header = 0
        self._start = 2
        self._end = 2
        self._data[0:2] = b"\x00\x00"

    cpdef write(self, bytes b):
        cdef bytearray data
        cdef int new_data_size
        cdef int chunk_size
        cdef int chunk_remaining
        cdef int new_end
        cdef int new_chunk_size

        data = self._data
        new_data_size = len(b)
        chunk_size = self._end - self._start
        max_chunk_size = self._max_chunk_size
        chunk_remaining = max_chunk_size - chunk_size
        if new_data_size > max_chunk_size:
            self.write(b[:chunk_remaining])
            self.chunk()
            self.write(b[chunk_remaining:])
            return
        if new_data_size > chunk_remaining:
            self.chunk()
        new_end = self._end + new_data_size
        new_chunk_size = new_end - self._start
        data[self._end:new_end] = b
        self._end = new_end
        data[self._header:(self._header + 2)] = struct_pack(">H", new_chunk_size)

    cpdef chunk(self):
        self._header = self._end
        self._start = self._header + 2
        self._end = self._start
        self._data[self._header:self._start] = b"\x00\x00"

    cpdef view(self):
        cdef int end
        cdef int chunk_size

        end = self._end
        chunk_size = end - self._start
        if chunk_size == 0:
            return memoryview(self._data[:self._header])
        else:
            return memoryview(self._data[:end])