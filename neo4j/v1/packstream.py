#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2016 "Neo Technology,"
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

"""
PackStream
==========

This module contains a full implementation of PackStream: the serialisation
format built specifically for Neo4j. The PackStream design is based heavily on
MessagePack but the implementation completely separate.

Note that PackStream uses big-endian order exclusively and the byte values
described below are all represented as hexadecimal.


Data Types
----------

PackStream allows serialisation of most common data types as well as a generic
'structure' type, used for composite values. The full list of types are:

- Null (absence of value)
- Boolean (true or false)
- Integer (signed 64-bit integer)
- Float (64-bit floating point number)
- String (UTF-8 encoded string data)
- List (ordered collection of values)
- Map (keyed collection of values)
- Structure (composite set of values with a type signature)

Neither unsigned integers nor byte arrays are supported but may be added in a
future version of the format. Note also that 32-bit floating point numbers
are not supported. This is a deliberate decision and these are unlikely to be
added in a future version.


Markers
-------

Every serialised value begins with a marker byte. The marker contains
information on data type as well as direct or indirect size information for
those types that require it. How that size information is encoded varies by
marker type.

Some values, such as boolean true, can be encoded within a single marker byte.
Many small integers (specifically between -16 and +127) are also encoded
within a single byte.

A number of marker bytes are reserved for future expansion of the format
itself. These bytes should not be used, and encountering them in an incoming
stream should treated as an error.


Sized Values
------------

Some value types require variable length representations and, as such, have
their size explicitly encoded. These values generally begin with a single
marker byte, followed by a size, followed by the data content itself. Here,
the marker denotes both type and scale and therefore determines the number of
bytes used to represent the size of the data. The size itself is either an
8-bit, 16-bit or 32-bit unsigned integer.

The diagram below illustrates the general layout for a sized value, here with a
16-bit size:

  Marker Size          Content
    <>  <--->  <--------------------->
    XX  XX XX  XX XX XX XX .. .. .. XX

Null
----

Null is always encoded using the single marker byte 0xC0.

    C0  -- Null


Boolean
-------

Boolean values are encoded within a single marker byte, using 0xC3 to denote
true and 0xC2 to denote false.

    C3  -- True

    C2  -- False


Floating Point Numbers
----------------------

These are double-precision floating points for approximations of any number,
notably for representing fractions and decimal numbers. Floats are encoded as a
single 0xC1 marker byte followed by 8 bytes, formatted according to the IEEE
754 floating-point "double format" bit layout.

- Bit 63 (the bit that is selected by the mask `0x8000000000000000`) represents
  the sign of the number.
- Bits 62-52 (the bits that are selected by the mask `0x7ff0000000000000`)
  represent the exponent.
- Bits 51-0 (the bits that are selected by the mask `0x000fffffffffffff`)
  represent the significand (sometimes called the mantissa) of the number.

    C1 3F F1 99 99 99 99 99 9A  -- Float(+1.1)

    C1 BF F1 99 99 99 99 99 9A  -- Float(-1.1)


Integers
--------

Integer values occupy either 1, 2, 3, 5 or 9 bytes depending on magnitude and
are stored as big-endian signed values. Several markers are designated
specifically as TINY_INT values and can therefore be used to pass a small
number in a single byte. These markers can be identified by a zero high-order
bit or by a high-order nibble containing only ones.

The available encodings are illustrated below and each shows a valid
representation for the decimal value 42:

    2A                          -- TINY_INT

    C8 2A                       -- INT_8

    C9 00 2A                    -- INT_16

    CA 00 00 00 2A              -- INT_32

    CB 00 00 00 00 00 00 00 2A  -- INT_64

Note that while encoding small numbers in wider formats is supported, it is
generally recommended to use the most compact representation possible. The
following table shows the optimal representation for every possible integer:

   Range Minimum             |  Range Maximum             | Representation
 ============================|============================|================
  -9 223 372 036 854 775 808 |             -2 147 483 649 | INT_64
              -2 147 483 648 |                    -32 769 | INT_32
                     -32 768 |                       -129 | INT_16
                        -128 |                        -17 | INT_8
                         -16 |                       +127 | TINY_INT
                        +128 |                    +32 767 | INT_16
                     +32 768 |             +2 147 483 647 | INT_32
              +2 147 483 648 | +9 223 372 036 854 775 807 | INT_64


String
----

String data is represented as UTF-8 encoded binary data. Note that sizes used
for string are the byte counts of the UTF-8 encoded data, not the character count
of the original string.

  Marker | Size                                        | Maximum size
 ========|=============================================|=====================
  80..8F | contained within low-order nibble of marker | 15 bytes
  D0     | 8-bit big-endian unsigned integer           | 255 bytes
  D1     | 16-bit big-endian unsigned integer          | 65 535 bytes
  D2     | 32-bit big-endian unsigned integer          | 4 294 967 295 bytes

For encoded string containing fewer than 16 bytes, including empty strings,
the marker byte should contain the high-order nibble `1000` followed by a
low-order nibble containing the size. The encoded data then immediately
follows the marker.

For encoded string containing 16 bytes or more, the marker 0xD0, 0xD1 or 0xD2
should be used, depending on scale. This marker is followed by the size and
the UTF-8 encoded data. Examples follow below:

    80  -- ""

    81 61  -- "a"

    D0 1A 61 62  63 64 65 66  67 68 69 6A  6B 6C 6D 6E
    6F 70 71 72  73 74 75 76  77 78 79 7A  -- "abcdefghijklmnopqrstuvwxyz"

    D0 18 45 6E  20 C3 A5 20  66 6C C3 B6  74 20 C3 B6
    76 65 72 20  C3 A4 6E 67  65 6E  -- "En å flöt över ängen"


Lists
-----

Lists are heterogeneous sequences of values and therefore permit a mixture of
types within the same list. The size of a list denotes the number of items
within that list, not the total packed byte size. The markers used to denote
a list are described in the table below:

  Marker | Size                                         | Maximum size
 ========|==============================================|=====================
  90..9F | contained within low-order nibble of marker  | 15 bytes
  D4     | 8-bit big-endian unsigned integer            | 255 items
  D5     | 16-bit big-endian unsigned integer           | 65 535 items
  D6     | 32-bit big-endian unsigned integer           | 4 294 967 295 items
  D7     | no size, runs until DF marker is encountered | unlimited

For lists containing fewer than 16 items, including empty lists, the marker
byte should contain the high-order nibble `1001` followed by a low-order
nibble containing the size. The items within the list are then serialised in
order immediately after the marker.

For lists containing 16 items or more, the marker 0xD4, 0xD5 or 0xD6 should be
used, depending on scale. This marker is followed by the size and list items,
serialized in order. Examples follow below:

    90  -- []

    93 01 02 03 -- [1,2,3]

    D4 14 01 02  03 04 05 06  07 08 09 00  01 02 03 04
    05 06 07 08  09 00  -- [1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6,7,8,9,0]

List streams (marker 0xD7) can be used for lists where the total number of
items is not known ahead of time. The items immediately follow the marker
and a final END_OF_STREAM (0xDF) marker denotes the end of the list.


Maps
----

Maps are sized sequences of pairs of values and permit a mixture of types
within the same map. The size of a map denotes the number of pairs within
that map, not the total packed byte size. The markers used to denote a map
are described in the table below:

  Marker | Size                                         | Maximum size
 ========|==============================================|=======================
  A0..AF | contained within low-order nibble of marker  | 15 entries
  D8     | 8-bit big-endian unsigned integer            | 255 entries
  D9     | 16-bit big-endian unsigned integer           | 65 535 entries
  DA     | 32-bit big-endian unsigned integer           | 4 294 967 295 entries
  DB     | no size, runs until DF marker is encountered | unlimited

For maps containing fewer than 16 key-value pairs, including empty maps,
the marker byte should contain the high-order nibble `1010` followed by a
low-order nibble containing the size. The items within the map are then
serialised in key-value-key-value order immediately after the marker. Keys
are typically string values.

For maps containing 16 pairs or more, the marker 0xD8, 0xD9 or 0xDA should be
used, depending on scale. This marker is followed by the size and map
entries, serialised in key-value-key-value order. Examples follow below:

    A0  -- {}

    A1 81 61 01  -- {a:1}

    D8 10 81 61  01 81 62 01  81 63 03 81  64 04 81 65
    05 81 66 06  81 67 07 81  68 08 81 69  09 81 6A 00
    81 6B 01 81  6C 02 81 6D  03 81 6E 04  81 6F 05 81
    70 06  -- {a:1,b:1,c:3,d:4,e:5,f:6,g:7,h:8,i:9,j:0,k:1,l:2,m:3,n:4,o:5,p:6}

Map streams (marker 0xDB) can be used for maps where the total number of
entries is not known ahead of time. The key-value pairs immediately follow
the marker and a final END_OF_STREAM (0xDF) marker denotes the end of the map.


Structures
----------

Structures represent composite values and consist, beyond the marker, of a
single byte signature followed by a sequence of fields, each an individual
value. The size of a structure is measured as the number of fields, not the
total packed byte size. The markers used to denote a structure are described
in the table below:

  Marker | Size                                        | Maximum size
 ========|=============================================|=======================
  B0..BF | contained within low-order nibble of marker | 15 fields
  DC     | 8-bit big-endian unsigned integer           | 255 fields
  DD     | 16-bit big-endian unsigned integer          | 65 535 fields

The signature byte is used to identify the type or class of the structure.
Signature bytes may hold any value between 0 and +127. Bytes with the high bit
set are reserved for future expansion.

For structures containing fewer than 16 fields, the marker byte should
contain the high-order nibble `1011` followed by a low-order nibble
containing the size. The marker is immediately followed by the signature byte
and the field values.

For structures containing 16 fields or more, the marker 0xDC or 0xDD should
be used, depending on scale. This marker is followed by the size, the signature
byte and the actual fields, serialised in order. Examples follow below:

    B3 01 01 02 03  -- Struct(sig=0x01, fields=[1,2,3])

    DC 10 7F 01  02 03 04 05  06 07 08 09  00 01 02 03
    04 05 06  -- Struct(sig=0x7F, fields=[1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6]

"""


from io import BytesIO
from struct import pack as struct_pack, unpack as struct_unpack
import sys

if sys.version_info >= (3,):
    INTEGER_TYPE = int
    STRING_TYPE = str
else:
    INTEGER_TYPE = (int, long)
    STRING_TYPE = (str, unicode)

__all__ = ["Packer", "pack", "packb", "Unpacker", "unpack", "unpackb"]

INFINITY = 1e309

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

TINY_STRING = [bytes(bytearray([x])) for x in range(0x80, 0x90)]
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
STRING_8 = b"\xD0"
STRING_16 = b"\xD1"
STRING_32 = b"\xD2"
LIST_8 = b"\xD4"
LIST_16 = b"\xD5"
LIST_32 = b"\xD6"
LIST_STREAM = b"\xD7"
MAP_8 = b"\xD8"
MAP_16 = b"\xD9"
MAP_32 = b"\xDA"
MAP_STREAM = b"\xDB"
STRUCT_8 = b"\xDC"
STRUCT_16 = b"\xDD"
END_OF_STREAM = b"\xDF"

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


class List(list):

    def __init__(self, capacity):
        self.capacity = capacity

    def append(self, item):
        if item is END_OF_STREAM:
            self.capacity = len(self)
        else:
            list.append(self, item)


class Map(dict):

    def __init__(self, capacity):
        self.capacity = capacity
        self.__key = NotImplemented

    def append(self, item):
        key = self.__key
        if key is NotImplemented:
            if item is END_OF_STREAM:
                self.capacity = len(self)
            else:
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

        # String
        elif isinstance(value, STRING_TYPE):
            if isinstance(value, bytes):
                value_bytes = value
            else:
                value_bytes = value.encode(ENCODING)
            self.pack_string_header(len(value_bytes))
            self.pack_raw(value_bytes)

        # Bytes (deliberately listed after String since in
        # Python 2, bytes should be treated as a String)
        elif isinstance(value, (bytes, bytearray)):
            self.pack_bytes_header(len(value))
            self.pack_raw(value)

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

    def pack_string_header(self, size):
        stream = self.stream
        if size < PLUS_2_TO_THE_4:
            stream.write(TINY_STRING[size])
        elif size < PLUS_2_TO_THE_8:
            stream.write(STRING_8)
            stream.write(PACKED_UINT_8[size])
        elif size < PLUS_2_TO_THE_16:
            stream.write(STRING_16)
            stream.write(PACKED_UINT_16[size])
        elif size < PLUS_2_TO_THE_32:
            stream.write(STRING_32)
            stream.write(struct_pack(UINT_32_STRUCT, size))
        else:
            raise OverflowError("String header size out of range")

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

    def pack_list_stream_header(self):
        self.stream.write(LIST_STREAM)

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

    def pack_map_stream_header(self):
        self.stream.write(MAP_STREAM)

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

    def pack_end_of_stream(self):
        self.stream.write(END_OF_STREAM)


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
        current_collection = List(INFINITY)
        current_capacity = current_collection.capacity
        current_size = len(current_collection)
        push_item = current_collection.append

        collection_stack = []
        push_collection = collection_stack.append
        pop_collection = collection_stack.pop

        stream_read = self.stream.read
        while True:
            marker_byte = stream_read(1)

            if not marker_byte:
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

                # String
                elif marker_high == 0x80:
                    value = stream_read(marker & 0x0F).decode(ENCODING)
                elif marker_byte == STRING_8:
                    byte_size = UNPACKED_UINT_8[stream_read(1)]
                    value = stream_read(byte_size).decode(ENCODING)
                elif marker_byte == STRING_16:
                    byte_size = UNPACKED_UINT_16[stream_read(2)]
                    value = stream_read(byte_size).decode(ENCODING)
                elif marker_byte == STRING_32:
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
                elif marker_byte == LIST_STREAM:
                    size = INFINITY
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
                elif marker_byte == MAP_STREAM:
                    size = INFINITY
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

                elif marker_byte == END_OF_STREAM:
                    value = END_OF_STREAM

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
