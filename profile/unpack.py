#!/usr/bin/env python3
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2015 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
#
# This file is part of Neo4j.
#
# Neo4j is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


from io import BytesIO
import sys
from neo4j.v1.packstream import packb, Unpacker


data = (b"\x71", ([1, 2, 3],))
#data = [1, [2, [3, 4]], 5]
times = 500000


def main():
    packed = packb(data)
    stream = BytesIO(packed)
    seek = stream.seek
    unpacker = Unpacker(stream)
    unpack = unpacker.unpack
    for _ in range(times):
        seek(0)
        unpacked, = unpack()
        #print(unpacked)
        #assert unpacked == [1, [2, [3, 4]], 5]


if __name__ == "__main__":
    main()
