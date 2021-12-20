#!/usr/bin/env python
# coding: utf-8

# Copyright (c) "Neo4j"
# Neo4j Sweden AB [http://neo4j.com]
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

import io
import struct
from unittest import TestCase

from neo4j.data import DataDehydrator
from neo4j.packstream import Packer
from neo4j.spatial import CartesianPoint


class CartesianPointTestCase(TestCase):

    def test_alias(self):
        x, y, z = 3.2, 4.0, -1.2
        p = CartesianPoint((x, y, z))
        self.assert_(hasattr(p, "x"))
        self.assertEqual(p.x, x)
        self.assert_(hasattr(p, "y"))
        self.assertEqual(p.y, y)
        self.assert_(hasattr(p, "z"))
        self.assertEqual(p.z, z)

    def test_dehydration_3d(self):
        coordinates = (1, -2, 3.1)
        p = CartesianPoint(coordinates)

        dehydrator = DataDehydrator()
        buffer = io.BytesIO()
        packer = Packer(buffer)
        packer.pack(dehydrator.dehydrate((p,))[0])
        self.assertEqual(
            buffer.getvalue(),
            b"\xB4Y" +
            b"\xC9" + struct.pack(">h", 9157) +
            b"".join(map(lambda c: b"\xC1" + struct.pack(">d", c), coordinates))
        )

    def test_dehydration_2d(self):
        coordinates = (.1, 0)
        p = CartesianPoint(coordinates)

        dehydrator = DataDehydrator()
        buffer = io.BytesIO()
        packer = Packer(buffer)
        packer.pack(dehydrator.dehydrate((p,))[0])
        self.assertEqual(
            buffer.getvalue(),
            b"\xB3X" +
            b"\xC9" + struct.pack(">h", 7203) +
            b"".join(map(lambda c: b"\xC1" + struct.pack(">d", c), coordinates))
        )
