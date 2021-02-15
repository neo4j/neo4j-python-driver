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
from neo4j.spatial import (
    Point,
    point_type,
)


class PointTestCase(TestCase):

    def test_wrong_type_arguments(self):
        for argument in (("a", "b"), ({"x": 1.0, "y": 2.0})):
            with self.subTest():
                with self.assertRaises(ValueError):
                    Point(argument)

    def test_number_arguments(self):
        for argument in ((1, 2), (1.2, 2.1)):
            with self.subTest():
                p = Point(argument)
                assert tuple(p) == argument

    def test_dehydration(self):
        MyPoint = point_type("MyPoint", ["x", "y"], {2: 1234})
        coordinates = (.1, 0)
        p = MyPoint(coordinates)

        dehydrator = DataDehydrator()
        buffer = io.BytesIO()
        packer = Packer(buffer)
        packer.pack(dehydrator.dehydrate((p,))[0])
        self.assertEqual(
            buffer.getvalue(),
            b"\xB3X" +
            b"\xC9" + struct.pack(">h", 1234) +
            b"".join(map(lambda c: b"\xC1" + struct.pack(">d", c), coordinates))
        )

    def test_immutable_coordinates(self):
        MyPoint = point_type("MyPoint", ["x", "y"], {2: 1234})
        coordinates = (.1, 0)
        p = MyPoint(coordinates)
        with self.assertRaises(AttributeError):
            p.x = 2.0
        with self.assertRaises(AttributeError):
            p.y = 2.0
        with self.assertRaises(TypeError):
            p[0] = 2.0
        with self.assertRaises(TypeError):
            p[1] = 2.0
