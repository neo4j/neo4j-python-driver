# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# This file is part of Neo4j.
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


from unittest import TestCase

from neo4j.spatial import WGS84Point


class WGS84PointTestCase(TestCase):

    def test_alias_3d(self):
        x, y, z = 3.2, 4.0, -1.2
        p = WGS84Point((x, y, z))

        self.assertTrue(hasattr(p, "longitude"))
        self.assertEqual(p.longitude, x)
        self.assertTrue(hasattr(p, "x"))
        self.assertEqual(p.x, x)

        self.assertTrue(hasattr(p, "latitude"))
        self.assertEqual(p.latitude, y)
        self.assertTrue(hasattr(p, "y"))
        self.assertEqual(p.y, y)

        self.assertTrue(hasattr(p, "height"))
        self.assertEqual(p.height, z)
        self.assertTrue(hasattr(p, "z"))
        self.assertEqual(p.z, z)

    def test_alias_2d(self):
        x, y = 3.2, 4.0
        p = WGS84Point((x, y))

        self.assertTrue(hasattr(p, "longitude"))
        self.assertEqual(p.longitude, x)
        self.assertTrue(hasattr(p, "x"))
        self.assertEqual(p.x, x)

        self.assertTrue(hasattr(p, "latitude"))
        self.assertEqual(p.latitude, y)
        self.assertTrue(hasattr(p, "y"))
        self.assertEqual(p.y, y)

        with self.assertRaises(AttributeError):
            p.height
        with self.assertRaises(AttributeError):
            p.z
