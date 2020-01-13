#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2020 "Neo4j,"
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


from unittest import TestCase

from neo4j.data import Record


class RecordTestCase(TestCase):

    def test_record_equality(self):
        record1 = Record(zip(["name", "empire"], ["Nigel", "The British Empire"]))
        record2 = Record(zip(["name", "empire"], ["Nigel", "The British Empire"]))
        record3 = Record(zip(["name", "empire"], ["Stefan", "Das Deutschland"]))
        assert record1 == record2
        assert record1 != record3
        assert record2 != record3

    def test_record_hashing(self):
        record1 = Record(zip(["name", "empire"], ["Nigel", "The British Empire"]))
        record2 = Record(zip(["name", "empire"], ["Nigel", "The British Empire"]))
        record3 = Record(zip(["name", "empire"], ["Stefan", "Das Deutschland"]))
        assert hash(record1) == hash(record2)
        assert hash(record1) != hash(record3)
        assert hash(record2) != hash(record3)

    def test_record_iter(self):
        a_record = Record(zip(["name", "empire"], ["Nigel", "The British Empire"]))
        assert list(a_record.__iter__()) == ["Nigel", "The British Empire"]

    def test_record_as_dict(self):
        a_record = Record(zip(["name", "empire"], ["Nigel", "The British Empire"]))
        assert dict(a_record) == {"name": "Nigel", "empire": "The British Empire"}

    def test_record_as_list(self):
        a_record = Record(zip(["name", "empire"], ["Nigel", "The British Empire"]))
        assert list(a_record) == ["Nigel", "The British Empire"]

    def test_record_len(self):
        a_record = Record(zip(["name", "empire"], ["Nigel", "The British Empire"]))
        assert len(a_record) == 2

    def test_record_repr(self):
        a_record = Record(zip(["name", "empire"], ["Nigel", "The British Empire"]))
        assert repr(a_record) == "<Record name='Nigel' empire='The British Empire'>"

    def test_record_data(self):
        r = Record(zip(["name", "age", "married"], ["Alice", 33, True]))
        self.assertEqual(r.data(), {"name": "Alice", "age": 33, "married": True})
        self.assertEqual(r.data("name"), {"name": "Alice"})
        self.assertEqual(r.data("age", "name"), {"age": 33, "name": "Alice"})
        self.assertEqual(r.data("age", "name", "shoe size"), {"age": 33, "name": "Alice", "shoe size": None})
        self.assertEqual(r.data(0, "name"), {"name": "Alice"})
        self.assertEqual(r.data(0), {"name": "Alice"})
        self.assertEqual(r.data(1, 0), {"age": 33, "name": "Alice"})
        with self.assertRaises(IndexError):
            _ = r.data(1, 0, 999)

    def test_record_keys(self):
        r = Record(zip(["name", "age", "married"], ["Alice", 33, True]))
        self.assertEqual(r.keys(), ["name", "age", "married"])

    def test_record_values(self):
        r = Record(zip(["name", "age", "married"], ["Alice", 33, True]))
        self.assertEqual(r.values(), ["Alice", 33, True])
        self.assertEqual(r.values("name"), ["Alice"])
        self.assertEqual(r.values("age", "name"), [33, "Alice"])
        self.assertEqual(r.values("age", "name", "shoe size"), [33, "Alice", None])
        self.assertEqual(r.values(0, "name"), ["Alice", "Alice"])
        self.assertEqual(r.values(0), ["Alice"])
        self.assertEqual(r.values(1, 0), [33, "Alice"])
        with self.assertRaises(IndexError):
            _ = r.values(1, 0, 999)

    def test_record_items(self):
        r = Record(zip(["name", "age", "married"], ["Alice", 33, True]))
        self.assertEqual(r.items(), [("name", "Alice"), ("age", 33), ("married", True)])
        self.assertEqual(r.items("name"), [("name", "Alice")])
        self.assertEqual(r.items("age", "name"), [("age", 33), ("name", "Alice")])
        self.assertEqual(r.items("age", "name", "shoe size"), [("age", 33), ("name", "Alice"), ("shoe size", None)])
        self.assertEqual(r.items(0, "name"), [("name", "Alice"), ("name", "Alice")])
        self.assertEqual(r.items(0), [("name", "Alice")])
        self.assertEqual(r.items(1, 0), [("age", 33), ("name", "Alice")])
        with self.assertRaises(IndexError):
            _ = r.items(1, 0, 999)

    def test_record_index(self):
        r = Record(zip(["name", "age", "married"], ["Alice", 33, True]))
        self.assertEqual(r.index("name"), 0)
        self.assertEqual(r.index("age"), 1)
        self.assertEqual(r.index("married"), 2)
        with self.assertRaises(KeyError):
            _ = r.index("shoe size")
        self.assertEqual(r.index(0), 0)
        self.assertEqual(r.index(1), 1)
        self.assertEqual(r.index(2), 2)
        with self.assertRaises(IndexError):
            _ = r.index(3)
        with self.assertRaises(TypeError):
            _ = r.index(None)

    def test_record_value(self):
        r = Record(zip(["name", "age", "married"], ["Alice", 33, True]))
        self.assertEqual(r.value(), "Alice")
        self.assertEqual(r.value("name"), "Alice")
        self.assertEqual(r.value("age"), 33)
        self.assertEqual(r.value("married"), True)
        self.assertEqual(r.value("shoe size"), None)
        self.assertEqual(r.value("shoe size", 6), 6)
        self.assertEqual(r.value(0), "Alice")
        self.assertEqual(r.value(1), 33)
        self.assertEqual(r.value(2), True)
        self.assertEqual(r.value(3), None)
        self.assertEqual(r.value(3, 6), 6)
        with self.assertRaises(TypeError):
            _ = r.value(None)

    def test_record_contains(self):
        r = Record(zip(["name", "age", "married"], ["Alice", 33, True]))
        self.assertTrue("Alice" in r)
        self.assertTrue(33 in r)
        self.assertTrue(True in r)
        self.assertFalse(7.5 in r)
        with self.assertRaises(TypeError):
            _ = r.index(None)

    def test_record_from_dict(self):
        r = Record({"name": "Alice", "age": 33})
        self.assertEqual("Alice", r["name"])
        self.assertEqual(33, r["age"])

    def test_record_get_slice(self):
        r = Record(zip(["name", "age", "married"], ["Alice", 33, True]))
        self.assertEqual(Record(zip(["name", "age"], ["Alice", 33])), r[0:2])

    def test_record_get_by_index(self):
        r = Record(zip(["name", "age", "married"], ["Alice", 33, True]))
        self.assertEqual("Alice", r[0])

    def test_record_get_by_name(self):
        r = Record(zip(["name", "age", "married"], ["Alice", 33, True]))
        self.assertEqual("Alice", r["name"])

    def test_record_get_by_out_of_bounds_index(self):
        r = Record(zip(["name", "age", "married"], ["Alice", 33, True]))
        self.assertIsNone(r[9])
