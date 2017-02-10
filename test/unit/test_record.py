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


from unittest import TestCase

from neo4j.v1 import Record


class RecordTestCase(TestCase):

    def test_record_equality(self):
        record1 = Record(["name", "empire"], ["Nigel", "The British Empire"])
        record2 = Record(["name", "empire"], ["Nigel", "The British Empire"])
        record3 = Record(["name", "empire"], ["Stefan", "Das Deutschland"])
        assert record1 == record2
        assert record1 != record3
        assert record2 != record3

    def test_record_hashing(self):
        record1 = Record(["name", "empire"], ["Nigel", "The British Empire"])
        record2 = Record(["name", "empire"], ["Nigel", "The British Empire"])
        record3 = Record(["name", "empire"], ["Stefan", "Das Deutschland"])
        assert hash(record1) == hash(record2)
        assert hash(record1) != hash(record3)
        assert hash(record2) != hash(record3)

    def test_record_keys(self):
        a_record = Record(["name", "empire"], ["Nigel", "The British Empire"])
        assert list(a_record.keys()) == ["name", "empire"]

    def test_record_values(self):
        a_record = Record(["name", "empire"], ["Nigel", "The British Empire"])
        assert list(a_record.values()) == ["Nigel", "The British Empire"]

    def test_record_items(self):
        a_record = Record(["name", "empire"], ["Nigel", "The British Empire"])
        assert list(a_record.items()) == [("name", "Nigel"), ("empire", "The British Empire")]

    def test_record_index(self):
        a_record = Record(["name", "empire"], ["Nigel", "The British Empire"])
        assert a_record.index("name") == 0
        assert a_record.index("empire") == 1
        with self.assertRaises(KeyError):
            a_record.index("crap")

    def test_record_contains(self):
        a_record = Record(["name", "empire"], ["Nigel", "The British Empire"])
        assert "name" in a_record
        assert "empire" in a_record
        assert "Germans" not in a_record

    def test_record_iter(self):
        a_record = Record(["name", "empire"], ["Nigel", "The British Empire"])
        assert list(a_record.__iter__()) == ["name", "empire"]

    def test_record_copy(self):
        original = Record(["name", "empire"], ["Nigel", "The British Empire"])
        duplicate = original.copy()
        assert dict(original) == dict(duplicate)
        assert original.keys() == duplicate.keys()
        assert original is not duplicate

    def test_record_as_dict(self):
        a_record = Record(["name", "empire"], ["Nigel", "The British Empire"])
        assert dict(a_record) == {"name": "Nigel", "empire": "The British Empire"}

    def test_record_as_list(self):
        a_record = Record(["name", "empire"], ["Nigel", "The British Empire"])
        assert list(a_record) == ["name", "empire"]

    def test_record_len(self):
        a_record = Record(["name", "empire"], ["Nigel", "The British Empire"])
        assert len(a_record) == 2

    def test_record_repr(self):
        a_record = Record(["name", "empire"], ["Nigel", "The British Empire"])
        assert repr(a_record) == "<Record name='Nigel' empire='The British Empire'>"
