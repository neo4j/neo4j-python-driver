#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2019 "Neo4j,"
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
from uuid import uuid4

import pytest

from neo4j.work.simple import DataDehydrator

import neo4j.api


def dehydrated_value(value):
    return DataDehydrator.fix_parameters({"_": value})["_"]


class ValueDehydrationTestCase(TestCase):

    def test_should_allow_none(self):
        self.assertIsNone(dehydrated_value(None))

    def test_should_allow_boolean(self):
        self.assertTrue(dehydrated_value(True))
        self.assertFalse(dehydrated_value(False))

    def test_should_allow_integer(self):
        self.assertEqual(dehydrated_value(0), 0)
        self.assertEqual(dehydrated_value(0x7F), 0x7F)
        self.assertEqual(dehydrated_value(0x7FFF), 0x7FFF)
        self.assertEqual(dehydrated_value(0x7FFFFFFF), 0x7FFFFFFF)
        self.assertEqual(dehydrated_value(0x7FFFFFFFFFFFFFFF), 0x7FFFFFFFFFFFFFFF)

    def test_should_disallow_oversized_integer(self):
        with self.assertRaises(ValueError):
            dehydrated_value(0x10000000000000000)
        with self.assertRaises(ValueError):
            dehydrated_value(-0x10000000000000000)

    def test_should_allow_float(self):
        self.assertEqual(dehydrated_value(0.0), 0.0)
        self.assertEqual(dehydrated_value(3.1415926), 3.1415926)

    def test_should_allow_string(self):
        self.assertEqual(dehydrated_value(u""), u"")
        self.assertEqual(dehydrated_value(u"hello, world"), u"hello, world")

    def test_should_allow_bytes(self):
        self.assertEqual(dehydrated_value(bytearray()), bytearray())
        self.assertEqual(dehydrated_value(bytearray([1, 2, 3])), bytearray([1, 2, 3]))

    def test_should_allow_list(self):
        self.assertEqual(dehydrated_value([]), [])
        self.assertEqual(dehydrated_value([1, 2, 3]), [1, 2, 3])

    def test_should_allow_dict(self):
        self.assertEqual(dehydrated_value({}), {})
        self.assertEqual(dehydrated_value({u"one": 1, u"two": 1, u"three": 1}), {u"one": 1, u"two": 1, u"three": 1})
        self.assertEqual(dehydrated_value(
            {u"list": [1, 2, 3, [4, 5, 6]], u"dict": {u"a": 1, u"b": 2}}),
            {u"list": [1, 2, 3, [4, 5, 6]], u"dict": {u"a": 1, u"b": 2}})

    def test_should_disallow_object(self):
        with self.assertRaises(TypeError):
            dehydrated_value(object())
        with self.assertRaises(TypeError):
            dehydrated_value(uuid4())


def test_bookmark_class():
    bookmark = neo4j.api.Bookmark(None)
    assert bookmark.values == frozenset()
    assert bool(bookmark) is False

    bookmark = neo4j.api.Bookmark("")
    assert bookmark.values == frozenset()
    assert bool(bookmark) is False

    assert repr(bookmark) == "<Bookmark values={}>"

    bookmark = neo4j.api.Bookmark("test1")
    assert bookmark.values == frozenset(["test1", ])
    assert bool(bookmark) is True

    bookmark = neo4j.api.Bookmark("test1", "test2", None, "")
    assert bookmark.values == frozenset(["test1", "test2"])
    assert bool(bookmark) is True

    assert repr(bookmark) == "<Bookmark values={'test1', 'test2'}>"

    not_ascii = "♥O◘♦♥O◘♦"

    with pytest.raises(ValueError):
        bookmark = neo4j.api.Bookmark(not_ascii)
