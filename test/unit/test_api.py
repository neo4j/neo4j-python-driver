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
from uuid import uuid4

from neo4j.v1.api import coerce_parameters


class ParameterTypeTestCase(TestCase):
    def test_should_allow_none(self):
        self.assertIsNone(coerce_parameters(None))

    def test_should_allow_boolean(self):
        self.assertTrue(coerce_parameters(True))
        self.assertFalse(coerce_parameters(False))

    def test_should_allow_integer(self):
        self.assertEqual(coerce_parameters(0), 0)
        self.assertEqual(coerce_parameters(0x7F), 0x7F)
        self.assertEqual(coerce_parameters(0x7FFF), 0x7FFF)
        self.assertEqual(coerce_parameters(0x7FFFFFFF), 0x7FFFFFFF)
        self.assertEqual(coerce_parameters(0x7FFFFFFFFFFFFFFF), 0x7FFFFFFFFFFFFFFF)

    def test_should_disallow_oversized_integer(self):
        with self.assertRaises(ValueError):
            coerce_parameters(0x10000000000000000)
        with self.assertRaises(ValueError):
            coerce_parameters(-0x10000000000000000)

    def test_should_allow_float(self):
        self.assertEqual(coerce_parameters(0.0), 0.0)
        self.assertEqual(coerce_parameters(3.1415926), 3.1415926)

    def test_should_allow_string(self):
        self.assertEqual(coerce_parameters(u""), u"")
        self.assertEqual(coerce_parameters(u"hello, world"), u"hello, world")

    def test_should_allow_bytes(self):
        self.assertEqual(coerce_parameters(bytearray()), bytearray())
        self.assertEqual(coerce_parameters(bytearray([1, 2, 3])), bytearray([1, 2, 3]))

    def test_should_allow_list(self):
        self.assertEqual(coerce_parameters([]), [])
        self.assertEqual(coerce_parameters([1, 2, 3]), [1, 2, 3])

    def test_should_allow_dict(self):
        self.assertEqual(coerce_parameters({}), {})
        self.assertEqual(coerce_parameters({u"one": 1, u"two": 1, u"three": 1}), {u"one": 1, u"two": 1, u"three": 1})
        self.assertEqual(coerce_parameters(
            {u"list": [1, 2, 3, [4, 5, 6]], u"dict": {u"a": 1, u"b": 2}}),
            {u"list": [1, 2, 3, [4, 5, 6]], u"dict": {u"a": 1, u"b": 2}})

    def test_should_disallow_object(self):
        with self.assertRaises(TypeError):
            coerce_parameters(object())
        with self.assertRaises(TypeError):
            coerce_parameters(uuid4())
