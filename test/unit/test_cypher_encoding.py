#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2018 "Neo Technology,"
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


from collections import OrderedDict
from unittest import TestCase, SkipTest

from neo4j.cypher.encoding import cypher_escape, cypher_repr, cypher_str, LabelSetView, PropertyDictView
from neo4j.v1.types.graph import Graph, Node, Path


class CypherEscapeTestCase(TestCase):

    def test_normal_identifier(self):
        self.assertEqual(cypher_escape(u"name"), u"name")

    def test_identifier_with_odd_characters(self):
        self.assertEqual(cypher_escape(u"odd name"), u"`odd name`")

    def test_non_identifier(self):
        with self.assertRaises(TypeError):
            _ = cypher_escape(1)


class CypherStrTestCase(TestCase):

    def test_unicode_string(self):
        self.assertEqual(cypher_str(u"hello, world"), u"hello, world")

    def test_byte_string(self):
        self.assertEqual(cypher_str(b"hello, world"), u"hello, world")

    def test_non_string(self):
        self.assertEqual(cypher_str(1), u"1")


class LabelSetViewTestCase(TestCase):

    @staticmethod
    def selected_repr_set(v):
        return set(repr(v).strip(":").split(":"))

    def test_creation(self):
        v = LabelSetView(["Person", "Human Being", "Employee"])
        self.assertEqual(set(v), {"Person", "Human Being", "Employee"})

    def test_repr(self):
        v = LabelSetView(["Person", "Human Being", "Employee"])
        self.assertEqual(self.selected_repr_set(v), {"Person", "`Human Being`", "Employee"})

    def test_selection(self):
        v = LabelSetView(["Person", "Human Being", "Employee"], ["Person", "Cat"])
        self.assertEqual(self.selected_repr_set(v), {"Person"})

    def test_attribute_selection(self):
        v = LabelSetView(["Person", "Human Being", "Employee"])
        self.assertEqual(self.selected_repr_set(v.Person.Person.Employee.Cat), {"Person", "Employee"})

    def test_length(self):
        v = LabelSetView(["Person", "Human Being", "Employee"])
        self.assertEqual(len(v), 3)

    def test_contains(self):
        v = LabelSetView(["Person", "Human Being", "Employee"])
        self.assertIn("Person", v)
        self.assertNotIn("Cat", v)


class PropertyDictViewTestCase(TestCase):

    @staticmethod
    def selected_repr_set(v):
        return set(repr(v).strip("{}").split(", "))

    def test_creation(self):
        v = PropertyDictView([("name", "Alice"), ("age", 33), ("is married", True)])
        self.assertEqual(dict(v), {"name": "Alice", "age": 33, "is married": True})

    def test_selection(self):
        v = PropertyDictView([("name", "Alice"), ("age", 33), ("is married", True)])
        self.assertEqual(self.selected_repr_set(v), {"name: 'Alice'", "age: 33", "`is married`: true"})

    def test_attribute_selection(self):
        v = PropertyDictView([("name", "Alice"), ("age", 33), ("is married", True)])
        self.assertEqual(self.selected_repr_set(v.name.name.age.gender), {"name: 'Alice'", "age: 33"})

    def test_length(self):
        v = PropertyDictView([("name", "Alice"), ("age", 33), ("is married", True)])
        self.assertEqual(len(v), 3)

    def test_contains(self):
        v = PropertyDictView([("name", "Alice"), ("age", 33), ("is married", True)])
        self.assertIn("name", v)
        self.assertNotIn("gender", v)


class PrimitiveReprTestCase(TestCase):

    def test_null(self):
        self.assertEqual(cypher_repr(None), "null")

    def test_boolean_true(self):
        self.assertEqual(cypher_repr(True), "true")

    def test_boolean_false(self):
        self.assertEqual(cypher_repr(False), "false")

    def test_integer(self):
        self.assertEqual(cypher_repr(42), "42")

    def test_float(self):
        self.assertEqual(cypher_repr(3.1415926), "3.1415926")

    def test_string(self):
        self.assertEqual(cypher_repr("hello, world"), "'hello, world'")

    def test_string_with_alternate_quotes(self):
        self.assertEqual(cypher_repr("hello, world", quote='"'), '"hello, world"')

    def test_empty_string(self):
        self.assertEqual(cypher_repr(""), "''")

    def test_string_containing_single_quote(self):
        self.assertEqual(cypher_repr("Adam's apple"), "\"Adam's apple\"")

    def test_string_containing_double_quote(self):
        self.assertEqual(cypher_repr("JSON is a \"data type\""), "'JSON is a \"data type\"'")

    def test_illegal_quote_character(self):
        with self.assertRaises(ValueError):
            _ = cypher_repr("hello, world", quote="`")

    def test_string_from_bytes(self):
        try:
            self.assertIn(cypher_repr(b"Abc"), {"'Abc'"})
        except TypeError:
            raise SkipTest("Must be Python 3")

    def test_bytes(self):
        value = bytearray([0x00, 0x33, 0x66, 0x99, 0xCC, 0xFF])
        self.assertEqual(cypher_repr(value), "bytearray([0x00, 0x33, 0x66, 0x99, 0xCC, 0xFF])")

    def test_list(self):
        self.assertEqual(cypher_repr([1, True, 3.1415]), "[1, true, 3.1415]")

    def test_list_with_alternate_sequence_separator(self):
        self.assertEqual(cypher_repr([1, True, 3.1415], sequence_separator="; "), "[1; true; 3.1415]")

    def test_map(self):
        value = OrderedDict([("one", "eins"), ("number 2", "zwei")])
        self.assertEqual(cypher_repr(value), "{one: 'eins', `number 2`: 'zwei'}")

    def test_map_with_alternate_key_value_separator(self):
        value = OrderedDict([("one", "eins"), ("number 2", "zwei")])
        self.assertEqual(cypher_repr(value, key_value_separator="="), "{one='eins', `number 2`='zwei'}")

    def test_map_with_empty_key(self):
        with self.assertRaises(ValueError):
            _ = cypher_repr({"": ""})

    def test_unsupported_type(self):
        with self.assertRaises(TypeError):
            _ = cypher_repr(object())


class NodeReprTestCase(TestCase):

    def test_empty_node(self):
        g = Graph()
        a = Node(g, 1)
        r = cypher_repr(a, node_template="{labels} {properties}")
        self.assertEqual(u"({})", r)

    def test_node_with_label(self):
        g = Graph()
        a = g.put_node(1, {"Person"})
        r = cypher_repr(a, node_template="{labels} {properties}")
        self.assertEqual(u"(:Person {})", r)

    def test_node_with_multiple_labels(self):
        g = Graph()
        a = g.put_node(1, {"Person", "Employee"})
        r = cypher_repr(a, node_template="{labels} {properties}")
        self.assertEqual(u"(:Employee:Person {})", r)

    def test_node_with_labels_and_properties(self):
        g = Graph()
        a = g.put_node(1, {"Person"}, name="Alice")
        r = cypher_repr(a, node_template="{labels} {properties}")
        self.assertEqual(u"(:Person {name: 'Alice'})", r)

    def test_node_with_only_properties(self):
        g = Graph()
        a = g.put_node(1, name="Alice", age=33)
        r = cypher_repr(a, node_template="{labels} {properties}")
        self.assertEqual(u"({age: 33, name: 'Alice'})", r)

    def test_selection(self):
        g = Graph()
        a = g.put_node(1, {"Person", "Employee"}, name="Alice", age=33)
        r = cypher_repr(a, node_template="{labels.Person} {properties.name}")
        self.assertEqual(u"(:Person {name: 'Alice'})", r)

    def test_selection_by_property_value_only(self):
        g = Graph()
        a = g.put_node(1, {"Person", "Employee"}, name="Alice", age=33)
        r = cypher_repr(a, node_template="{property.name}")
        self.assertEqual(u"(Alice)", r)

    def test_failed_selection(self):
        g = Graph()
        a = g.put_node(1, {"Person", "Employee"}, name="Alice", age=33)
        r = cypher_repr(a, node_template="{labels.Cat} {properties.paws}")
        self.assertEqual(u"({})", r)


class RelationshipReprTestCase(TestCase):

    def test_basic_relationship(self):
        g = Graph()
        a = g.put_node(1)
        b = g.put_node(2)
        ab = g.put_relationship(9, a, b, "KNOWS", since=1999)
        r = cypher_repr(ab)
        self.assertEqual(u'(_1)-[:KNOWS {since: 1999}]->(_2)', r)

    def test_alternate_related_node_template(self):
        g = Graph()
        a = g.put_node(1, name="Alice")
        b = g.put_node(2, name="Bob")
        ab = g.put_relationship(9, a, b, "KNOWS", since=1999)
        r = cypher_repr(ab, related_node_template="{property.name}")
        self.assertEqual(u'(Alice)-[:KNOWS {since: 1999}]->(Bob)', r)

    def test_alternate_relationship_template(self):
        g = Graph()
        a = g.put_node(1, name="Alice")
        b = g.put_node(2, name="Bob")
        ab = g.put_relationship(9, a, b, "KNOWS", since=1999)
        r = cypher_repr(ab, related_node_template="{property.name}", relationship_template="{type}")
        self.assertEqual(u'(Alice)-[:KNOWS]->(Bob)', r)


class PathReprTestCase(TestCase):

    def test_basic_path(self):
        g = Graph()
        a = g.put_node(1)
        b = g.put_node(2)
        c = g.put_node(3)
        d = g.put_node(4)
        ab = g.put_relationship(7, a, b, "KNOWS")
        cb = g.put_relationship(8, c, b, "KNOWS")
        cd = g.put_relationship(9, c, d, "KNOWS")
        p = Path(a, ab, cb, cd)
        r = cypher_repr(p)
        self.assertEqual(u"(_1)-[:KNOWS {}]->(_2)<-[:KNOWS {}]-(_3)-[:KNOWS {}]->(_4)", r)


class GraphReprTestCase(TestCase):

    def test_basic_graph(self):
        g = Graph()
        a = g.put_node(1)
        b = g.put_node(2)
        c = g.put_node(3)
        d = g.put_node(4)
        ab = g.put_relationship(7, a, b, "KNOWS")
        cb = g.put_relationship(8, c, b, "KNOWS")
        cd = g.put_relationship(9, c, d, "KNOWS")
        r = cypher_repr(g)
        self.assertEqual(u"<Graph order=4 size=3>", r)
