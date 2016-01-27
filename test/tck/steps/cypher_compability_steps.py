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

import json

from behave import *

from neo4j.v1 import compat, Node, Relationship, Path
from test.tck.tck_util import Value, Type, string_to_type, send_string, send_parameters

use_step_matcher("re")


@given("init: (?P<statement>.+);")
def step_impl(context, statement):
    send_string(statement)


@when("running: (?P<statement>.+);")
def step_impl(context, statement):
    context.results = {"as_string": send_string(statement)}


@then("result should be (?P<type>.+)\(s\)")
def step_impl(context, type):
    result = context.results["as_string"]
    given = driver_result_to_comparable_result(result)
    expected = table_to_comparable_result(context.table, type)
    if not unordered_equal(given, expected):
        raise Exception("%s does not match given: \n%s expected: \n%s" % (type, given, expected))


@then("result should be empty")
def step_impl(context):
    assert context.results
    for result in context.results.values():
        assert len(result) == 0


@then("result should map to types")
def step_impl(context):
    keys = context.table.headings
    values = context.table.rows[0]
    context.types = {keys[i]: values[i] for i in range(len(keys))}


@then("result should be mixed")
def step_impl(context):
    result = context.results["as_string"]
    given = driver_result_to_comparable_result(result)
    expected = table_to_comparable_result(context.table, context.types)
    if given != expected:
        raise Exception("Mixed response does not match given: %s expected: %s" % (given, expected))


@when('running parametrized: (?P<statement>.+);')
def step_impl(context, statement):
    parameters = {}
    keys = context.table.headings
    assert len(context.table.rows) == 1
    values = context.table[0]
    for i, v in enumerate(values):
        if v[0] == '"':
            val = v[1:-1]
        else:
            val = int(v)
        parameters[keys[i]] = val

    context.results = {"as_string": send_parameters(statement, parameters)}


def get_properties(prop):
    split_at = prop.find('{')
    if split_at != -1:
        n_start, prop = split_two(prop, split_at)
        split_at = prop.find('}')
        if split_at == -1:
            raise ValueError(
                    "Node properties not correctly representetd. Found starrting '{' but no ending '}' in : %s" % prop)
        prop, n_end = split_two(prop, split_at + 1)
        n = n_start + n_end
        properties = json.loads(prop)
        return properties, n
    else:
        return {}, prop


def get_labels(labels):
    labels = labels.split(':')
    if len(labels) == 1:
        return [], labels[0]
    n = labels[0]
    labels = labels[1:]
    if labels[-1].find(' ') != -1:
        labels[-1], n_end = split_two(labels[-1], labels[-1].find(' '))
        n += n_end
    return labels, n


def get_labels_and_properties(n):
    prop, n = get_properties(n)
    labels, n = get_labels(n)
    return labels, prop, n


def split_two(string_entity, split_at):
    return string_entity[:split_at], string_entity[split_at:]


def get_node(string_entity):
    if string_entity is None:
        return None, ''
    if string_entity[0] != "(":
        raise ValueError("Excpected node which shuld start with '('. Got: %s" % string_entity[0])
    split_at = string_entity.find(')')
    if split_at == -1:
        raise ValueError("Excpected node which shuld end with ')'. Found no such character in: %s" % string_entity)
    n, string_entity = split_two(string_entity, split_at + 1)
    n = n[1:-1]
    labels, properties, n = get_labels_and_properties(n)
    node = Node(labels=labels, properties=properties)
    return node, string_entity


def get_relationship(string_entity):
    point_up = None
    if string_entity[:3] == "<-[":
        point_up = False
        string_entity = string_entity[3:]
        rel = string_entity[:string_entity.find(']-')]
        string_entity = string_entity[string_entity.find(']-') + 2:]
    elif string_entity[:2] == "-[":
        point_up = True
        string_entity = string_entity[2:]
        rel = string_entity[:string_entity.find(']-')]
        string_entity = string_entity[string_entity.find(']->') + 3:]
    elif string_entity[0] == "[":
        string_entity = string_entity[1:]
        rel = string_entity[:string_entity.find(']')]
        string_entity = string_entity[string_entity.find(']') + 1:]
    else:
        raise ValueError("Cant identify relationship from: %s" % string_entity)
    type, properties, str = get_labels_and_properties(rel)
    if len(type) > 1:
        raise ValueError("Relationship can only have one type. Got: %s" % type)
    relationship = Relationship(1, 2, type[0], properties=properties)
    return relationship, string_entity, point_up


def get_path(string_path):
    n, string_path = get_node(string_path)
    list_of_nodes_and_rel = [n]
    n.identity = 0
    while string_path != '':
        r, string_path, point_up = get_relationship(string_path)
        n, string_path = get_node(string_path)
        n.identity = len(list_of_nodes_and_rel)
        if point_up:
            r.start = list_of_nodes_and_rel[-1].identity
            r.end = n.identity
        else:
            r.start = n.identity
            r.end = list_of_nodes_and_rel[-1].identity
        list_of_nodes_and_rel.append(r)
        list_of_nodes_and_rel.append(n)
    path = Path(list_of_nodes_and_rel[0], *list_of_nodes_and_rel[1:])
    return path, string_path


def _string_value_to_comparable(val, type):
    def get_val(v):
        if type == Type.INTEGER:
            return Value(int(v))
        elif type == Type.STRING:
            return Value(v)
        elif type == Type.NODE:
            return Value(get_node(v)[0])
        elif type == Type.RELATIONSHIP:
            return Value(get_relationship(v)[0])
        elif type == Type.PATH:
            return Value(get_path(v)[0])
        else:
            raise ValueError("Not recognized type: %s" % type)

    assert isinstance(type, compat.string)
    if val == 'null':
        return Value(None)
    if val[0] == '[':
        if type != Type.RELATIONSHIP or (type == Type.RELATIONSHIP and val[1] == '['):
            val = val[1:-1].split(", ")
    if isinstance(val, list):
        return tuple([get_val(v) for v in val])
    else:
        return get_val(val)


def _driver_value_to_comparable(val):
    if isinstance(val, list):
        return tuple([Value(v) for v in val])
    else:
        return Value(val)


def table_to_comparable_result(table, types):
    result = []
    keys = table.headings
    if isinstance(types, compat.string):
        types = {key: string_to_type(types) for key in keys}
    elif isinstance(types, dict):
        assert set(types.keys()) == set(keys)
    else:
        raise ValueError("types must be either string of single type or map of types corresponding to result keys Got:"
                         " %s" % types)
    for row in table:
        result.append(
                {keys[i]: _string_value_to_comparable(row[i], string_to_type(types[keys[i]])) for i in range(len(row))})
    return result


def driver_result_to_comparable_result(result):
    records = []
    for record in result:
        records.append({key: _driver_value_to_comparable(record[key]) for key in record})
    return records


def unordered_equal(one, two):
    l1 = one[:]
    l2 = two[:]
    assert isinstance(l1, list)
    assert isinstance(l2, list)
    assert len(l1) == len(l2)
    for d1 in l1:
        size = len(l2)
        for d2 in l2:
            if d1 == d2:
                l2.remove(d2)
                break
        if size == len(l2):
            return False
    return True
