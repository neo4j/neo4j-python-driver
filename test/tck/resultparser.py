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
import re
from neo4j.v1 import Node, Relationship, Path
from test_value import TestValue


def parse_values_to_comparable(row):
    return _parse(row, True)


def parse_values(row):
    return _parse(row, False)


def _parse(row, comparable=False):
    if is_array(row):
        values = get_array(row)
        result = []
        for val in values:
            result.append(_parse(val, comparable))
        return result
    elif is_map(row):
        if comparable:
            raise ValueError("No support for pasing maps of Node/Rels/paths atm")
        return get_map(row)
    else:
        if comparable:
            return value_to_comparable_object(row)
        else:
            return value_to_object(row)


def is_array(val):
    return val[0] == '[' and val[-1] == ']' and val[1] != ':'


def get_array(val):
    return val[1:-1].split(',')


def is_map(val):
    return val[0] == '{' and val[-1] == '}'


def get_map(val):
    return json.loads(val)


def value_to_comparable_object(val):
    return TestValue(value_to_object(val))


def value_to_object(val):
    val = val.strip()
    PATH = '^(<\().*(\)>)$'
    NODE = '^(\().*(\))$'
    RELATIONSHIP = '^(\[:).*(\])$'
    INTEGER = '^(-?[0-9]+)$'
    STRING = '^(").*(")$'
    NULL = '^null$'
    BOOLEAN = '^(false|true)$'
    # TEST FLOAT AFTER INT
    FLOAT = '^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$'
    if re.match(PATH, val):
        return get_path(val)[0]
    if re.match(RELATIONSHIP, val):
        return get_relationship(val)[0]
    if re.match(NODE, val):
        return get_node(val)[0]
    if re.match(NULL, val):
        return None
    if re.match(BOOLEAN, val):
        return bool(val)
    if re.match(STRING, val):
        return val[1:-1]
    if re.match(INTEGER, val):
        return int(val)
    if re.match(FLOAT, val):
        return float(val)
    raise TypeError("String value does not match any type. Got: %s" % val)


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
    id = 0
    string_path = string_path[1:-1]
    n, string_path = get_node(string_path)
    list_of_nodes_and_rel = [n]
    id+=1
    n.id = id
    while string_path != '':
        r, string_path, point_up = get_relationship(string_path)
        n, string_path = get_node(string_path)
        id+=1
        n.id = id
        if point_up:
            r.start = list_of_nodes_and_rel[-1].id
            r.end = n.id
            r.id = 0
        else:
            r.start = n.id
            r.end = list_of_nodes_and_rel[-1].id
            r.id = 0
        list_of_nodes_and_rel.append(r)
        list_of_nodes_and_rel.append(n)
    path = Path(list_of_nodes_and_rel[0], *list_of_nodes_and_rel[1:])
    return path, string_path
