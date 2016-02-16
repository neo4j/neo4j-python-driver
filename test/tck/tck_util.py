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

from neo4j.v1 import compat, Relationship, Node, Path

from neo4j.v1 import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost")


def send_string(text):
    session = driver.session()
    cursor = session.run(text)
    session.close()
    return list(cursor.stream())


def send_parameters(statement, parameters):
    session = driver.session()
    cursor = session.run(statement, parameters)
    session.close()
    return list(cursor.stream())


def to_unicode(val):
    try:
        return unicode(val)
    except NameError:
        return str(val)


def string_to_type(str):
    str = str.upper()
    if str == Type.INTEGER.upper():
        return Type.INTEGER
    elif str == Type.FLOAT.upper():
        return Type.FLOAT
    elif str == Type.BOOLEAN.upper():
        return Type.BOOLEAN
    elif str == Type.NULL.upper():
        return Type.NULL
    elif str == Type.STRING.upper():
        return Type.STRING
    elif str == Type.NODE.upper():
        return Type.NODE
    elif str == Type.RELATIONSHIP.upper():
        return Type.RELATIONSHIP
    elif str == Type.PATH.upper():
        return Type.PATH
    else:
        raise ValueError("Not recognized type: %s" % str)


class Type:
    INTEGER = "Integer"
    FLOAT = "Float"
    BOOLEAN = "Boolean"
    STRING = "String"
    NODE = "Node"
    RELATIONSHIP = "Relationship"
    PATH = "Path"
    NULL = "Null"


class TestValue:
    content = None

    def __init__(self, entity):
        self.content = {}
        if isinstance(entity, Node):
            self.content = self.create_node(entity)
        elif isinstance(entity, Relationship):
            self.content = self.create_relationship(entity)
        elif isinstance(entity, Path):
            self.content = self.create_path(entity)
        elif isinstance(entity, int) or isinstance(entity, float) or isinstance(entity,
                                                                                (str, compat.string)) or entity is None:
            self.content['value'] = entity
        else:
            raise ValueError("Do not support object type: %s" % entity)

    def __hash__(self):
        return hash(repr(self))

    def __eq__(self, other):
        return self.content == other.content

    def __repr__(self):
        return str(self.content)

    def create_node(self, entity):
        content = {'properties': entity.properties, 'labels': entity.labels, 'obj': "node"}

        return content

    def create_path(self, entity):
        content = {}
        prev_id = entity.start.identity
        p = []
        for i, rel in enumerate(list(entity)):
            n = entity.nodes[i + 1]
            current_id = n.identity
            if rel.start == prev_id and rel.end == current_id:
                rel.start = i
                rel.end = i + 1
            elif rel.start == current_id and rel.end == prev_id:
                rel.start = i + 1
                rel.end = i
            else:
                raise ValueError(
                        "Relationships end and start should point to surrounding nodes. Rel: %s N1id: %s N2id: %s. At entity#%s" % (
                            rel, current_id, prev_id, i))
            p += [self.create_relationship(rel, True), self.create_node(n)]
            prev_id = current_id
        content['path'] = p
        content['obj'] = "path"
        content['start'] = self.create_node(entity.start)
        return content

    def create_relationship(self, entity, include_start_end=False):
        content = {'obj': "relationship"}
        if include_start_end:
            self.content['start'] = entity.start
            self.content['end'] = entity.end
        content['type'] = entity.type
        content['properties'] = entity.properties
        return content
