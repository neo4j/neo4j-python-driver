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


from tck.test_value import TestValue

from neo4j.v1 import GraphDatabase, basic_auth
from tck.resultparser import parse_values_to_comparable

BOLT_URI = "bolt://localhost:7687"
AUTH_TOKEN = basic_auth("neotest", "neotest")

driver = GraphDatabase.driver(BOLT_URI, auth=AUTH_TOKEN, encrypted=False)
runners = []


def send_string(statement):
    runner = Runner(statement).run()
    runners.append(runner)
    return runner


def send_parameters(statement, parameters):
    runner = Runner(statement, parameters).run()
    runners.append(runner)
    return runner

try:
    to_unicode = unicode
except NameError:
    to_unicode = str


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


class Runner:
    def __init__(self, statement, parameter=None):
        self.session = driver.session()
        self.statement = statement
        self.parameter = parameter
        self.result = None

    def run(self):
        self.result = self.session.run(self.statement, self.parameter)
        return self

    def close(self):
        self.session.close()


def table_to_comparable_result(table):
    result = []
    keys = table.headings
    for row in table:
        result.append(
            {keys[i]: parse_values_to_comparable(row[i]) for i in range(len(row))})
    return result


def driver_value_to_comparable(val):
    if isinstance(val, list):
        l = [driver_value_to_comparable(v) for v in val]
        return l
    else:
        return TestValue(val)


def driver_single_result_to_comparable(record):
    return [{key: driver_value_to_comparable(record[key]) for key in record}]


def driver_result_to_comparable_result(result):
    records = []
    for record in result:
        records.append({key: driver_value_to_comparable(record[key]) for key in record})
    return records


def unordered_equal(given, expected):
    l1 = given[:]
    l2 = expected[:]
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
