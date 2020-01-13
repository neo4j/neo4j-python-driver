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


from math import isnan

from pytest import raises


def test_null(cypher_eval):
    assert cypher_eval("RETURN null") is None


def test_boolean_true(cypher_eval):
    assert cypher_eval("RETURN true") is True


def test_boolean_false(cypher_eval):
    assert cypher_eval("RETURN false") is False


def test_integer(cypher_eval):
    assert cypher_eval("RETURN 123456789") == 123456789


def test_float(cypher_eval):
    assert cypher_eval("RETURN 3.1415926") == 3.1415926


def test_float_nan(cypher_eval):
    assert isnan(cypher_eval("WITH $x AS x RETURN x", x=float("NaN")))


def test_float_positive_infinity(cypher_eval):
    infinity = float("+Inf")
    assert cypher_eval("WITH $x AS x RETURN x", x=infinity) == infinity


def test_float_negative_infinity(cypher_eval):
    infinity = float("-Inf")
    assert cypher_eval("WITH $x AS x RETURN x", x=infinity) == infinity


def test_string(cypher_eval):
    assert cypher_eval("RETURN 'hello, world'") == "hello, world"


def test_bytes(cypher_eval):
    data = bytearray([0x00, 0x33, 0x66, 0x99, 0xCC, 0xFF])
    assert cypher_eval("CREATE (a {x:$x}) RETURN a.x", x=data) == data


def test_list(cypher_eval):
    data = ["one", "two", "three"]
    assert cypher_eval("WITH $x AS x RETURN x", x=data) == data


def test_map(cypher_eval):
    data = {"one": "eins", "two": "zwei", "three": "drei"}
    assert cypher_eval("WITH $x AS x RETURN x", x=data) == data


def test_non_string_map_keys(session):
    with raises(TypeError):
        _ = session.run("RETURN $x", x={1: 'eins', 2: 'zwei', 3: 'drei'})
