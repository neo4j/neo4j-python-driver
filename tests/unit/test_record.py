#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) "Neo4j"
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


import pytest

from neo4j.data import Record

# python -m pytest -s -v tests/unit/test_record.py


def test_record_equality():
    record1 = Record(zip(["name", "empire"], ["Nigel", "The British Empire"]))
    record2 = Record(zip(["name", "empire"], ["Nigel", "The British Empire"]))
    record3 = Record(zip(["name", "empire"], ["Stefan", "Das Deutschland"]))
    assert record1 == record2
    assert record1 != record3
    assert record2 != record3


def test_record_hashing():
    record1 = Record(zip(["name", "empire"], ["Nigel", "The British Empire"]))
    record2 = Record(zip(["name", "empire"], ["Nigel", "The British Empire"]))
    record3 = Record(zip(["name", "empire"], ["Stefan", "Das Deutschland"]))
    assert hash(record1) == hash(record2)
    assert hash(record1) != hash(record3)
    assert hash(record2) != hash(record3)


def test_record_iter():
    a_record = Record(zip(["name", "empire"], ["Nigel", "The British Empire"]))
    assert list(a_record.__iter__()) == ["Nigel", "The British Empire"]


def test_record_as_dict():
    a_record = Record(zip(["name", "empire"], ["Nigel", "The British Empire"]))
    assert dict(a_record) == {"name": "Nigel", "empire": "The British Empire"}


def test_record_as_list():
    a_record = Record(zip(["name", "empire"], ["Nigel", "The British Empire"]))
    assert list(a_record) == ["Nigel", "The British Empire"]


def test_record_len():
    a_record = Record(zip(["name", "empire"], ["Nigel", "The British Empire"]))
    assert len(a_record) == 2


def test_record_repr():
    a_record = Record(zip(["name", "empire"], ["Nigel", "The British Empire"]))
    assert repr(a_record) == "<Record name='Nigel' empire='The British Empire'>"


def test_record_data():
    r = Record(zip(["name", "age", "married"], ["Alice", 33, True]))
    assert r.data() == {"name": "Alice", "age": 33, "married": True}
    assert r.data("name") == {"name": "Alice"}
    assert r.data("age", "name") == {"age": 33, "name": "Alice"}
    assert r.data("age", "name", "shoe size") == {"age": 33, "name": "Alice", "shoe size": None}
    assert r.data(0, "name") == {"name": "Alice"}
    assert r.data(0) == {"name": "Alice"}
    assert r.data(1, 0) == {"age": 33, "name": "Alice"}
    with pytest.raises(IndexError):
        _ = r.data(1, 0, 999)


def test_record_keys():
    r = Record(zip(["name", "age", "married"], ["Alice", 33, True]))
    assert r.keys() == ["name", "age", "married"]


def test_record_values():
    r = Record(zip(["name", "age", "married"], ["Alice", 33, True]))
    assert r.values() == ["Alice", 33, True]
    assert r.values("name") == ["Alice"]
    assert r.values("age", "name") == [33, "Alice"]
    assert r.values("age", "name", "shoe size") == [33, "Alice", None]
    assert r.values(0, "name") == ["Alice", "Alice"]
    assert r.values(0) == ["Alice"]
    assert r.values(1, 0) == [33, "Alice"]
    with pytest.raises(IndexError):
        _ = r.values(1, 0, 999)


def test_record_items():
    r = Record(zip(["name", "age", "married"], ["Alice", 33, True]))
    assert r.items() == [("name", "Alice"), ("age", 33), ("married", True)]
    assert r.items("name") == [("name", "Alice")]
    assert r.items("age", "name") == [("age", 33), ("name", "Alice")]
    assert r.items("age", "name", "shoe size") == [("age", 33), ("name", "Alice"), ("shoe size", None)]
    assert r.items(0, "name") == [("name", "Alice"), ("name", "Alice")]
    assert r.items(0) == [("name", "Alice")]
    assert r.items(1, 0) == [("age", 33), ("name", "Alice")]
    with pytest.raises(IndexError):
        _ = r.items(1, 0, 999)


def test_record_index():
    r = Record(zip(["name", "age", "married"], ["Alice", 33, True]))
    assert r.index("name") == 0
    assert r.index("age") == 1
    assert r.index("married") == 2
    with pytest.raises(KeyError):
        _ = r.index("shoe size")
    assert r.index(0) == 0
    assert r.index(1) == 1
    assert r.index(2) == 2
    with pytest.raises(IndexError):
        _ = r.index(3)
    with pytest.raises(TypeError):
        _ = r.index(None)


def test_record_value():
    r = Record(zip(["name", "age", "married"], ["Alice", 33, True]))
    assert r.value() == "Alice"
    assert r.value("name") == "Alice"
    assert r.value("age") == 33
    assert r.value("married") is True
    assert r.value("shoe size") is None
    assert r.value("shoe size", 6) == 6
    assert r.value(0) == "Alice"
    assert r.value(1) == 33
    assert r.value(2) is True
    assert r.value(3) is None
    assert r.value(3, 6) == 6
    with pytest.raises(TypeError):
        _ = r.value(None)


def test_record_contains():
    r = Record(zip(["name", "age", "married"], ["Alice", 33, True]))
    assert "Alice" in r
    assert 33 in r
    assert True in r
    assert 7.5 not in r
    with pytest.raises(TypeError):
        _ = r.index(None)


def test_record_from_dict():
    r = Record({"name": "Alice", "age": 33})
    assert r["name"] == "Alice"
    assert r["age"] == 33


def test_record_get_slice():
    r = Record(zip(["name", "age", "married"], ["Alice", 33, True]))
    assert Record(zip(["name", "age"], ["Alice", 33])) == r[0:2]


def test_record_get_by_index():
    r = Record(zip(["name", "age", "married"], ["Alice", 33, True]))
    assert r[0] == "Alice"


def test_record_get_by_name():
    r = Record(zip(["name", "age", "married"], ["Alice", 33, True]))
    assert r["name"] == "Alice"


def test_record_get_by_out_of_bounds_index():
    r = Record(zip(["name", "age", "married"], ["Alice", 33, True]))
    assert r[9] is None
