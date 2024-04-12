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
import pytz

from neo4j.data import (
    Graph,
    Node,
    Path,
    Record,
    Relationship,
)
from neo4j.spatial import (
    CartesianPoint,
    WGS84Point,
)
from neo4j.time import (
    Date,
    DateTime,
    Duration,
    Time,
)

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


_RECORD_DATA_ALICE_KEYS = ["name", "age", "married"]
_RECORD_DATA_ALICE_VALUES = ["Alice", 33, True]
_RECORD_DATA_GRAPH = Graph()
_RECORD_DATA_REL_KNOWS = _RECORD_DATA_GRAPH.relationship_type("KNOWS")
_RECORD_DATA_REL_FOLLOWS = _RECORD_DATA_GRAPH.relationship_type("FOLLOWS")


def _record_data_alice_know_bob() -> Relationship:
    alice = Node(_RECORD_DATA_GRAPH, 1, ["Person"], {"name": "Alice"})
    bob = Node(_RECORD_DATA_GRAPH, 2, ["Person"], {"name": "Bob"})
    alice_knows_bob = _RECORD_DATA_REL_KNOWS(_RECORD_DATA_GRAPH, 4,
                                             {"proper": "tea"})
    alice_knows_bob._start_node = alice
    alice_knows_bob._end_node = bob
    return alice_knows_bob


def _record_data_make_path() -> Path:
    alice_knows_bob = _record_data_alice_know_bob()
    alice = alice_knows_bob.start_node
    assert alice is not None
    bob = alice_knows_bob.end_node
    carlos = Node(_RECORD_DATA_GRAPH, 3, ["Person"], {"name": "Carlos"})
    carlos_follows_bob = _RECORD_DATA_REL_FOLLOWS(_RECORD_DATA_GRAPH, 5,
                                                  {"proper": "tea"})
    carlos_follows_bob._start_node = carlos
    carlos_follows_bob._end_node = bob
    return Path(alice, alice_knows_bob, carlos_follows_bob)


@pytest.mark.parametrize(
    ("keys", "expected"),
    (
        (
            (),
            {"name": "Alice", "age": 33, "married": True},
        ),
        (
            ("name",),
            {"name": "Alice"},
        ),
        (
            ("age", "name"),
            {"age": 33, "name": "Alice"},
        ),
        (
            ("age", "name", "shoe size"),
            {"age": 33, "name": "Alice", "shoe size": None},
        ),
        (
            ("age", "name", "shoe size"),
            {"age": 33, "name": "Alice", "shoe size": None},
        ),
        (
            ("age", "name", "shoe size"),
            {"age": 33, "name": "Alice", "shoe size": None},
        ),
        (
            (0, "name"),
            {"name": "Alice"},
        ),
        (
            (0,),
            {"name": "Alice"},
        ),
        (
            (1, 0),
            {"age": 33, "name": "Alice"},
        ),
    ),
)
def test_record_data_keys(keys, expected):
    record = Record(zip(_RECORD_DATA_ALICE_KEYS, _RECORD_DATA_ALICE_VALUES))
    assert record.data(*keys) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    (
        *(
            (value, value)
            for value in (
                None,
                True,
                False,
                0,
                1,
                -1,
                2147483647,
                -2147483648,
                3.141592653589,
                "",
                "Hello, world!",
                "👋, 🌍!",
                [],
                [1, 2.0, "3", True, None],
                {"foo": ["bar", 1]},
                b"",
                b"foobar",
                Date(2021, 1, 1),
                Time(12, 34, 56, 123456789),
                Time(1, 2, 3, 4, pytz.FixedOffset(60)),
                DateTime(2021, 1, 1, 12, 34, 56, 123456789),
                DateTime(2018, 10, 12, 11, 37, 41, 474716862,
                         pytz.FixedOffset(60)),
                pytz.timezone("Europe/Stockholm").localize(
                    DateTime(2018, 10, 12, 11, 37, 41, 474716862)
                ),
                Duration(1, 2, 3, 4, 5, 6, 7),
                CartesianPoint((1, 2.0)),
                CartesianPoint((1, 2.0, 3)),
                WGS84Point((1, 2.0)),
                WGS84Point((1, 2.0, 3)),
            )
        ),
        *(
            (value, expected)
            for value, expected in (
                (
                    Node(_RECORD_DATA_GRAPH, 1, ["Person"], {"name": "Alice"}),
                    {"name": "Alice"},
                ),
                (
                    _record_data_alice_know_bob(),
                    (
                        {"name": "Alice"},
                        "KNOWS",
                        {"name": "Bob"},
                    )
                ),
                (
                    _record_data_make_path(),
                    [
                        {"name": "Alice"},
                        "KNOWS",
                        {"name": "Bob"},
                        "FOLLOWS",
                        {"name": "Carlos"},
                    ]
                ),
            )
        ),
    )
)
@pytest.mark.parametrize("wrapper", (None, lambda x: [x], lambda x: {"x": x}))
def test_record_data_types(value, expected, wrapper):
    if wrapper is not None:
        value = wrapper(value)
        expected = wrapper(expected)
    record = Record([("key", value)])
    assert record.data("key") == {"key": expected}


def test_record_index_error():
    record = Record(zip(_RECORD_DATA_ALICE_KEYS, _RECORD_DATA_ALICE_VALUES))
    with pytest.raises(IndexError):
        record.data(1, 0, 999)


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


def test_record_value_kwargs():
    r = Record(zip(["name", "age", "married"], ["Alice", 33, True]))
    assert r.value() == "Alice"
    assert r.value(key="name") == "Alice"
    assert r.value(key="age") == 33
    assert r.value(key="married") is True
    assert r.value(key="shoe size") is None
    assert r.value(key="shoe size", default=6) == 6
    assert r.value(key=0) == "Alice"
    assert r.value(key=1) == 33
    assert r.value(key=2) is True
    assert r.value(key=3) is None
    assert r.value(key=3, default=6) == 6


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


def test_record_get_item():
    r = Record(zip(["x", "y"], ["foo", "bar"]))
    assert r["x"] == "foo"
    assert r["y"] == "bar"
    with pytest.raises(KeyError):
        _ = r["z"]
    with pytest.raises(TypeError):
        _ = r[object()]


@pytest.mark.parametrize("len_", (0, 1, 2, 42))
def test_record_len_generic(len_):
    r = Record(("key_%i" % i, "val_%i" % i) for i in range(len_))
    assert len(r) == len_


@pytest.mark.parametrize("len_", range(3))
def test_record_repr_generic(len_):
    r = Record(("key_%i" % i, "val_%i" % i) for i in range(len_))
    assert repr(r)


@pytest.mark.parametrize(("raw", "keys", "serialized"), (
    (
        zip(["x", "y", "z"], [1, 2, 3]),
        (),
        {"x": 1, "y": 2, "z": 3}
    ),
    (
        zip(["x", "y", "z"], [1, 2, 3]),
        (1, 2),
        {"y": 2, "z": 3}
    ),
    (
        zip(["x", "y", "z"], [1, 2, 3]),
        ("z", "x"),
        {"x": 1, "z": 3}
    ),
    (
        zip(["x"], [None]),
        (),
        {"x": None}
    ),
    (
        zip(["x", "y"], [True, False]),
        (),
        {"x": True, "y": False}
    ),
    (
        zip(["x", "y", "z"], [0.0, 1.0, 3.141592653589]),
        (),
        {"x": 0.0, "y": 1.0, "z": 3.141592653589}
    ),
    (
        zip(["x"], ["hello, world"]),
        (),
        {"x": "hello, world"}
    ),
    (
        zip(["x"], [bytearray([1, 2, 3])]),
        (),
        {"x": bytearray([1, 2, 3])}
    ),
    (
        zip(["x"], [[1, 2, 3]]),
        (),
        {"x": [1, 2, 3]}
    ),
    (
        zip(["x"], [{"one": 1, "two": 2}]),
        (),
        {"x": {"one": 1, "two": 2}}
    ),
    (
        zip(
            ["a"],
            [Node(None, 42, "Person", {"name": "Alice"})]
        ),
        (),
        {"a": {"name": "Alice"}}
    ),
))
def test_data(raw, keys, serialized):
    assert Record(raw).data(*keys) == serialized


def test_data_relationship():
    g = Graph()
    gh = Graph.Hydrator(g)
    alice = gh.hydrate_node(1, {"Person"}, {"name": "Alice", "age": 33})
    bob = gh.hydrate_node(2, {"Person"}, {"name": "Bob", "age": 44})
    alice_knows_bob = gh.hydrate_relationship(1, alice.id, bob.id, "KNOWS",
                                              {"since": 1999})
    record = Record(zip(["a", "b", "r"], [alice, bob, alice_knows_bob]))
    assert record.data() == {
        "a": {"name": "Alice", "age": 33},
        "b": {"name": "Bob", "age": 44},
        "r": (
            {"name": "Alice", "age": 33},
            "KNOWS",
            {"name": "Bob", "age": 44}
        ),
    }


def test_data_unbound_relationship():
    g = Graph()
    gh = Graph.Hydrator(g)
    some_one_knows_some_one = gh.hydrate_relationship(
        1, 42, 43, "KNOWS", {"since": 1999}
    )
    record = Record(zip(["r"], [some_one_knows_some_one]))
    assert record.data() == {"r": ({}, "KNOWS", {})}


@pytest.mark.parametrize("cyclic", (True, False))
def test_data_path(cyclic):
    g = Graph()
    gh = Graph.Hydrator(g)
    alice = gh.hydrate_node(1, {"Person"}, {"name": "Alice", "age": 33})
    bob = gh.hydrate_node(2, {"Person"}, {"name": "Bob", "age": 44})
    if cyclic:
        carol = alice
    else:
        carol = gh.hydrate_node(3, {"Person"}, {"name": "Carol", "age": 55})
    r = [gh.hydrate_unbound_relationship(1, "KNOWS", {"since": 1999}),
         gh.hydrate_unbound_relationship(2, "DISLIKES", {})]
    path = gh.hydrate_path([alice, bob, carol], r, [1, 1, -2, 2])

    record = Record(zip(["r"], [path]))
    assert record.data() == {
        "r": [dict(alice), "KNOWS", dict(bob), "DISLIKES", dict(carol)]
    }
