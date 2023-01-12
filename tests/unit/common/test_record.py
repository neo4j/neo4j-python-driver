# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from __future__ import annotations

import traceback

import pytest

from neo4j import Record
from neo4j._codec.hydration import BrokenHydrationObject
from neo4j._codec.hydration.v1 import HydrationHandler
from neo4j.exceptions import BrokenRecordError
from neo4j.graph import Node


# python -m pytest -s -v tests/unit/test_record.py


def test_record_equality() -> None:
    record1 = Record(zip(["name", "empire"], ["Nigel", "The British Empire"]))
    record2 = Record(zip(["name", "empire"], ["Nigel", "The British Empire"]))
    record3 = Record(zip(["name", "empire"], ["Stefan", "Das Deutschland"]))
    assert record1 == record2
    assert record1 != record3
    assert record2 != record3


def test_record_hashing() -> None:
    record1 = Record(zip(["name", "empire"], ["Nigel", "The British Empire"]))
    record2 = Record(zip(["name", "empire"], ["Nigel", "The British Empire"]))
    record3 = Record(zip(["name", "empire"], ["Stefan", "Das Deutschland"]))
    assert hash(record1) == hash(record2)
    assert hash(record1) != hash(record3)
    assert hash(record2) != hash(record3)


def test_record_iter() -> None:
    a_record = Record(zip(["name", "empire"], ["Nigel", "The British Empire"]))
    assert list(a_record.__iter__()) == ["Nigel", "The British Empire"]


def test_record_as_dict() -> None:
    a_record = Record(zip(["name", "empire"], ["Nigel", "The British Empire"]))
    assert dict(a_record) == {"name": "Nigel", "empire": "The British Empire"}


def test_record_as_list() -> None:
    a_record = Record(zip(["name", "empire"], ["Nigel", "The British Empire"]))
    assert list(a_record) == ["Nigel", "The British Empire"]


def test_record_len() -> None:
    a_record = Record(zip(["name", "empire"], ["Nigel", "The British Empire"]))
    assert len(a_record) == 2


def test_record_repr() -> None:
    a_record = Record(zip(["name", "empire"], ["Nigel", "The British Empire"]))
    assert repr(a_record) == "<Record name='Nigel' empire='The British Empire'>"


def test_record_data() -> None:
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


def test_record_keys() -> None:
    r = Record(zip(["name", "age", "married"], ["Alice", 33, True]))
    assert r.keys() == ["name", "age", "married"]


def test_record_values() -> None:
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


def test_record_items() -> None:
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


def test_record_index() -> None:
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
        _ = r.index(None)  # type: ignore[arg-type]


def test_record_value() -> None:
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
        _ = r.value(None)  # type: ignore[arg-type]


def test_record_value_kwargs() -> None:
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


def test_record_contains() -> None:
    r = Record(zip(["name", "age", "married"], ["Alice", 33, True]))
    assert "Alice" in r
    assert 33 in r
    assert True in r
    assert 7.5 not in r
    with pytest.raises(TypeError):
        _ = r.index(None)  # type: ignore[arg-type]


def test_record_from_dict() -> None:
    r = Record({"name": "Alice", "age": 33})
    assert r["name"] == "Alice"
    assert r["age"] == 33


def test_record_get_slice() -> None:
    r = Record(zip(["name", "age", "married"], ["Alice", 33, True]))
    assert Record(zip(["name", "age"], ["Alice", 33])) == r[0:2]


def test_record_get_by_index() -> None:
    r = Record(zip(["name", "age", "married"], ["Alice", 33, True]))
    assert r[0] == "Alice"


def test_record_get_by_name() -> None:
    r = Record(zip(["name", "age", "married"], ["Alice", 33, True]))
    assert r["name"] == "Alice"


def test_record_get_by_out_of_bounds_index() -> None:
    r = Record(zip(["name", "age", "married"], ["Alice", 33, True]))
    assert r[9] is None


def test_record_get_item() -> None:
    r = Record(zip(["x", "y"], ["foo", "bar"]))
    assert r["x"] == "foo"
    assert r["y"] == "bar"
    with pytest.raises(KeyError):
        _ = r["z"]
    with pytest.raises(TypeError):
        _ = r[object()]  # type: ignore[index]


@pytest.mark.parametrize("len_", (0, 1, 2, 42))
def test_record_len_generic(len_) -> None:
    r = Record(("key_%i" % i, "val_%i" % i) for i in range(len_))
    assert len(r) == len_


@pytest.mark.parametrize("len_", range(3))
def test_record_repr_generic(len_) -> None:
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
            [Node(
                None,  # type: ignore[arg-type]
                "42", 42, "Person", {"name": "Alice"}
            )]),
        (),
        {"a": {"name": "Alice"}}
    ),
))
def test_data(raw, keys, serialized) -> None:
    assert Record(raw).data(*keys) == serialized


def test_data_relationship() -> None:
    hydration_scope = HydrationHandler().new_hydration_scope()
    gh = hydration_scope._graph_hydrator
    alice = gh.hydrate_node(1, {"Person"}, {"name": "Alice", "age": 33})
    bob = gh.hydrate_node(2, {"Person"}, {"name": "Bob", "age": 44})
    alice_knows_bob = gh.hydrate_relationship(1, 1, 2, "KNOWS",
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


def test_data_unbound_relationship() -> None:
    hydration_scope = HydrationHandler().new_hydration_scope()
    gh = hydration_scope._graph_hydrator
    some_one_knows_some_one = gh.hydrate_relationship(
        1, 42, 43, "KNOWS", {"since": 1999}
    )
    record = Record(zip(["r"], [some_one_knows_some_one]))
    assert record.data() == {"r": ({}, "KNOWS", {})}


@pytest.mark.parametrize("cyclic", (True, False))
def test_data_path(cyclic) -> None:
    hydration_scope = HydrationHandler().new_hydration_scope()
    gh = hydration_scope._graph_hydrator
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


@pytest.mark.parametrize(("accessor", "should_raise"), (
    (lambda r: r["a"], False),
    (lambda r: r["b"], True),
    (lambda r: r["c"], False),
    (lambda r: r[0], False),
    (lambda r: r[1], True),
    (lambda r: r[2], False),
    (lambda r: r.value("a"), False),
    (lambda r: r.value("b"), True),
    (lambda r: r.value("c"), False),
    (lambda r: r.value("made-up"), False),
    (lambda r: r.value(0), False),
    (lambda r: r.value(1), True),
    (lambda r: r.value(2), False),
    (lambda r: r.value(3), False),
    (lambda r: r.values(0, 2, "made-up"), False),
    (lambda r: r.values(0, 1), True),
    (lambda r: r.values(2, 1), True),
    (lambda r: r.values(1), True),
    (lambda r: r.values(1, "made-up"), True),
    (lambda r: r.values(1, 0), True),
    (lambda r: r.values("a", "c", "made-up"), False),
    (lambda r: r.values("a", "b"), True),
    (lambda r: r.values("c", "b"), True),
    (lambda r: r.values("b"), True),
    (lambda r: r.values("b", "made-up"), True),
    (lambda r: r.values("b", "a"), True),
    (lambda r: r.data(0, 2, "made-up"), False),
    (lambda r: r.data(0, 1), True),
    (lambda r: r.data(2, 1), True),
    (lambda r: r.data(1), True),
    (lambda r: r.data(1, "made-up"), True),
    (lambda r: r.data(1, 0), True),
    (lambda r: r.data("a", "c", "made-up"), False),
    (lambda r: r.data("a", "b"), True),
    (lambda r: r.data("c", "b"), True),
    (lambda r: r.data("b"), True),
    (lambda r: r.data("b", "made-up"), True),
    (lambda r: r.data("b", "a"), True),
    (lambda r: r.get("a"), False),
    (lambda r: r.get("b"), True),
    (lambda r: r.get("c"), False),
    (lambda r: r.get("made-up"), False),
    (lambda r: list(r), True),
    (lambda r: list(r.items()), True),
    (lambda r: r.index("a"), False),
    (lambda r: r.index("b"), False),
    (lambda r: r.index("c"), False),
    (lambda r: r.index(0), False),
    (lambda r: r.index(1), False),
    (lambda r: r.index(2), False),
))
def test_record_with_error(accessor, should_raise) -> None:
    class TestException(Exception):
        pass

    # raising and catching exceptions to get the stacktrace populated
    try:
        raise TestException("test")
    except TestException as e:
        exc = e
    frames = list(traceback.walk_tb(exc.__traceback__))
    r = Record((("a", 1), ("b", BrokenHydrationObject(exc, None)), ("c", 3)))
    if not should_raise:
        accessor(r)
        return
    with pytest.raises(BrokenRecordError) as raised:
        accessor(r)
    exc_value = raised.value
    assert exc_value.__cause__ is exc
    assert list(traceback.walk_tb(exc_value.__cause__.__traceback__)) == frames
