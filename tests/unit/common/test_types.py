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


from itertools import product

import pytest

from neo4j._codec.hydration.v1 import HydrationHandler
from neo4j.graph import (
    Graph,
    Node,
    Path,
    Relationship,
)


# python -m pytest -s -v tests/unit/test_types.py


# Node


@pytest.mark.parametrize(("id_", "element_id"), (
    (123, "123"),
    (123, None),
    (None, "foobar"),
))
def test_can_create_node(id_, element_id):
    hydration_scope = HydrationHandler().new_hydration_scope()
    gh = hydration_scope._graph_hydrator

    fields = [id_, {"Person"}, {"name": "Alice", "age": 33}]
    if element_id is not None:
        fields.append(element_id)
    alice = gh.hydrate_node(*fields)
    assert isinstance(alice, Node)
    with pytest.warns(DeprecationWarning, match="element_id"):
        assert alice.id == id_
    if element_id is not None:
        assert alice.element_id == element_id
    else:
        assert alice.element_id == str(id_)
    assert alice.labels == {"Person"}
    assert set(alice.keys()) == {"name", "age"}
    assert set(alice.values()) == {"Alice", 33}
    assert set(alice.items()) == {("name", "Alice"), ("age", 33)}
    assert dict(alice) == {"name": "Alice", "age": 33}
    assert alice.get("name") == "Alice"
    assert alice.get("age") == 33
    assert alice.get("unknown") is None
    assert alice.get("unknown", None) is None
    assert alice.get("unknown", False) is False
    assert alice.get("unknown", "default") == "default"
    assert len(alice) == 2
    assert alice["name"] == "Alice"
    assert alice["age"] == 33
    assert "name" in alice
    assert "age" in alice
    assert set(iter(alice)) == {"name", "age"}


def test_node_with_null_properties():
    hydration_scope = HydrationHandler().new_hydration_scope()
    gh = hydration_scope._graph_hydrator
    stuff = gh.hydrate_node(1, (), {"good": ["puppies", "kittens"],
                                    "bad": None})
    assert isinstance(stuff, Node)
    assert set(stuff.keys()) == {"good"}
    assert stuff.get("good") == ["puppies", "kittens"]
    assert stuff.get("bad") is None
    assert len(stuff) == 1
    assert stuff["good"] == ["puppies", "kittens"]
    assert stuff["bad"] is None
    assert "good" in stuff
    assert "bad" not in stuff


@pytest.mark.parametrize(("g1", "id1", "eid1", "props1",
                          "g2", "id2", "eid2", "props2"), (
    (*n1, *n2)
    for n1, n2 in product(
        (  # type: ignore
            (g, id_, element_id, props)  # type: ignore
            for g in (0, 1)
            for id_, element_id in (
                (1, "1"),
                (1234, "1234"),
                (None, "1234"),
                (None, "foobar"),
            )
            for props in (None, {}, {"a": 1})
        ),
        repeat=2
    )
))
def test_node_equality(g1, id1, eid1, props1, g2, id2, eid2, props2):
    graphs = (Graph(), Graph())
    node_1 = Node(graphs[g1], eid1, id1, props1)
    node_2 = Node(graphs[g2], eid2, id2, props2)
    if g1 == g2 and eid1 == eid2:
        assert node_1 == node_2
    else:
        assert node_1 != node_2
    assert node_1 != "this is not a node"


@pytest.mark.parametrize("legacy_id", (True, False))
def test_node_hashing(legacy_id):
    g = Graph()
    node_1 = Node(g, "1234" + ("abc" if not legacy_id else ""), 1234)
    node_2 = Node(g, "1234" + ("abc" if not legacy_id else ""), 1234)
    node_3 = Node(g, "5678" + ("abc" if not legacy_id else ""), 5678)
    assert hash(node_1) == hash(node_2)
    assert hash(node_1) != hash(node_3)


def test_node_v1_repr():
    hydration_scope = HydrationHandler().new_hydration_scope()
    gh = hydration_scope._graph_hydrator
    alice = gh.hydrate_node(1, {"Person"}, {"name": "Alice"})
    assert repr(alice) == (
        "<Node element_id='1' labels=frozenset({'Person'}) "
        "properties={'name': 'Alice'}>"
    )


@pytest.mark.parametrize("legacy_id", (True, False))
def test_node_v2_repr(legacy_id):
    hydration_scope = HydrationHandler().new_hydration_scope()
    gh = hydration_scope._graph_hydrator
    id_ = 1234
    element_id = str(id_) if legacy_id else "foobar"
    alice = gh.hydrate_node(id_, {"Person"}, {"name": "Alice"}, element_id)
    assert repr(alice) == (
        f"<Node element_id={element_id!r} "
        "labels=frozenset({'Person'}) properties={'name': 'Alice'}>"
    )


# Relationship


def test_can_create_relationship_v1():
    hydration_scope = HydrationHandler().new_hydration_scope()
    gh = hydration_scope._graph_hydrator
    alice = gh.hydrate_node(1, {"Person"}, {"name": "Alice", "age": 33})
    bob = gh.hydrate_node(2, {"Person"}, {"name": "Bob", "age": 44})
    alice_knows_bob = gh.hydrate_relationship(1, 1, 2, "KNOWS",
                                              {"since": 1999})
    assert isinstance(alice_knows_bob, Relationship)
    assert alice_knows_bob.start_node == alice
    assert alice_knows_bob.type == "KNOWS"
    assert alice_knows_bob.end_node == bob
    assert dict(alice_knows_bob) == {"since": 1999}
    assert set(alice_knows_bob.keys()) == {"since"}
    assert set(alice_knows_bob.values()) == {1999}
    assert set(alice_knows_bob.items()) == {("since", 1999)}
    assert alice_knows_bob.get("since") == 1999


@pytest.mark.parametrize("legacy_id", (True, False))
def test_can_create_relationship_v2(legacy_id):
    hydration_scope = HydrationHandler().new_hydration_scope()
    gh = hydration_scope._graph_hydrator
    alice = gh.hydrate_node(
        1, {"Person"}, {"name": "Alice", "age": 33},
        "1" if legacy_id else "alice"
    )
    bob = gh.hydrate_node(
        2, {"Person"}, {"name": "Bob", "age": 44},
        "2" if legacy_id else "bob"
    )
    alice_knows_bob = gh.hydrate_relationship(
        1,
        1, 2,
        "KNOWS", {"since": 1999},
        "1" if legacy_id else "alice_knows_bob",
        "1" if legacy_id else "alice", "2" if legacy_id else "bob"
    )
    assert isinstance(alice_knows_bob, Relationship)
    assert alice_knows_bob.start_node == alice
    assert alice_knows_bob.type == "KNOWS"
    assert alice_knows_bob.end_node == bob
    assert dict(alice_knows_bob) == {"since": 1999}
    assert set(alice_knows_bob.keys()) == {"since"}
    assert set(alice_knows_bob.values()) == {1999}
    assert set(alice_knows_bob.items()) == {("since", 1999)}
    assert alice_knows_bob.get("since") == 1999


def test_relationship_v1_repr():
    hydration_scope = HydrationHandler().new_hydration_scope()
    gh = hydration_scope._graph_hydrator
    _alice = gh.hydrate_node(1, {"Person"}, {"name": "Alice"})
    _bob = gh.hydrate_node(2, {"Person"}, {"name": "Bob"})
    alice_knows_bob = gh.hydrate_relationship(3, 1, 2, "KNOWS",
                                              {"since": 1999})
    assert repr(alice_knows_bob) == (
        "<Relationship element_id='3' "
        "nodes=(<Node element_id='1' labels=frozenset({'Person'}) "
        "properties={'name': 'Alice'}>, <Node element_id='2' "
        "labels=frozenset({'Person'}) properties={'name': 'Bob'}>) "
        "type='KNOWS' properties={'since': 1999}>"
    )


@pytest.mark.parametrize("legacy_id", (True, False))
def test_relationship_v2_repr(legacy_id):
    hydration_scope = HydrationHandler().new_hydration_scope()
    gh = hydration_scope._graph_hydrator
    gh.hydrate_node(
        1, {"Person"}, {"name": "Alice"},
        "1" if legacy_id else "alice"
    )
    gh.hydrate_node(
        2, {"Person"}, {"name": "Bob"},
        "2" if legacy_id else "bob"
    )
    alice_knows_bob = gh.hydrate_relationship(
        1, 1, 2, "KNOWS", {"since": 1999},
        "1" if legacy_id else "alice_knows_bob",
        "1" if legacy_id else "alice", "2" if legacy_id else "bob"
    )
    expected_eid = "1" if legacy_id else "alice_knows_bob"
    expected_eid_alice = "1" if legacy_id else "alice"
    expected_eid_bob = "2" if legacy_id else "bob"
    assert repr(alice_knows_bob) == (
        f"<Relationship element_id={expected_eid!r} "
        f"nodes=(<Node element_id={expected_eid_alice!r} "
        "labels=frozenset({'Person'}) properties={'name': 'Alice'}>, "
        f"<Node element_id={expected_eid_bob!r} "
        "labels=frozenset({'Person'}) properties={'name': 'Bob'}>) "
        "type='KNOWS' properties={'since': 1999}>"
    )


# Path

def test_can_create_path_v1():
    hydration_scope = HydrationHandler().new_hydration_scope()
    gh = hydration_scope._graph_hydrator
    alice = gh.hydrate_node(1, {"Person"}, {"name": "Alice", "age": 33})
    bob = gh.hydrate_node(2, {"Person"}, {"name": "Bob", "age": 44})
    carol = gh.hydrate_node(3, {"Person"}, {"name": "Carol", "age": 55})
    alice_knows_bob = gh.hydrate_relationship(1, 1, 2, "KNOWS",
                                              {"since": 1999})
    carol_dislikes_bob = gh.hydrate_relationship(2, 3, 2, "DISLIKES", {})
    path = Path(alice, alice_knows_bob, carol_dislikes_bob)
    assert isinstance(path, Path)
    assert path.start_node is alice
    assert path.end_node is carol
    assert path.nodes == (alice, bob, carol)
    assert path.relationships == (alice_knows_bob, carol_dislikes_bob)
    assert list(path) == [alice_knows_bob, carol_dislikes_bob]


@pytest.mark.parametrize("legacy_id", (True, False))
def test_can_create_path_v2(legacy_id):
    hydration_scope = HydrationHandler().new_hydration_scope()
    gh = hydration_scope._graph_hydrator
    alice = gh.hydrate_node(
        1, {"Person"}, {"name": "Alice", "age": 33},
        "1" if legacy_id else "alice"
    )
    bob = gh.hydrate_node(
        2, {"Person"}, {"name": "Bob", "age": 44},
        "2" if legacy_id else "bob"
    )
    carol = gh.hydrate_node(
        3, {"Person"}, {"name": "Carol", "age": 55},
        "3" if legacy_id else "carol"
    )
    alice_knows_bob = gh.hydrate_relationship(
        1, 1, 2, "KNOWS",  {"since": 1999},
        "1" if legacy_id else "alice_knows_bob",
        "1" if legacy_id else "alice", "2" if legacy_id else "bob"

    )
    carol_dislikes_bob = gh.hydrate_relationship(
        2, 3, 2, "DISLIKES", {},
        "2" if legacy_id else "carol_dislikes_bob",
        "3" if legacy_id else "carol", "2" if legacy_id else "bob"
    )
    path = Path(alice, alice_knows_bob, carol_dislikes_bob)
    assert isinstance(path, Path)
    assert path.start_node is alice
    assert path.end_node is carol
    assert path.nodes == (alice, bob, carol)
    assert path.relationships == (alice_knows_bob, carol_dislikes_bob)
    assert list(path) == [alice_knows_bob, carol_dislikes_bob]


@pytest.mark.parametrize("cyclic", (True, False))
def test_can_hydrate_path(cyclic):
    hydration_scope = HydrationHandler().new_hydration_scope()
    gh = hydration_scope._graph_hydrator
    alice = gh.hydrate_node(1, {"Person"}, {"name": "Alice", "age": 33}, "1")
    bob = gh.hydrate_node(2, {"Person"}, {"name": "Bob", "age": 44}, "2")
    if cyclic:
        carol_id = 1
        carol_eid = "1"
        carol = alice
    else:
        carol_id = 3
        carol_eid = "3"
        carol = gh.hydrate_node(carol_id, {"Person"},
                                {"name": "Carol", "age": 55}, carol_eid)
    r = [gh.hydrate_unbound_relationship(1, "KNOWS", {"since": 1999}, "1"),
         gh.hydrate_unbound_relationship(2, "DISLIKES", {}), "2"]
    path = gh.hydrate_path([alice, bob, carol], r, [1, 1, -2, 2])
    assert path.start_node is alice
    assert path.end_node is carol
    assert path.nodes == (alice, bob, carol)
    expected_alice_knows_bob = gh.hydrate_relationship(
        1, 1, 2, "KNOWS", {"since": 1999}, "1", "1", "2"
    )
    expected_carol_dislikes_bob = gh.hydrate_relationship(
        2, carol_id, 2, "DISLIKES", {}, "2", carol_eid, "2"
    )
    assert path.relationships == (expected_alice_knows_bob,
                                  expected_carol_dislikes_bob)
    assert list(path) == [expected_alice_knows_bob,
                          expected_carol_dislikes_bob]


def test_path_v1_equality():
    hydration_scope = HydrationHandler().new_hydration_scope()
    gh = hydration_scope._graph_hydrator
    alice = gh.hydrate_node(1, {"Person"}, {"name": "Alice", "age": 33})
    _bob = gh.hydrate_node(2, {"Person"}, {"name": "Bob", "age": 44})
    _carol = gh.hydrate_node(3, {"Person"}, {"name": "Carol", "age": 55})
    alice_knows_bob = gh.hydrate_relationship(1, 1, 2, "KNOWS",
                                              {"since": 1999})
    carol_dislikes_bob = gh.hydrate_relationship(2, 3, 2, "DISLIKES", {})
    path_1 = Path(alice, alice_knows_bob, carol_dislikes_bob)
    path_2 = Path(alice, alice_knows_bob, carol_dislikes_bob)
    assert path_1 == path_2
    assert path_1 != "this is not a path"


@pytest.mark.parametrize("legacy_id", (True, False))
def test_path_v2_equality(legacy_id):
    hydration_scope = HydrationHandler().new_hydration_scope()
    gh = hydration_scope._graph_hydrator
    alice = gh.hydrate_node(
        1, {"Person"}, {"name": "Alice", "age": 33},
        "1" if legacy_id else "alice"
    )
    _bob = gh.hydrate_node(
        2, {"Person"}, {"name": "Bob", "age": 44},
        "2" if legacy_id else "bob"
    )
    _carol = gh.hydrate_node(
        3, {"Person"}, {"name": "Carol", "age": 55},
        "3" if legacy_id else "carol"
    )
    alice_knows_bob = gh.hydrate_relationship(
        1, 1, 2, "KNOWS", {"since": 1999},
        "1" if legacy_id else "alice_knows_bob",
        "1" if legacy_id else "alice", "2" if legacy_id else "bob"
    )
    carol_dislikes_bob = gh.hydrate_relationship(
        2, 3, 2, "DISLIKES", {},
        "2" if legacy_id else "carol_dislikes_bob",
        "3" if legacy_id else "carol", "2" if legacy_id else "bob"
    )
    path_1 = Path(alice, alice_knows_bob, carol_dislikes_bob)
    path_2 = Path(alice, alice_knows_bob, carol_dislikes_bob)
    assert path_1 == path_2
    assert path_1 != "this is not a path"


def test_path_v1_hashing():
    hydration_scope = HydrationHandler().new_hydration_scope()
    gh = hydration_scope._graph_hydrator
    alice = gh.hydrate_node(1, {"Person"}, {"name": "Alice", "age": 33})
    _bob = gh.hydrate_node(2, {"Person"}, {"name": "Bob", "age": 44})
    _carol = gh.hydrate_node(3, {"Person"}, {"name": "Carol", "age": 55})
    alice_knows_bob = gh.hydrate_relationship(1, 1, 2, "KNOWS",
                                              {"since": 1999})
    carol_dislikes_bob = gh.hydrate_relationship(2, 3, 2, "DISLIKES", {})
    path_1 = Path(alice, alice_knows_bob, carol_dislikes_bob)
    path_2 = Path(alice, alice_knows_bob, carol_dislikes_bob)
    assert hash(path_1) == hash(path_2)


@pytest.mark.parametrize("legacy_id", (True, False))
def test_path_v2_hashing(legacy_id):
    hydration_scope = HydrationHandler().new_hydration_scope()
    gh = hydration_scope._graph_hydrator
    alice = gh.hydrate_node(
        1, {"Person"}, {"name": "Alice", "age": 33},
        "1" if legacy_id else "alice"
    )
    _bob = gh.hydrate_node(
        2, {"Person"}, {"name": "Bob", "age": 44},
        "2" if legacy_id else "bob"
    )
    _carol = gh.hydrate_node(
        3, {"Person"}, {"name": "Carol", "age": 55},
        "3" if legacy_id else "carol"
    )
    alice_knows_bob = gh.hydrate_relationship(
        1, 1, 2, "KNOWS", {"since": 1999},
        "1" if legacy_id else "alice_knows_bob",
        "1" if legacy_id else "alice", "2" if legacy_id else "bob"
    )
    carol_dislikes_bob = gh.hydrate_relationship(
        2, 3, 2, "DISLIKES", {},
        "2" if legacy_id else "carol_dislikes_bob",
        "3" if legacy_id else "carol", "2" if legacy_id else "bob"
    )
    path_1 = Path(alice, alice_knows_bob, carol_dislikes_bob)
    path_2 = Path(alice, alice_knows_bob, carol_dislikes_bob)
    assert hash(path_1) == hash(path_2)


def test_path_v1_repr():
    hydration_scope = HydrationHandler().new_hydration_scope()
    gh = hydration_scope._graph_hydrator
    alice = gh.hydrate_node(1, {"Person"}, {"name": "Alice"})
    _bob = gh.hydrate_node(2, {"Person"}, {"name": "Bob"})
    _carol = gh.hydrate_node(3, {"Person"}, {"name": "Carol"})
    alice_knows_bob = gh.hydrate_relationship(1, 1, 2, "KNOWS",
                                              {"since": 1999})
    carol_dislikes_bob = gh.hydrate_relationship(2, 3, 2, "DISLIKES", {})
    path = Path(alice, alice_knows_bob, carol_dislikes_bob)
    assert repr(path) == (
        "<Path start=<Node element_id='1' labels=frozenset({'Person'}) "
        "properties={'name': 'Alice'}> end=<Node element_id='3' "
        "labels=frozenset({'Person'}) properties={'name': 'Carol'}> size=2>"
    )


@pytest.mark.parametrize("legacy_id", (True, False))
def test_path_v2_repr(legacy_id):
    hydration_scope = HydrationHandler().new_hydration_scope()
    gh = hydration_scope._graph_hydrator
    alice = gh.hydrate_node(
        1, {"Person"}, {"name": "Alice"},
        "1" if legacy_id else "alice"

    )
    bob = gh.hydrate_node(
        2, {"Person"}, {"name": "Bob"},
        "2" if legacy_id else "bob"

    )
    carol = gh.hydrate_node(
        3, {"Person"}, {"name": "Carol"},
        "3" if legacy_id else "carol"

    )
    alice_knows_bob = gh.hydrate_relationship(
        1, 1, 2, "KNOWS", {"since": 1999},
        "1" if legacy_id else "alice_knows_bob",
        alice.element_id, bob.element_id
    )
    carol_dislikes_bob = gh.hydrate_relationship(
        2, 3, 2, "DISLIKES", {},
        "2" if legacy_id else "carol_dislikes_bob",
        carol.element_id, bob.element_id
    )
    path = Path(alice, alice_knows_bob, carol_dislikes_bob)
    assert repr(path) == (
        f"<Path start=<Node element_id={alice.element_id!r} "
        "labels=frozenset({'Person'}) properties={'name': 'Alice'}> "
        f"end=<Node element_id={carol.element_id!r} "
        "labels=frozenset({'Person'}) properties={'name': 'Carol'}> size=2>"
    )


def test_graph_views_v1():
    hydration_scope = HydrationHandler().new_hydration_scope()
    gh = hydration_scope._graph_hydrator
    alice = gh.hydrate_node(1, {"Person"}, {"name": "Alice"})
    bob = gh.hydrate_node(2, {"Person"}, {"name": "Bob"})
    carol = gh.hydrate_node(3, {"Person"}, {"name": "Carol"})
    alice_knows_bob = gh.hydrate_relationship(1, 1, 2, "KNOWS",
                                              {"since": 1999})
    carol_dislikes_bob = gh.hydrate_relationship(2, 3, 2, "DISLIKES", {})

    g = hydration_scope.get_graph()
    assert len(g.nodes) == 3
    for id_, node in ((1, alice), (2, bob), (3, carol)):
        with pytest.warns(DeprecationWarning, match=r"element_id \(str\)"):
            assert g.nodes[id_] == node
        assert g.nodes[str(id_)] == node

    assert len(g.relationships) == 2
    for id_, rel in ((1, alice_knows_bob), (2, carol_dislikes_bob)):
        with pytest.warns(DeprecationWarning, match=r"element_id \(str\)"):
            assert g.relationships[id_] == rel
        assert g.relationships[str(id_)] == rel


@pytest.mark.parametrize("legacy_id", (True, False))
def test_graph_views_v2_repr(legacy_id):
    hydration_scope = HydrationHandler().new_hydration_scope()
    gh = hydration_scope._graph_hydrator

    alice_element_id = "1" if legacy_id else "alice"
    bob_element_id = "2" if legacy_id else "bob"
    carol_element_id = "3" if legacy_id else "carol"

    alice = gh.hydrate_node(1, {"Person"}, {"name": "Alice"}, alice_element_id)
    bob = gh.hydrate_node(2, {"Person"}, {"name": "Bob"}, bob_element_id)
    carol = gh.hydrate_node(3, {"Person"}, {"name": "Carol"}, carol_element_id)

    alice_knows_bob_element_id = "1" if legacy_id else "alice_knows_bob"
    carol_dislikes_bob_element_id = "2" if legacy_id else "carol_dislikes_bob"

    alice_knows_bob = gh.hydrate_relationship(
        1, 1, 2, "KNOWS", {"since": 1999}, alice_knows_bob_element_id,
        alice_element_id, bob_element_id
    )
    carol_dislikes_bob = gh.hydrate_relationship(
        2, 3, 2, "DISLIKES", {}, carol_dislikes_bob_element_id,
        carol_element_id, bob_element_id
    )

    g = hydration_scope.get_graph()
    assert len(g.nodes) == 3
    for id_, element_id, node in (
        (1, alice_element_id, alice),
        (2, bob_element_id, bob),
        (3, carol_element_id, carol)
    ):
        with pytest.warns(DeprecationWarning, match=r"element_id \(str\)"):
            assert g.nodes[id_] == node
        assert g.nodes[element_id] == node
        if not legacy_id:
            with pytest.raises(KeyError):
                g.nodes[str(id_)]

    assert len(g.relationships) == 2
    for id_, element_id, rel in (
        (1, alice_knows_bob_element_id, alice_knows_bob),
        (2, carol_dislikes_bob_element_id, carol_dislikes_bob)
    ):
        with pytest.warns(DeprecationWarning, match=r"element_id \(str\)"):
            assert g.relationships[id_] == rel
        assert g.relationships[element_id] == rel
        if not legacy_id:
            with pytest.raises(KeyError):
                g.relationships[str(id_)]
