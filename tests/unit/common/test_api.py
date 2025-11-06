# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
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

import itertools

import pytest

import neo4j._api
from neo4j.addressing import Address
from neo4j.exceptions import ConfigurationError


standard_ascii = [chr(i) for i in range(128)]
not_ascii = "♥O◘♦♥O◘♦"

_bm_input_mark = pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        ((not_ascii,), ValueError),
        (("", not_ascii), ValueError),
        (("bookmark1", chr(129)), ValueError),
    ],
)


@_bm_input_mark
def test_bookmarks_initialization_with_invalid_strings(
    test_input: tuple[str], expected
) -> None:
    with pytest.raises(expected):
        neo4j.Bookmarks.from_raw_values(test_input)


@pytest.mark.parametrize("test_as_generator", [True, False])
@pytest.mark.parametrize(
    "values",
    (
        ("bookmark1", "bookmark2", "bookmark3"),
        {"bookmark1", "bookmark2", "bookmark3"},
        frozenset(("bookmark1", "bookmark2", "bookmark3")),
        ["bookmark1", "bookmark2", "bookmark3"],
        ("bookmark1", "bookmark2", "bookmark1"),
        ("bookmark1", ""),
        ("bookmark1",),
        (),
    ),
)
def test_bookmarks_raw_values(test_as_generator, values) -> None:
    expected = frozenset(values)
    if test_as_generator:
        values = (v for v in values)
    received = neo4j.Bookmarks().from_raw_values(values).raw_values
    assert isinstance(received, frozenset)
    assert received == expected


@pytest.mark.parametrize(
    ("values", "exc_type"),
    (
        (("bookmark1", None), TypeError),
        ((neo4j.Bookmarks(),), TypeError),
        (neo4j.Bookmarks(), TypeError),
        ((None,), TypeError),
        (None, TypeError),
        ((False,), TypeError),
        (((),), TypeError),
        (([],), TypeError),
        (({},), TypeError),
        ((set(),), TypeError),
        ((frozenset(),), TypeError),
        ((["bookmark1", "bookmark2"],), TypeError),
        ((not_ascii,), ValueError),
    ),
)
def test_bookmarks_invalid_raw_values(values, exc_type) -> None:
    with pytest.raises(exc_type):
        neo4j.Bookmarks().from_raw_values(values)


@pytest.mark.parametrize(
    ("values", "expected_repr"),
    (
        (("bm1", "bm2"), "<Bookmarks values={'bm1', 'bm2'}>"),
        (("bm2", "bm1"), "<Bookmarks values={'bm1', 'bm2'}>"),
        (("bm42",), "<Bookmarks values={'bm42'}>"),
        ((), "<Bookmarks values={}>"),
    ),
)
def test_bookmarks_repr(values, expected_repr) -> None:
    bookmarks = neo4j.Bookmarks().from_raw_values(values)
    assert repr(bookmarks) == expected_repr


@pytest.mark.parametrize(
    ("values1", "values2"),
    (
        values
        for values in itertools.combinations_with_replacement(
            (
                ("bookmark1",),
                ("bookmark1", "bookmark2"),
                ("bookmark3",),
                (),
            ),
            2,
        )
    ),
)
def test_bookmarks_combination(values1, values2) -> None:
    bookmarks1 = neo4j.Bookmarks().from_raw_values(values1)
    bookmarks2 = neo4j.Bookmarks().from_raw_values(values2)
    bookmarks3 = bookmarks1 + bookmarks2
    assert bookmarks3.raw_values == (bookmarks2 + bookmarks1).raw_values
    assert bookmarks3.raw_values == frozenset(values1) | frozenset(values2)


def test_serverinfo_initialization() -> None:
    address = Address(("bolt://localhost", 7687))
    version = (3, 0)

    server_info = neo4j.ServerInfo(address, version)
    assert server_info.address is address
    assert server_info.protocol_version is version
    with pytest.raises(AttributeError):
        _ = server_info.connection_id  # type: ignore  # expected to fail


@pytest.mark.parametrize(
    ("test_input", "expected_agent"),
    [
        ({"server": "Neo4j/3.0.0"}, "Neo4j/3.0.0"),
        ({"server": "Neo4j/3.X.Y"}, "Neo4j/3.X.Y"),
        ({"server": "Neo4j/4.3.1"}, "Neo4j/4.3.1"),
    ],
)
@pytest.mark.parametrize("protocol_version", ((3, 0), (4, 3), (42, 1337)))
def test_serverinfo_with_metadata(
    test_input, expected_agent, protocol_version
) -> None:
    address = Address(("bolt://localhost", 7687))

    server_info = neo4j.ServerInfo(address, protocol_version)

    server_info.update(test_input)

    assert server_info.agent == expected_agent
    assert server_info.protocol_version == protocol_version


@pytest.mark.parametrize(
    (
        "test_input",
        "expected_driver_type",
        "expected_security_type",
        "expected_error",
    ),
    [
        (
            "bolt://localhost:7676",
            neo4j._api.DRIVER_BOLT,
            neo4j._api.SECURITY_TYPE_NOT_SECURE,
            None,
        ),
        (
            "bolt+ssc://localhost:7676",
            neo4j._api.DRIVER_BOLT,
            neo4j._api.SECURITY_TYPE_SELF_SIGNED_CERTIFICATE,
            None,
        ),
        (
            "bolt+s://localhost:7676",
            neo4j._api.DRIVER_BOLT,
            neo4j._api.SECURITY_TYPE_SECURE,
            None,
        ),
        (
            "neo4j://localhost:7676",
            neo4j._api.DRIVER_NEO4J,
            neo4j._api.SECURITY_TYPE_NOT_SECURE,
            None,
        ),
        (
            "neo4j+ssc://localhost:7676",
            neo4j._api.DRIVER_NEO4J,
            neo4j._api.SECURITY_TYPE_SELF_SIGNED_CERTIFICATE,
            None,
        ),
        (
            "neo4j+s://localhost:7676",
            neo4j._api.DRIVER_NEO4J,
            neo4j._api.SECURITY_TYPE_SECURE,
            None,
        ),
        ("undefined://localhost:7676", None, None, ConfigurationError),
        ("localhost:7676", None, None, ConfigurationError),
        ("://localhost:7676", None, None, ConfigurationError),
        (
            "bolt+routing://localhost:7676",
            neo4j._api.DRIVER_NEO4J,
            neo4j._api.SECURITY_TYPE_NOT_SECURE,
            ConfigurationError,
        ),
        ("bolt://username@localhost:7676", None, None, ConfigurationError),
        (
            "bolt://username:password@localhost:7676",
            None,
            None,
            ConfigurationError,
        ),
    ],
)
def test_uri_scheme(
    test_input, expected_driver_type, expected_security_type, expected_error
) -> None:
    if expected_error:
        with pytest.raises(expected_error):
            neo4j._api.parse_neo4j_uri(test_input)
    else:
        driver_type, security_type, _parsed = neo4j._api.parse_neo4j_uri(
            test_input
        )
        assert driver_type == expected_driver_type
        assert security_type == expected_security_type


def test_parse_routing_context() -> None:
    context = neo4j._api.parse_routing_context(query="name=molly&color=white")
    assert context == {"name": "molly", "color": "white"}


def test_parse_routing_context_should_error_when_value_missing() -> None:
    with pytest.raises(ConfigurationError):
        neo4j._api.parse_routing_context("name=&color=white")


def test_parse_routing_context_should_error_when_key_duplicate() -> None:
    with pytest.raises(ConfigurationError):
        neo4j._api.parse_routing_context("name=molly&name=white")
