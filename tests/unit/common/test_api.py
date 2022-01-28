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


from copy import deepcopy
import itertools
from uuid import uuid4

import pytest

import neo4j.api
from neo4j.data import DataDehydrator
from neo4j.exceptions import ConfigurationError


standard_ascii = [chr(i) for i in range(128)]
not_ascii = "♥O◘♦♥O◘♦"


def dehydrated_value(value):
    return DataDehydrator.fix_parameters({"_": value})["_"]


def test_value_dehydration_should_allow_none():
    assert dehydrated_value(None) is None


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (True, True),
        (False, False),
    ]
)
def test_value_dehydration_should_allow_boolean(test_input, expected):
    assert dehydrated_value(test_input) is expected


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (0, 0),
        (1, 1),
        (0x7F, 0x7F),
        (0x7FFF, 0x7FFF),
        (0x7FFFFFFF, 0x7FFFFFFF),
        (0x7FFFFFFFFFFFFFFF, 0x7FFFFFFFFFFFFFFF),
    ]
)
def test_value_dehydration_should_allow_integer(test_input, expected):
    assert dehydrated_value(test_input) == expected


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (0x10000000000000000, ValueError),
        (-0x10000000000000000, ValueError),
    ]
)
def test_value_dehydration_should_disallow_oversized_integer(test_input, expected):
    with pytest.raises(expected):
        dehydrated_value(test_input)


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (0.0, 0.0),
        (-0.1, -0.1),
        (3.1415926, 3.1415926),
        (-3.1415926, -3.1415926),
    ]
)
def test_value_dehydration_should_allow_float(test_input, expected):
    assert dehydrated_value(test_input) == expected


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (u"", u""),
        (u"hello, world", u"hello, world"),
        ("".join(standard_ascii), "".join(standard_ascii)),
    ]
)
def test_value_dehydration_should_allow_string(test_input, expected):
    assert dehydrated_value(test_input) == expected


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (bytearray(), bytearray()),
        (bytearray([1, 2, 3]), bytearray([1, 2, 3])),
    ]
)
def test_value_dehydration_should_allow_bytes(test_input, expected):
    assert dehydrated_value(test_input) == expected


@pytest.mark.parametrize(
    "test_input, expected",
    [
        ([], []),
        ([1, 2, 3], [1, 2, 3]),
        ([1, 3.1415926, "string", None], [1, 3.1415926, "string", None])
    ]
)
def test_value_dehydration_should_allow_list(test_input, expected):
    assert dehydrated_value(test_input) == expected


@pytest.mark.parametrize(
    "test_input, expected",
    [
        ({}, {}),
        ({u"one": 1, u"two": 1, u"three": 1}, {u"one": 1, u"two": 1, u"three": 1}),
        ({u"list": [1, 2, 3, [4, 5, 6]], u"dict": {u"a": 1, u"b": 2}}, {u"list": [1, 2, 3, [4, 5, 6]], u"dict": {u"a": 1, u"b": 2}}),
        ({"alpha": [1, 3.1415926, "string", None]}, {"alpha": [1, 3.1415926, "string", None]}),
    ]
)
def test_value_dehydration_should_allow_dict(test_input, expected):
    assert dehydrated_value(test_input) == expected


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (object(), TypeError),
        (uuid4(), TypeError),
    ]
)
def test_value_dehydration_should_disallow_object(test_input, expected):
    with pytest.raises(expected):
        dehydrated_value(test_input)


def test_bookmark_is_deprecated():
    with pytest.deprecated_call():
        neo4j.Bookmark()


def test_bookmark_initialization_with_no_values():
    with pytest.deprecated_call():
        bookmark = neo4j.Bookmark()
    assert bookmark.values == frozenset()
    assert bool(bookmark) is False
    assert repr(bookmark) == "<Bookmark values={}>"


@pytest.mark.parametrize(
    "test_input, expected_values, expected_bool, expected_repr",
    [
        ((None,), frozenset(), False, "<Bookmark values={}>"),
        ((None, None), frozenset(), False, "<Bookmark values={}>"),
        (("bookmark1", None), frozenset({"bookmark1"}), True, "<Bookmark values={'bookmark1'}>"),
        (("bookmark1", None, "bookmark2", None), frozenset({"bookmark1", "bookmark2"}), True, "<Bookmark values={'bookmark1', 'bookmark2'}>"),
        ((None, "bookmark1", None, "bookmark2", None, None, "bookmark3"), frozenset({"bookmark1", "bookmark2", "bookmark3"}), True, "<Bookmark values={'bookmark1', 'bookmark2', 'bookmark3'}>"),
    ]
)
def test_bookmark_initialization_with_values_none(test_input, expected_values, expected_bool, expected_repr):
    with pytest.deprecated_call():
        bookmark = neo4j.Bookmark(*test_input)
    assert bookmark.values == expected_values
    assert bool(bookmark) is expected_bool
    assert repr(bookmark) == expected_repr


@pytest.mark.parametrize(
    "test_input, expected_values, expected_bool, expected_repr",
    [
        (("",), frozenset(), False, "<Bookmark values={}>"),
        (("", ""), frozenset(), False, "<Bookmark values={}>"),
        (("bookmark1", ""), frozenset({"bookmark1"}), True, "<Bookmark values={'bookmark1'}>"),
        (("bookmark1", "", "bookmark2", ""), frozenset({"bookmark1", "bookmark2"}), True, "<Bookmark values={'bookmark1', 'bookmark2'}>"),
        (("", "bookmark1", "", "bookmark2", "", "", "bookmark3"), frozenset({"bookmark1", "bookmark2", "bookmark3"}), True, "<Bookmark values={'bookmark1', 'bookmark2', 'bookmark3'}>"),
    ]
)
def test_bookmark_initialization_with_values_empty_string(test_input, expected_values, expected_bool, expected_repr):
    with pytest.deprecated_call():
        bookmark = neo4j.Bookmark(*test_input)
    assert bookmark.values == expected_values
    assert bool(bookmark) is expected_bool
    assert repr(bookmark) == expected_repr


@pytest.mark.parametrize(
    "test_input, expected_values, expected_bool, expected_repr",
    [
        (("bookmark1",), frozenset({"bookmark1"}), True, "<Bookmark values={'bookmark1'}>"),
        (("bookmark1", "bookmark2", "bookmark3"), frozenset({"bookmark1", "bookmark2", "bookmark3"}), True, "<Bookmark values={'bookmark1', 'bookmark2', 'bookmark3'}>"),
        (standard_ascii, frozenset(standard_ascii), True, "<Bookmark values={{'{values}'}}>".format(values="', '".join(standard_ascii)))
    ]
)
def test_bookmark_initialization_with_valid_strings(test_input, expected_values, expected_bool, expected_repr):
    with pytest.deprecated_call():
        bookmark = neo4j.Bookmark(*test_input)
    assert bookmark.values == expected_values
    assert bool(bookmark) is expected_bool
    assert repr(bookmark) == expected_repr


@pytest.mark.parametrize(
    "test_input, expected",
    [
        ((not_ascii,), ValueError),
        (("", not_ascii,), ValueError),
        (("bookmark1", chr(129),), ValueError),
    ]
)
def test_bookmark_initialization_with_invalid_strings(test_input, expected):
    with pytest.raises(expected):
        neo4j.Bookmark(*test_input)


@pytest.mark.parametrize("test_as_generator", [True, False])
@pytest.mark.parametrize("values", (
    ("bookmark1", "bookmark2", "bookmark3"),
    {"bookmark1", "bookmark2", "bookmark3"},
    frozenset(("bookmark1", "bookmark2", "bookmark3")),
    ["bookmark1", "bookmark2", "bookmark3"],
    ("bookmark1", "bookmark2", "bookmark1"),
    ("bookmark1", ""),
    ("bookmark1",),
    (),
    (not_ascii,),
))
def test_bookmarks_raw_values(test_as_generator, values):
    expected = frozenset(values)
    if test_as_generator:
        values = (v for v in values)
    received = neo4j.Bookmarks().from_raw_values(values).raw_values
    assert isinstance(received, frozenset)
    assert received == expected


@pytest.mark.parametrize(("values", "exc_type"), (
    (("bookmark1", None), TypeError),
    ((neo4j.Bookmarks(),), TypeError),
    (neo4j.Bookmarks(), TypeError),
    ((None,), TypeError),
    (None, TypeError),
    ((False,), TypeError),
    (((),), TypeError),
    (([],), TypeError),
    ((dict(),), TypeError),
    ((set(),), TypeError),
    ((frozenset(),), TypeError),
    ((["bookmark1", "bookmark2"],), TypeError),
))
def test_bookmarks_invalid_raw_values(values, exc_type):
    with pytest.raises(exc_type):
        neo4j.Bookmarks().from_raw_values(values)


@pytest.mark.parametrize(("values", "expected_repr"), (
    (("bm1", "bm2"), "<Bookmarks values={'bm1', 'bm2'}>"),
    (("bm2", "bm1"), "<Bookmarks values={'bm1', 'bm2'}>"),
    (("bm42",), "<Bookmarks values={'bm42'}>"),
    ((), "<Bookmarks values={}>"),
))
def test_bookmarks_repr(values, expected_repr):
    bookmarks = neo4j.Bookmarks().from_raw_values(values)
    assert repr(bookmarks) == expected_repr


@pytest.mark.parametrize(("values1", "values2"), (
    (values
     for values in itertools.combinations_with_replacement(
         (
             ("bookmark1",),
             ("bookmark1", "bookmark2"),
             ("bookmark3",),
             (),
         ),
         2
     ))
))
def test_bookmarks_combination(values1, values2):
    bookmarks1 = neo4j.Bookmarks().from_raw_values(values1)
    bookmarks2 = neo4j.Bookmarks().from_raw_values(values2)
    bookmarks3 = bookmarks1 + bookmarks2
    assert bookmarks3.raw_values == (bookmarks2 + bookmarks1).raw_values
    assert bookmarks3.raw_values == frozenset(values1) | frozenset(values2)


@pytest.mark.parametrize(
    "test_input, expected_str, expected_repr",
    [
        ((), "", "Version()"),
        ((None,), "None", "Version(None,)"),
        (("3",), "3", "Version('3',)"),
        (("3", "0"), "3.0", "Version('3', '0')"),
        ((3,), "3", "Version(3,)"),
        ((3, 0), "3.0", "Version(3, 0)"),
        ((3, 0, 0), "3.0.0", "Version(3, 0, 0)"),
        ((3, 0, 0, 0), "3.0.0.0", "Version(3, 0, 0, 0)"),
    ]
)
def test_version_initialization(test_input, expected_str, expected_repr):
    version = neo4j.Version(*test_input)
    assert str(version) == expected_str
    assert repr(version) == expected_repr


@pytest.mark.parametrize(
    "test_input, expected_str, expected_repr",
    [
        (bytearray([0, 0, 0, 0]), "0.0", "Version(0, 0)"),
        (bytearray([0, 0, 0, 1]), "1.0", "Version(1, 0)"),
        (bytearray([0, 0, 1, 0]), "0.1", "Version(0, 1)"),
        (bytearray([0, 0, 1, 1]), "1.1", "Version(1, 1)"),
        (bytearray([0, 0, 254, 254]), "254.254", "Version(254, 254)"),
    ]
)
def test_version_from_bytes_with_valid_bolt_version_handshake(test_input, expected_str, expected_repr):
    version = neo4j.Version.from_bytes(test_input)
    assert str(version) == expected_str
    assert repr(version) == expected_repr


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (bytearray([0, 0, 0]), ValueError),
        (bytearray([0, 0, 0, 0, 0]), ValueError),
        (bytearray([1, 0, 0, 0]), ValueError),
        (bytearray([0, 1, 0, 0]), ValueError),
        (bytearray([1, 1, 0, 0]), ValueError),
    ]
)
def test_version_from_bytes_with_not_valid_bolt_version_handshake(test_input, expected):
    with pytest.raises(expected):
        version = neo4j.Version.from_bytes(test_input)


@pytest.mark.parametrize(
    "test_input, expected",
    [
        ((), bytearray([0, 0, 0, 0])),
        ((0,), bytearray([0, 0, 0, 0])),
        ((1,), bytearray([0, 0, 0, 1])),
        ((0, 0), bytearray([0, 0, 0, 0])),
        ((1, 0), bytearray([0, 0, 0, 1])),
        ((1, 2), bytearray([0, 0, 2, 1])),
        ((255, 255), bytearray([0, 0, 255, 255])),
    ]
)
def test_version_to_bytes_with_valid_bolt_version(test_input, expected):
    version = neo4j.Version(*test_input)
    assert version.to_bytes() == expected


def test_serverinfo_initialization():

    from neo4j.addressing import Address

    address = Address(("bolt://localhost", 7687))
    version = neo4j.Version(3, 0)

    server_info = neo4j.ServerInfo(address, version)
    assert server_info.address is address
    assert server_info.protocol_version is version
    assert server_info.agent is None
    assert server_info.connection_id is None


@pytest.mark.parametrize(
    "test_input, expected_agent",
    [
        ({"server": "Neo4j/3.0.0"}, "Neo4j/3.0.0"),
        ({"server": "Neo4j/3.X.Y"}, "Neo4j/3.X.Y"),
        ({"server": "Neo4j/4.3.1"}, "Neo4j/4.3.1"),
    ]
)
@pytest.mark.parametrize("protocol_version", ((3, 0), (4, 3), (42, 1337)))
def test_serverinfo_with_metadata(test_input, expected_agent,
                                  protocol_version):
    from neo4j.addressing import Address

    address = Address(("bolt://localhost", 7687))
    version = neo4j.Version(*protocol_version)

    server_info = neo4j.ServerInfo(address, version)

    server_info.update(test_input)

    assert server_info.agent == expected_agent
    assert server_info.protocol_version == version


@pytest.mark.parametrize(
    "test_input, expected_driver_type, expected_security_type, expected_error",
    [
        ("bolt://localhost:7676", neo4j.api.DRIVER_BOLT, neo4j.api.SECURITY_TYPE_NOT_SECURE, None),
        ("bolt+ssc://localhost:7676", neo4j.api.DRIVER_BOLT, neo4j.api.SECURITY_TYPE_SELF_SIGNED_CERTIFICATE, None),
        ("bolt+s://localhost:7676", neo4j.api.DRIVER_BOLT, neo4j.api.SECURITY_TYPE_SECURE, None),
        ("neo4j://localhost:7676", neo4j.api.DRIVER_NEO4j, neo4j.api.SECURITY_TYPE_NOT_SECURE, None),
        ("neo4j+ssc://localhost:7676", neo4j.api.DRIVER_NEO4j, neo4j.api.SECURITY_TYPE_SELF_SIGNED_CERTIFICATE, None),
        ("neo4j+s://localhost:7676", neo4j.api.DRIVER_NEO4j, neo4j.api.SECURITY_TYPE_SECURE, None),
        ("undefined://localhost:7676", None, None, ConfigurationError),
        ("localhost:7676", None, None, ConfigurationError),
        ("://localhost:7676", None, None, ConfigurationError),
        ("bolt+routing://localhost:7676", neo4j.api.DRIVER_NEO4j, neo4j.api.SECURITY_TYPE_NOT_SECURE, ConfigurationError),
        ("bolt://username@localhost:7676", None, None, ConfigurationError),
        ("bolt://username:password@localhost:7676", None, None, ConfigurationError),
    ]
)
def test_uri_scheme(test_input, expected_driver_type, expected_security_type, expected_error):
    if expected_error:
        with pytest.raises(expected_error):
            neo4j.api.parse_neo4j_uri(test_input)
    else:
        driver_type, security_type, parsed = neo4j.api.parse_neo4j_uri(test_input)
        assert driver_type == expected_driver_type
        assert security_type == expected_security_type


def test_parse_routing_context():
    context = neo4j.api.parse_routing_context(query="name=molly&color=white")
    assert context == {"name": "molly", "color": "white"}


def test_parse_routing_context_should_error_when_value_missing():
    with pytest.raises(ConfigurationError):
        neo4j.api.parse_routing_context("name=&color=white")


def test_parse_routing_context_should_error_when_key_duplicate():
    with pytest.raises(ConfigurationError):
        neo4j.api.parse_routing_context("name=molly&name=white")
