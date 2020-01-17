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


import pytest
from uuid import uuid4

import neo4j.api
from neo4j.work.simple import DataDehydrator

standard_ascii = [chr(i) for i in range(128)]
not_ascii = "♥O◘♦♥O◘♦"

# python -m pytest tests/unit/test_api.py -s


def dehydrated_value(value):
    return DataDehydrator.fix_parameters({"_": value})["_"]


def test_value_dehydration_should_allow_none():
    # python -m pytest tests/unit/test_api.py -s -k test_value_dehydration_should_allow_none
    assert dehydrated_value(None) is None


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (True, True),
        (False, False),
    ]
)
def test_value_dehydration_should_allow_boolean(test_input, expected):
    # python -m pytest tests/unit/test_api.py -s -k test_value_dehydration_should_allow_boolean
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
    # python -m pytest tests/unit/test_api.py -s -k test_value_dehydration_should_allow_integer
    assert dehydrated_value(test_input) == expected


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (0x10000000000000000, ValueError),
        (-0x10000000000000000, ValueError),
    ]
)
def test_value_dehydration_should_disallow_oversized_integer(test_input, expected):
    # python -m pytest tests/unit/test_api.py -s -k test_value_dehydration_should_disallow_oversized_integer
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
    # python -m pytest tests/unit/test_api.py -s -k test_value_dehydration_should_allow_float
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
    # python -m pytest tests/unit/test_api.py -s -k test_value_dehydration_should_allow_string
    assert dehydrated_value(test_input) == expected


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (bytearray(), bytearray()),
        (bytearray([1, 2, 3]), bytearray([1, 2, 3])),
    ]
)
def test_value_dehydration_should_allow_bytes(test_input, expected):
    # python -m pytest tests/unit/test_api.py -s -k test_value_dehydration_should_allow_bytes
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
    # python -m pytest tests/unit/test_api.py -s -k test_value_dehydration_should_allow_list
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
    # python -m pytest tests/unit/test_api.py -s -k test_value_dehydration_should_allow_dict
    assert dehydrated_value(test_input) == expected


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (object(), TypeError),
        (uuid4(), TypeError),
    ]
)
def test_value_dehydration_should_disallow_object(test_input, expected):
    # python -m pytest tests/unit/test_api.py -s -k test_value_dehydration_should_disallow_object
    with pytest.raises(expected):
        dehydrated_value(test_input)


def test_bookmark_initialization_with_no_values():
    # python -m pytest tests/unit/test_api.py -s -k test_bookmark_initialization_with_no_values
    bookmark = neo4j.api.Bookmark()
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
    # python -m pytest tests/unit/test_api.py -s -k test_bookmark_initialization_with_values_none
    bookmark = neo4j.api.Bookmark(*test_input)
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
    # python -m pytest tests/unit/test_api.py -s -k test_bookmark_initialization_with_values_empty_string
    bookmark = neo4j.api.Bookmark(*test_input)
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
    # python -m pytest tests/unit/test_api.py -s -k test_bookmark_initialization_with_valid_strings
    bookmark = neo4j.api.Bookmark(*test_input)
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
    # python -m pytest tests/unit/test_api.py -s -k test_bookmark_initialization_with_invalid_strings
    with pytest.raises(expected) as e:
        bookmark = neo4j.api.Bookmark(*test_input)


@pytest.mark.parametrize(
    "test_input, expected_str, expected_repr",
    [
        ((), "", "Version()"),
        ((None,), "None", "Version(None,)"),
        (("3",), "3", "Version('3',)"),
        (("3", "0"), "3.0", "Version('3', '0')"),
        ((3,), "3", "Version(3,)"),
        ((3,0), "3.0", "Version(3, 0)"),
        ((3, 0, 0), "3.0.0", "Version(3, 0, 0)"),
        ((3, 0, 0, 0), "3.0.0.0", "Version(3, 0, 0, 0)"),
    ]
)
def test_version_initialization(test_input, expected_str, expected_repr):
    # python -m pytest tests/unit/test_api.py -s -k test_version_initialization
    version = neo4j.api.Version(*test_input)
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
    # python -m pytest tests/unit/test_api.py -s -k test_version_from_bytes_with_valid_bolt_version_handshake
    version = neo4j.api.Version.from_bytes(test_input)
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
    # python -m pytest tests/unit/test_api.py -s -k test_version_from_bytes_with_not_valid_bolt_version_handshake
    with pytest.raises(expected):
        version = neo4j.api.Version.from_bytes(test_input)


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
    # python -m pytest tests/unit/test_api.py -s -k test_version_to_bytes_with_valid_bolt_version
    version = neo4j.api.Version(*test_input)
    assert version.to_bytes() == expected


@pytest.mark.parametrize(
    "test_input, expected",
    [
        ((0, 0, 0), ValueError),
        ((-1, -1), ValueError),
        ((256, 256), ValueError),
        ((None, None), TypeError),
        (("0", "0"), TypeError),
    ]
)
def test_version_to_bytes_with_not_valid_bolt_version(test_input, expected):
    # python -m pytest tests/unit/test_api.py -s -k test_version_to_bytes_with_valid_bolt_version
    version = neo4j.api.Version(*test_input)
    with pytest.raises(expected):
        byte_version = version.to_bytes()


def test_serverinfo_initialization():
    # python -m pytest tests/unit/test_api.py -s -k test_serverinfo_initialization

    from neo4j.addressing import Address

    address = Address(("bolt://localhost", 7687))
    version = neo4j.api.Version(3, 0)

    server_info = neo4j.api.ServerInfo(address, version)
    assert server_info.address is address
    assert server_info.protocol_version is version
    assert server_info.metadata == {}

    assert server_info.agent is None
    assert server_info.version_info() is None


@pytest.mark.parametrize(
    "test_input, expected_agent, expected_version_info",
    [
        ({"server": "Neo4j/3.0.0"}, "Neo4j/3.0.0", (3, 0, 0)),
        ({"server": "Neo4j/3.X.Y"}, "Neo4j/3.X.Y", (3, "X", "Y")),
    ]
)
def test_serverinfo_with_metadata(test_input, expected_agent, expected_version_info):
    # python -m pytest tests/unit/test_api.py -s -k test_serverinfo_with_metadata
    from neo4j.addressing import Address

    address = Address(("bolt://localhost", 7687))
    version = neo4j.api.Version(3, 0)

    server_info = neo4j.api.ServerInfo(address, version)

    server_info.metadata = test_input

    assert server_info.agent == expected_agent
    assert server_info.version_info() == expected_version_info
