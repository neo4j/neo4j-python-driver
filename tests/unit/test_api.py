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

# python -m pytest tests/unit/test_api.py -s


def dehydrated_value(value):
    return DataDehydrator.fix_parameters({"_": value})["_"]


def test_value_dehydration_should_allow_none():
    # python -m pytest tests/unit/test_api.py -s -k test_value_dehydration_should_allow_none
    assert dehydrated_value(None) is None


def test_value_dehydration_should_allow_boolean():
    # python -m pytest tests/unit/test_api.py -s -k test_value_dehydration_should_allow_boolean
    assert dehydrated_value(True) is True
    assert dehydrated_value(False) is False


def test_value_dehydration_should_allow_integer():
    # python -m pytest tests/unit/test_api.py -s -k test_value_dehydration_should_allow_integer
    assert dehydrated_value(0) == 0
    assert dehydrated_value(1) == 1
    assert dehydrated_value(-1) == -1
    assert dehydrated_value(0x7F) == 0x7F
    assert dehydrated_value(0x7FFF) == 0x7FFF
    assert dehydrated_value(0x7FFFFFFF) == 0x7FFFFFFF
    assert dehydrated_value(0x7FFFFFFFFFFFFFFF) == 0x7FFFFFFFFFFFFFFF


def test_value_dehydration_should_disallow_oversized_integer():
    # python -m pytest tests/unit/test_api.py -s -k test_value_dehydration_should_disallow_oversized_integer
    with pytest.raises(ValueError):
        dehydrated_value(0x10000000000000000)
    with pytest.raises(ValueError):
        dehydrated_value(-0x10000000000000000)


def test_value_dehydration_should_allow_float():
    # python -m pytest tests/unit/test_api.py -s -k test_value_dehydration_should_allow_float
    assert dehydrated_value(0.0) == 0.0
    assert dehydrated_value(-0.1) == -0.1
    assert dehydrated_value(3.1415926) == 3.1415926
    assert dehydrated_value(-3.1415926) == -3.1415926


def test_value_dehydration_should_allow_string():
    # python -m pytest tests/unit/test_api.py -s -k test_value_dehydration_should_allow_string
    assert dehydrated_value(u"") == u""
    assert dehydrated_value(u"hello, world") == u"hello, world"
    assert dehydrated_value("".join(standard_ascii)) == "".join(standard_ascii)


def test_value_dehydration_should_allow_bytes():
    # python -m pytest tests/unit/test_api.py -s -k test_value_dehydration_should_allow_bytes
    assert dehydrated_value(bytearray()) == bytearray()
    assert dehydrated_value(bytearray([1, 2, 3])) == bytearray([1, 2, 3])


def test_value_dehydration_should_allow_list():
    # python -m pytest tests/unit/test_api.py -s -k test_value_dehydration_should_allow_list
    assert dehydrated_value([]) == []
    assert dehydrated_value([1, 2, 3]) == [1, 2, 3]
    assert dehydrated_value([1, 3.1415926, "string", None]) == [1, 3.1415926, "string", None]


def test_value_dehydration_should_allow_dict():
    # python -m pytest tests/unit/test_api.py -s -k test_value_dehydration_should_allow_dict
    assert dehydrated_value({}) == {}
    assert dehydrated_value({u"one": 1, u"two": 1, u"three": 1}) == {u"one": 1, u"two": 1, u"three": 1}
    assert dehydrated_value({u"list": [1, 2, 3, [4, 5, 6]], u"dict": {u"a": 1, u"b": 2}}) == {u"list": [1, 2, 3, [4, 5, 6]], u"dict": {u"a": 1, u"b": 2}}
    assert dehydrated_value({"alpha": [1, 3.1415926, "string", None]}) == {"alpha": [1, 3.1415926, "string", None]}


def test_value_dehydration_should_disallow_object():
    # python -m pytest tests/unit/test_api.py -s -k test_value_dehydration_should_disallow_object
    with pytest.raises(TypeError):
        dehydrated_value(object())
    with pytest.raises(TypeError):
        dehydrated_value(uuid4())


def test_bookmark_initialization_with_no_values():
    # python -m pytest tests/unit/test_api.py -s -k test_bookmark_initialization_with_no_values
    bookmark = neo4j.api.Bookmark()
    assert bookmark.values == frozenset()
    assert bool(bookmark) is False
    assert repr(bookmark) == "<Bookmark values={}>"


def test_bookmark_initialization_with_values_none():
    # python -m pytest tests/unit/test_api.py -s -k test_bookmark_initialization_with_values_none

    bookmark = neo4j.api.Bookmark(None)
    assert bookmark.values == frozenset()
    assert bool(bookmark) is False
    assert repr(bookmark) == "<Bookmark values={}>"

    bookmark = neo4j.api.Bookmark(None, None)
    assert bookmark.values == frozenset()
    assert bool(bookmark) is False
    assert repr(bookmark) == "<Bookmark values={}>"

    bookmark = neo4j.api.Bookmark("bookmark1", None)
    assert bookmark.values == frozenset({"bookmark1"})
    assert bool(bookmark) is True
    assert repr(bookmark) == "<Bookmark values={'bookmark1'}>"

    bookmark = neo4j.api.Bookmark("bookmark1", None, "bookmark2", None)
    assert bookmark.values == frozenset({"bookmark1", "bookmark2"})
    assert bool(bookmark) is True
    assert repr(bookmark) == "<Bookmark values={'bookmark1', 'bookmark2'}>"

    bookmark = neo4j.api.Bookmark(None, "bookmark1", None, "bookmark2", None, None, "bookmark3")
    assert bookmark.values == frozenset({"bookmark1", "bookmark2", "bookmark3"})
    assert bool(bookmark) is True
    assert repr(bookmark) == "<Bookmark values={'bookmark1', 'bookmark2', 'bookmark3'}>"


def test_bookmark_initialization_with_valid_strings():
    # python -m pytest tests/unit/test_api.py -s -k test_bookmark_initialization_with_valid_strings
    bookmark = neo4j.api.Bookmark("bookmark1")
    assert bookmark.values == frozenset({"bookmark1"})
    assert bool(bookmark) is True
    assert repr(bookmark) == "<Bookmark values={'bookmark1'}>"

    bookmark = neo4j.api.Bookmark("bookmark1", "bookmark2", "bookmark3")
    assert bookmark.values == frozenset({"bookmark1", "bookmark2", "bookmark3"})
    assert bool(bookmark) is True
    assert repr(bookmark) == "<Bookmark values={'bookmark1', 'bookmark2', 'bookmark3'}>"

    bookmark = neo4j.api.Bookmark(*standard_ascii)
    assert bookmark.values == frozenset(standard_ascii)
    assert bool(bookmark) is True
    assert repr(bookmark) == "<Bookmark values={{'{values}'}}>".format(values="', '".join(standard_ascii))


def test_bookmark_initialization_with_invalid_strings():
    # python -m pytest tests/unit/test_api.py -s -k test_bookmark_initialization_with_invalid_strings

    bookmark = neo4j.api.Bookmark("")
    assert bookmark.values == frozenset()
    assert bool(bookmark) is False
    assert repr(bookmark) == "<Bookmark values={}>"

    bookmark = neo4j.api.Bookmark("", "")
    assert bookmark.values == frozenset()
    assert bool(bookmark) is False
    assert repr(bookmark) == "<Bookmark values={}>"

    bookmark = neo4j.api.Bookmark("bookmark1", "")
    assert bookmark.values == frozenset({"bookmark1"})
    assert bool(bookmark) is True
    assert repr(bookmark) == "<Bookmark values={'bookmark1'}>"

    bookmark = neo4j.api.Bookmark("bookmark1", "", "bookmark2", "")
    assert bookmark.values == frozenset({"bookmark1", "bookmark2"})
    assert bool(bookmark) is True
    assert repr(bookmark) == "<Bookmark values={'bookmark1', 'bookmark2'}>"

    bookmark = neo4j.api.Bookmark("", "bookmark1", "", "bookmark2", "", "", "bookmark3")
    assert bookmark.values == frozenset({"bookmark1", "bookmark2", "bookmark3"})
    assert bool(bookmark) is True
    assert repr(bookmark) == "<Bookmark values={'bookmark1', 'bookmark2', 'bookmark3'}>"

    not_ascii = "♥O◘♦♥O◘♦"

    with pytest.raises(ValueError) as e:
        bookmark = neo4j.api.Bookmark(not_ascii)

    with pytest.raises(ValueError) as e:
        bookmark = neo4j.api.Bookmark("", not_ascii)

    with pytest.raises(ValueError) as e:
        bookmark = neo4j.api.Bookmark("" + chr(129))


def test_version_initialization():
    # python -m pytest tests/unit/test_api.py -s -k test_version_initialization

    version = neo4j.api.Version()
    assert str(version) == ""
    assert repr(version) == "Version()"

    version = neo4j.api.Version(None)
    assert str(version) == "None"
    assert repr(version) == "Version(None,)"

    version = neo4j.api.Version("3")
    assert str(version) == "3"
    assert repr(version) == "Version('3',)"

    version = neo4j.api.Version("3", "0")
    assert str(version) == "3.0"
    assert repr(version) == "Version('3', '0')"

    version = neo4j.api.Version("3", "0", "0")
    assert str(version) == "3.0.0"
    assert repr(version) == "Version('3', '0', '0')"

    version = neo4j.api.Version(3)
    assert str(version) == "3"
    assert repr(version) == "Version(3,)"

    version = neo4j.api.Version(3, 0)
    assert str(version) == "3.0"
    assert repr(version) == "Version(3, 0)"

    version = neo4j.api.Version(3, 0, 0)
    assert str(version) == "3.0.0"
    assert repr(version) == "Version(3, 0, 0)"


def test_version_from_bytes_with_valid_bolt_version_handshake():
    # python -m pytest tests/unit/test_api.py -s -k test_version_from_bytes_with_valid_bolt_version_handshake

    byte_version = bytearray([0, 0, 0, 0])

    version = neo4j.api.Version.from_bytes(byte_version)
    assert str(version) == "0.0"
    assert repr(version) == "Version(0, 0)"

    byte_version = bytearray([0, 0, 0, 1])

    version = neo4j.api.Version.from_bytes(byte_version)
    assert str(version) == "1.0"
    assert repr(version) == "Version(1, 0)"

    byte_version = bytearray([0, 0, 1, 0])

    version = neo4j.api.Version.from_bytes(byte_version)
    assert str(version) == "0.1"
    assert repr(version) == "Version(0, 1)"

    byte_version = bytearray([0, 0, 1, 1])

    version = neo4j.api.Version.from_bytes(byte_version)
    assert str(version) == "1.1"
    assert repr(version) == "Version(1, 1)"

    byte_version = bytearray([0, 0, 254, 254])
    version = neo4j.api.Version.from_bytes(byte_version)
    assert str(version) == "254.254"
    assert repr(version) == "Version(254, 254)"


def test_version_from_bytes_with_not_valid_bolt_version_handshake():
    # python -m pytest tests/unit/test_api.py -s -k test_version_from_bytes_with_not_valid_bolt_version_handshake

    byte_version = bytearray([0, 0, 0])
    with pytest.raises(ValueError):
        version = neo4j.api.Version.from_bytes(byte_version)

    byte_version = bytearray([0, 0, 0, 0, 0])
    with pytest.raises(ValueError):
        version = neo4j.api.Version.from_bytes(byte_version)

    byte_version = bytearray([1, 0, 0, 0])
    with pytest.raises(ValueError):
        version = neo4j.api.Version.from_bytes(byte_version)

    byte_version = bytearray([0, 1, 0, 0])
    with pytest.raises(ValueError):
        version = neo4j.api.Version.from_bytes(byte_version)

    byte_version = bytearray([1, 1, 0, 0])
    with pytest.raises(ValueError):
        version = neo4j.api.Version.from_bytes(byte_version)


def test_version_to_bytes_with_valid_bolt_version():
    # python -m pytest tests/unit/test_api.py -s -k test_version_to_bytes_with_valid_bolt_version

    version = neo4j.api.Version()
    assert version.to_bytes() == bytearray([0, 0, 0, 0])

    version = neo4j.api.Version(0)
    assert version.to_bytes() == bytearray([0, 0, 0, 0])

    version = neo4j.api.Version(1)
    assert version.to_bytes() == bytearray([0, 0, 0, 1])

    version = neo4j.api.Version(0, 0)
    assert version.to_bytes() == bytearray([0, 0, 0, 0])

    version = neo4j.api.Version(1, 0)
    assert version.to_bytes() == bytearray([0, 0, 0, 1])

    version = neo4j.api.Version(1, 2)
    assert version.to_bytes() == bytearray([0, 0, 2, 1])

    version = neo4j.api.Version(255, 255)
    assert version.to_bytes() == bytearray([0, 0, 255, 255])


def test_version_to_bytes_with_not_valid_bolt_version():
    # python -m pytest tests/unit/test_api.py -s -k test_version_to_bytes_with_valid_bolt_version

    version = neo4j.api.Version(0, 0, 0)
    with pytest.raises(ValueError):
        byte_version = version.to_bytes()

    version = neo4j.api.Version(-1, -1)
    with pytest.raises(ValueError):
        byte_version = version.to_bytes()

    version = neo4j.api.Version(256, 256)
    with pytest.raises(ValueError):
        byte_version = version.to_bytes()

    version = neo4j.api.Version(None, None)
    with pytest.raises(TypeError):
        byte_version = version.to_bytes()

    version = neo4j.api.Version("0", "0")
    with pytest.raises(TypeError):
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


def test_serverinfo_with_metadata():
    # python -m pytest tests/unit/test_api.py -s -k test_serverinfo_with_metadata

    from neo4j.addressing import Address

    address = Address(("bolt://localhost", 7687))
    version = neo4j.api.Version(3, 0)

    server_info = neo4j.api.ServerInfo(address, version)
    server_info.metadata = {"server": "Neo4j/3.0.0"}

    assert server_info.agent == "Neo4j/3.0.0"
    assert server_info.version_info() == (3, 0, 0)

    server_info.metadata = {"server": "Neo4j/3.X.Y"}
    assert server_info.agent == "Neo4j/3.X.Y"
    assert server_info.version_info() == (3, "X", "Y")


# def test_security_initialization():
#     # python -m pytest tests/unit/test_api.py -s -k test_security_initialization
#     security = neo4j.api.Security()
#     assert security.verify_cert is True
#
#     security = neo4j.api.Security(verify_cert=True)
#     assert security.verify_cert is True
#
#     security = neo4j.api.Security(verify_cert=False)
#     assert security.verify_cert is False
#
#
# def test_security_to_ssl_context():
#     # python -m pytest tests/unit/test_api.py -s -k test_security_to_ssl_context
#
#     # https://docs.python.org/3/library/ssl.html#ssl.SSLContext
#
#     from ssl import (
#         SSLContext,
#         OP_NO_TLSv1,
#         OP_NO_TLSv1_1,
#         CERT_NONE,
#         CERT_REQUIRED,
#         OP_ALL,
#         OP_NO_TLSv1_1,
#         OP_NO_TLSv1,
#         OP_NO_SSLv3,
#         OP_CIPHER_SERVER_PREFERENCE,
#         OP_ENABLE_MIDDLEBOX_COMPAT,
#         OP_NO_COMPRESSION,
#     )
#
#     security = neo4j.api.Security(verify_cert=True)
#     assert security.verify_cert is True
#     context = security.to_ssl_context()
#     assert isinstance(context, SSLContext) is True
#
#     assert context.options == OP_ALL | OP_NO_TLSv1_1 | OP_NO_TLSv1 | OP_NO_SSLv3 | OP_CIPHER_SERVER_PREFERENCE | OP_ENABLE_MIDDLEBOX_COMPAT | OP_NO_COMPRESSION
#     assert context.verify_mode == CERT_REQUIRED
#
#     security = neo4j.api.Security(verify_cert=False)
#     assert security.verify_cert is False
#     context = security.to_ssl_context()
#     assert isinstance(context, SSLContext) is True
#
#     # https://docs.python.org/3/library/ssl.html#ssl.CERT_REQUIRED
#
#     assert context.options == OP_ALL | OP_NO_TLSv1_1 | OP_NO_TLSv1 | OP_NO_SSLv3 | OP_CIPHER_SERVER_PREFERENCE | OP_ENABLE_MIDDLEBOX_COMPAT | OP_NO_COMPRESSION
#     assert context.verify_mode == CERT_NONE