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

from neo4j.exceptions import (
    Neo4jError,
    ClientError,
    CypherSyntaxError,
    CypherTypeError,
    ConstraintError,
    AuthError,
    Forbidden,
    ForbiddenOnReadOnlyDatabase,
    NotALeader,
    DatabaseError,
    TransientError,
    DatabaseUnavailable,
    DriverError,
    TransactionError,
    TransactionNestingError,
    SessionExpired,
    ServiceUnavailable,
    RoutingServiceUnavailable,
    WriteServiceUnavailable,
    ReadServiceUnavailable,
    ConfigurationError,
    AuthConfigurationError,
    CertificateConfigurationError,
    ResultConsumedError,
    CLASSIFICATION_CLIENT,
    CLASSIFICATION_DATABASE,
    CLASSIFICATION_TRANSIENT,
)

from neo4j._exceptions import (
    BoltError,
    BoltHandshakeError,
    BoltRoutingError,
    BoltConnectionError,
    BoltSecurityError,
    BoltConnectionBroken,
    BoltConnectionClosed,
    BoltFailure,
    BoltIncompleteCommitError,
    BoltProtocolError,
)

from neo4j.io import Bolt


# python -m pytest tests/unit/test_exceptions.py -s -v

def test_bolt_error():
    with pytest.raises(BoltError) as e:
        error = BoltError("Error Message", address="localhost")
        # assert repr(error) == "BoltError('Error Message')" This differs between python version 3.6 "BoltError('Error Message',)"  and 3.7
        assert str(error) == "Error Message"
        assert error.args == ("Error Message",)
        assert error.address == "localhost"
        raise error

    # The regexp parameter of the match method is matched with the re.search function.
    with pytest.raises(AssertionError):
        e.match("FAIL!")

    assert e.match("Error Message")


def test_bolt_protocol_error():
    with pytest.raises(BoltProtocolError) as e:
        error = BoltProtocolError("Driver does not support Bolt protocol version: 0x%06X%02X" % (2, 5), address="localhost")
        assert error.address == "localhost"
        raise error

    # The regexp parameter of the match method is matched with the re.search function.
    with pytest.raises(AssertionError):
        e.match("FAIL!")

    e.match("Driver does not support Bolt protocol version: 0x00000205")


def test_bolt_handshake_error():
    handshake = b"\x00\x00\x00\x04\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00"
    response = b"\x00\x00\x00\x00"
    supported_versions = Bolt.protocol_handlers().keys()

    with pytest.raises(BoltHandshakeError) as e:
        error = BoltHandshakeError("The Neo4J server does not support communication with this driver. Supported Bolt Protocols {}".format(supported_versions), address="localhost", request_data=handshake, response_data=response)
        assert error.address == "localhost"
        assert error.request_data == handshake
        assert error.response_data == response
        raise error

    e.match("The Neo4J server does not support communication with this driver. Supported Bolt Protocols ")


def test_serviceunavailable():
    with pytest.raises(ServiceUnavailable) as e:
        error = ServiceUnavailable("Test error message")
        raise error

    assert e.value.__cause__ is None


def test_serviceunavailable_raised_from_bolt_protocol_error_with_implicit_style():
    error = BoltProtocolError("Driver does not support Bolt protocol version: 0x%06X%02X" % (2, 5), address="localhost")
    with pytest.raises(ServiceUnavailable) as e:
        assert error.address == "localhost"
        try:
            raise error
        except BoltProtocolError as error_bolt_protocol:
            raise ServiceUnavailable(str(error_bolt_protocol)) from error_bolt_protocol

    # The regexp parameter of the match method is matched with the re.search function.
    with pytest.raises(AssertionError):
        e.match("FAIL!")

    e.match("Driver does not support Bolt protocol version: 0x00000205")
    assert e.value.__cause__ is error


def test_serviceunavailable_raised_from_bolt_protocol_error_with_explicit_style():
    error = BoltProtocolError("Driver does not support Bolt protocol version: 0x%06X%02X" % (2, 5), address="localhost")

    with pytest.raises(ServiceUnavailable) as e:
        assert error.address == "localhost"
        try:
            raise error
        except BoltProtocolError as error_bolt_protocol:
            error_nested = ServiceUnavailable(str(error_bolt_protocol))
            error_nested.__cause__ = error_bolt_protocol
            raise error_nested

    # The regexp parameter of the match method is matched with the re.search function.
    with pytest.raises(AssertionError):
        e.match("FAIL!")

    e.match("Driver does not support Bolt protocol version: 0x00000205")
    assert e.value.__cause__ is error


def test_neo4jerror_hydrate_with_no_args():
    error = Neo4jError.hydrate()

    assert isinstance(error, DatabaseError)
    assert error.classification == CLASSIFICATION_DATABASE
    assert error.category == "General"
    assert error.title == "UnknownError"
    assert error.metadata == {}
    assert error.message == "An unknown error occurred"
    assert error.code == "Neo.DatabaseError.General.UnknownError"


def test_neo4jerror_hydrate_with_message_and_code_rubish():
    error = Neo4jError.hydrate(message="Test error message", code="ASDF_asdf")

    assert isinstance(error, DatabaseError)
    assert error.classification == CLASSIFICATION_DATABASE
    assert error.category == "General"
    assert error.title == "UnknownError"
    assert error.metadata == {}
    assert error.message == "Test error message"
    assert error.code == "ASDF_asdf"


def test_neo4jerror_hydrate_with_message_and_code_database():
    error = Neo4jError.hydrate(message="Test error message", code="Neo.DatabaseError.General.UnknownError")

    assert isinstance(error, DatabaseError)
    assert error.classification == CLASSIFICATION_DATABASE
    assert error.category == "General"
    assert error.title == "UnknownError"
    assert error.metadata == {}
    assert error.message == "Test error message"
    assert error.code == "Neo.DatabaseError.General.UnknownError"


def test_neo4jerror_hydrate_with_message_and_code_transient():
    error = Neo4jError.hydrate(message="Test error message", code="Neo.TransientError.General.TestError")
    # error = Neo4jError.hydrate(message="Test error message", code="Neo.{}.General.TestError".format(CLASSIFICATION_TRANSIENT))

    assert isinstance(error, TransientError)
    assert error.classification == CLASSIFICATION_TRANSIENT
    assert error.category == "General"
    assert error.title == "TestError"
    assert error.metadata == {}
    assert error.message == "Test error message"
    assert error.code == "Neo.{}.General.TestError".format(CLASSIFICATION_TRANSIENT)


def test_neo4jerror_hydrate_with_message_and_code_client():
    error = Neo4jError.hydrate(message="Test error message", code="Neo.{}.General.TestError".format(CLASSIFICATION_CLIENT))

    assert isinstance(error, ClientError)
    assert error.classification == CLASSIFICATION_CLIENT
    assert error.category == "General"
    assert error.title == "TestError"
    assert error.metadata == {}
    assert error.message == "Test error message"
    assert error.code == "Neo.{}.General.TestError".format(CLASSIFICATION_CLIENT)
