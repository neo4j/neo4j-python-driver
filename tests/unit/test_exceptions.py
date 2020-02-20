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
    SessionExpired,
    ServiceUnavailable,
    RoutingServiceUnavailable,
    WriteServiceUnavailable,
    ReadServiceUnavailable,
    # ConfigurationError,
    # AuthConfigurationError,
    # CertificateConfigurationError,
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
