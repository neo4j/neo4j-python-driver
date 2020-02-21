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


from pytest import mark, raises, warns, skip

from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError
from neo4j._exceptions import BoltHandshakeError


def test_bolt_uri(bolt_uri, auth):
    try:
        with GraphDatabase.driver(bolt_uri, auth=auth) as driver:
            with driver.session() as session:
                value = session.run("RETURN 1").single().value()
                assert value == 1
    except BoltHandshakeError as error:
        skip(error.args[0])

# def test_readonly_bolt_uri(readonly_bolt_uri, auth):
#     with GraphDatabase.driver(readonly_bolt_uri, auth=auth) as driver:
#         with driver.session() as session:
#             value = session.run("RETURN 1").single().value()
#             assert value == 1


def test_neo4j_uri(neo4j_uri, auth):
    try:
        with GraphDatabase.driver(neo4j_uri, auth=auth) as driver:
            with driver.session() as session:
                value = session.run("RETURN 1").single().value()
                assert value == 1
    except ServiceUnavailable as error:
        if error.args[0] == "Server does not support routing":
            skip(error.args[0])
    except BoltHandshakeError as error:
        skip(error.args[0])

def test_normal_use_case(bolt_driver):
    session = bolt_driver.session()
    value = session.run("RETURN 1").single().value()
    assert value == 1


def test_invalid_url_scheme(service):
    address = service.addresses[0]
    uri = "x://{}:{}".format(address[0], address[1])
    try:
        with raises(ValueError):
            _ = GraphDatabase.driver(uri, auth=service.auth)
    except BoltHandshakeError as error:
        skip(error.args[0])


def test_fail_nicely_when_using_http_port(service):
    from tests.integration.conftest import NEO4J_PORTS
    address = service.addresses[0]
    uri = "bolt://{}:{}".format(address[0], NEO4J_PORTS["http"])
    try:
        with raises(ServiceUnavailable):
            _ = GraphDatabase.driver(uri, auth=service.auth)
    except BoltHandshakeError as error:
        skip(error.args[0])


def test_custom_resolver(service):
    _, port = service.addresses[0]

    def my_resolver(socket_address):
        assert socket_address == ("*", 7687)
        yield "99.99.99.99", port     # should be rejected as unable to connect
        yield "127.0.0.1", port       # should succeed

    try:
        with GraphDatabase.driver("bolt://*", auth=service.auth,
                                  connect_timeout=3,  # enables rapid timeout
                                  resolver=my_resolver) as driver:
            with driver.session() as session:
                summary = session.run("RETURN 1").summary()
                assert summary.server.address == ("127.0.0.1", port)
    except BoltHandshakeError as error:
        skip(error.args[0])


def test_encrypted_arg_can_still_be_used(bolt_uri, auth):
    with warns(UserWarning):
        try:
            with GraphDatabase.driver(bolt_uri, auth=auth, encrypted=False) as driver:
                assert not driver.secure
        except BoltHandshakeError as error:
            skip(error.args[0])


def test_insecure_by_default(bolt_driver):
    assert not bolt_driver.secure


def test_should_fail_on_incorrect_password(bolt_uri):
    with raises(AuthError):
        try:
            with GraphDatabase.driver(bolt_uri, auth=("neo4j", "wrong-password")) as driver:
                with driver.session() as session:
                    _ = session.run("RETURN 1")
        except BoltHandshakeError as error:
            skip(error.args[0])
