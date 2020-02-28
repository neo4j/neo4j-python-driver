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


""" Base classes and helpers.
"""


class Auth:
    """ Container for auth details.
    """

    #: By default we should not send any realm
    realm = None

    def __init__(self, scheme, principal, credentials, realm=None, **parameters):
        self.scheme = scheme
        self.principal = principal
        self.credentials = credentials
        if realm:
            self.realm = realm
        if parameters:
            self.parameters = parameters


# For backwards compatibility
AuthToken = Auth


def basic_auth(user, password, realm=None):
    """ Generate a basic auth token for a given user and password.

    :param user: user name
    :param password: current password
    :param realm: specifies the authentication provider
    :return: auth token for use with :meth:`GraphDatabase.driver`
    """
    return Auth("basic", user, password, realm)


def kerberos_auth(base64_encoded_ticket):
    """ Generate a kerberos auth token with the base64 encoded ticket

    :param base64_encoded_ticket: a base64 encoded service ticket
    :return: an authentication token that can be used to connect to Neo4j
    """
    return Auth("kerberos", "", base64_encoded_ticket)


def custom_auth(principal, credentials, realm, scheme, **parameters):
    """ Generate a basic auth token for a given user and password.

    :param principal: specifies who is being authenticated
    :param credentials: authenticates the principal
    :param realm: specifies the authentication provider
    :param scheme: specifies the type of authentication
    :param parameters: parameters passed along to the authentication provider
    :return: auth token for use with :meth:`GraphDatabase.driver`
    """
    return Auth(scheme, principal, credentials, realm, **parameters)


class Bookmark:

    def __init__(self, *values):
        """
        :param values: ASCII string values
        """
        if values:
            bookmarks = []
            for ix in values:
                try:
                    if ix:
                        ix.encode("ascii")
                        bookmarks.append(ix)
                except UnicodeEncodeError as e:
                    raise ValueError("The value {} is not ASCII".format(ix))
            self.values = frozenset(bookmarks)
        else:
            self.values = frozenset()

    def __repr__(self):
        """
        :return: repr string with sorted values
        """
        return "<Bookmark values={{{}}}>".format(", ".join(["'{}'".format(ix) for ix in sorted(self.values)]))

    def __bool__(self):
        return bool(self.values)


class ServerInfo:

    def __init__(self, address, protocol_version):
        self.address = address
        self.protocol_version = protocol_version
        self.metadata = {}

    @property
    def agent(self):
        return self.metadata.get("server")

    def version_info(self):
        if not self.agent:
            return None
        _, _, value = self.agent.partition("/")
        value = value.replace("-", ".").split(".")
        for i, v in enumerate(value):
            try:
                value[i] = int(v)
            except ValueError:
                pass
        return tuple(value)


class Version(tuple):

    def __new__(cls, *v):
        return super().__new__(cls, v)

    def __repr__(self):
        return "{}{}".format(self.__class__.__name__, super().__repr__())

    def __str__(self):
        return ".".join(map(str, self))

    def to_bytes(self):
        b = bytearray(4)
        for i, v in enumerate(self):
            if not 0 <= i < 2:
                raise ValueError("Too many version components")
            if not 0 <= v < 256:
                raise ValueError("Version component {} is out of range".format(v))
            b[-i - 1] = v
        return bytes(b)

    @classmethod
    def from_bytes(cls, b):
        b = bytearray(b)
        if len(b) != 4:
            raise ValueError("Byte representation must be exactly four bytes")
        if b[0] != 0 or b[1] != 0:
            raise ValueError("First two bytes must contain zero")
        return Version(b[-1], b[-2])


READ_ACCESS = "READ"
WRITE_ACCESS = "WRITE"