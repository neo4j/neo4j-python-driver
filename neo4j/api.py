#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2019 "Neo4j,"
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


class Bookmark:

    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return "<Bookmark value=%r>" % self.value

    def __bool__(self):
        return bool(self.value)


class Security:
    """ Container for security details.
    """

    verify_cert = True

    @classmethod
    def default(cls):
        return cls()

    def __init__(self, verify_cert=True):
        self.verify_cert = verify_cert

    def to_ssl_context(self):
        # See https://docs.python.org/3.7/library/ssl.html#protocol-versions
        from ssl import SSLContext, PROTOCOL_TLS_CLIENT, OP_NO_TLSv1, OP_NO_TLSv1_1, CERT_REQUIRED
        ssl_context = SSLContext(PROTOCOL_TLS_CLIENT)
        ssl_context.options |= OP_NO_TLSv1
        ssl_context.options |= OP_NO_TLSv1_1
        if self.verify_cert:
            ssl_context.verify_mode = CERT_REQUIRED
        ssl_context.set_default_verify_paths()
        return ssl_context


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
