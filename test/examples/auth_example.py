#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2017 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
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

# tag::basic-auth-import[]
from neo4j.v1 import GraphDatabase
# end::basic-auth-import[]

# tag::kerberos-auth-import[]
from neo4j.v1 import kerberos_auth
# end::kerberos-auth-import[]


class BasicAuthExample:
    # tag::basic-auth[]
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
    # end::basic-auth[]

    def close(self):
        self._driver.close()

    def can_connect(self):
        result = self._driver.session().run("RETURN 1")
        return result.single()[0] == 1


class KerberosAuthExample:
    # tag::kerberos-auth[]
    def __init__(self, uri, ticket):
        self._driver = GraphDatabase.driver(uri, auth=kerberos_auth(ticket))
    # end::kerberos-auth[]

    def close(self):
        self._driver.close()


class CustomAuthExample:
    # tag::custom-auth[]
    def __init__(self, uri, principal, credentials, realm, scheme, parameters):
        self._driver = GraphDatabase.driver(uri, auth=(principal, credentials, realm, scheme, parameters))
    # end::custom-auth[]

    def close(self):
        self._driver.close()
