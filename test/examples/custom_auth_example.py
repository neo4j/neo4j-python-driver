#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2018 "Neo Technology,"
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

# tag::custom-auth-import[]
from neo4j.v1 import GraphDatabase, custom_auth
# end::custom-auth-import[]


class CustomAuthExample:
    # tag::custom-auth[]
    def __init__(self, uri, principal, credentials, realm, scheme, **parameters):
        self._driver = GraphDatabase.driver(uri, auth=custom_auth(principal, credentials, realm, scheme, **parameters))
    # end::custom-auth[]

    def close(self):
        self._driver.close()

    def can_connect(self):
        with self._driver.session() as session:
            result = session.run("RETURN 1")
            return result.single()[0] == 1
