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

# tag::config-connection-pool-import[]
from neo4j.v1 import GraphDatabase
# end::config-connection-pool-import[]


class ConfigConnectionPoolExample:
    # tag::config-connection-pool[]
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password),
                                            max_connection_lifetime=30 * 60, max_connection_pool_size=50,
                                            connection_acquisition_timeout=2 * 60)
    # end::config-connection-pool[]

    def close(self):
        self._driver.close()

    def can_connect(driver):
        result = driver.session().run("RETURN 1")
        return result.single()[0] == 1