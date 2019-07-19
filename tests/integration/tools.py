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


from threading import RLock
from unittest import TestCase

from boltkit.server import Neo4jService

from neo4j import GraphDatabase
from tests.env import NEO4J_USER, NEO4J_PASSWORD


class IntegrationTestCase(TestCase):
    """ Base class for test cases that integrate with a server.
    """

    bolt_port = 7687
    bolt_address = ("localhost", bolt_port)

    bolt_uri = "bolt://%s:%d" % bolt_address
    bolt_routing_uri = "bolt+routing://%s:%d" % bolt_address

    user = NEO4J_USER or "neo4j"
    password = NEO4J_PASSWORD or "password"
    auth = (user, password)

    service = None
    service_lock = RLock()

    @classmethod
    def edition(cls):
        with GraphDatabase.driver(cls.bolt_uri, auth=cls.auth) as driver:
            with driver.session() as session:
                return (session.run("CALL dbms.components")
                        .single().value("edition"))

    @classmethod
    def start_service(cls):
        with cls.service_lock:
            assert cls.service is None
            cls.service = Neo4jService(auth=cls.auth)
            cls.service.start(timeout=300)
            address = cls.service.addresses[0]
            cls.bolt_uri = "bolt://{}:{}".format(address[0], address[1])

    @classmethod
    def stop_service(cls):
        with cls.service_lock:
            if cls.service:
                cls.service.stop(timeout=300)
                cls.service = None

    @classmethod
    def setUpClass(cls):
        cls.start_service()

    @classmethod
    def tearDownClass(cls):
        cls.stop_service()


class DirectIntegrationTestCase(IntegrationTestCase):

    driver = None

    def setUp(self):
        from neo4j import GraphDatabase
        self.driver = GraphDatabase.driver(self.bolt_uri, auth=self.auth)

    def tearDown(self):
        self.driver.close()


class RoutingIntegrationTestCase(IntegrationTestCase):

    driver = None

    def setUp(self):
        from neo4j import GraphDatabase
        self.driver = GraphDatabase.driver(self.bolt_routing_uri, auth=self.auth)

    def tearDown(self):
        self.driver.close()
