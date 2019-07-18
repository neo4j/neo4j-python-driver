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


from os import makedirs, remove
from os.path import basename, dirname, join as path_join, realpath, isfile, expanduser
import platform
from threading import RLock
from unittest import TestCase, SkipTest
from shutil import copyfile
from sys import exit, stderr
try:
    from urllib.request import urlretrieve
except ImportError:
    from urllib import urlretrieve

from boltkit.server import Neo4jService

from neo4j import GraphDatabase
from neo4j.exceptions import AuthError

from test.env import NEO4J_SERVER_PACKAGE, NEO4J_USER, NEO4J_PASSWORD, NEOCTRL_ARGS


def copy_dist(source, target):
    if isfile(target) and "SNAPSHOT" not in basename(source):
        return target
    try:
        makedirs(dirname(target))
    except OSError:
        pass
    if source.startswith("http:"):
        stderr.write("Downloading package from {}\n".format(source))
        urlretrieve(source, target)
        return target
    else:
        return copyfile(source, target)


def is_listening(address):
    from socket import create_connection
    try:
        s = create_connection(address)
    except IOError:
        return False
    else:
        s.close()
        return True


class ServerVersion(object):
    def __init__(self, product, version_tuple, tags_tuple):
        self.product = product
        self.version_tuple = version_tuple
        self.tags_tuple = tags_tuple

    def at_least_version(self, major, minor):
        return self.version_tuple >= (major, minor)

    @classmethod
    def from_str(cls, full_version):
        if full_version is None:
            return ServerVersion("Neo4j", (3, 0), ())
        product, _, tagged_version = full_version.partition("/")
        tags = tagged_version.split("-")
        version = map(int, tags[0].split("."))
        return ServerVersion(product, tuple(version), tuple(tags[1:]))


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
    def server_version_info(cls):
        with GraphDatabase.driver(cls.bolt_uri, auth=cls.auth) as driver:
            with driver.session() as session:
                full_version = session.run("RETURN 1").summary().server.agent
                return ServerVersion.from_str(full_version)

    @classmethod
    def at_least_server_version(cls, major, minor):
        return cls.server_version_info().at_least_version(major, minor)

    @classmethod
    def protocol_version(cls):
        with GraphDatabase.driver(cls.bolt_uri, auth=cls.auth) as driver:
            with driver.session() as session:
                return session.run("RETURN 1").summary().protocol_version

    @classmethod
    def edition(cls):
        with GraphDatabase.driver(cls.bolt_uri, auth=cls.auth) as driver:
            with driver.session() as session:
                return (session.run("CALL dbms.components")
                        .single().value("edition"))

    @classmethod
    def at_least_protocol_version(cls, version):
        return cls.protocol_version() >= version

    @classmethod
    def assert_supports_spatial_types(cls):
        if not cls.at_least_protocol_version(2):
            raise SkipTest("Spatial types require Bolt protocol v2 or above")

    @classmethod
    def assert_supports_temporal_types(cls):
        if not cls.at_least_protocol_version(2):
            raise SkipTest("Temporal types require Bolt protocol v2 or above")

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
