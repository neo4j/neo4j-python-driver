#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2018 "Neo4j,"
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


from test.integration.tools import IntegrationTestCase

from os import makedirs, remove
from os.path import basename, dirname, join as path_join, realpath, isfile, expanduser
import platform
from unittest import TestCase, SkipTest
from shutil import copyfile
from sys import exit, stderr
try:
    from urllib.request import urlretrieve
except ImportError:
    from urllib import urlretrieve

from boltkit.controller import WindowsController, UnixController

from neo4j.v1 import GraphDatabase
from neo4j.exceptions import AuthError
from neo4j.util import ServerVersion

from test.env import NEO4J_SERVER_PACKAGE, NEO4J_USER, NEO4J_PASSWORD


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


class GraphDatabaseServer(object):

    bolt_port = 7687
    bolt_address = ("localhost", bolt_port)

    bolt_uri = "bolt://%s:%d" % bolt_address
    bolt_routing_uri = "bolt+routing://%s:%d" % bolt_address

    user = NEO4J_USER or "test"
    password = NEO4J_PASSWORD or "test"
    auth_token = (user, password)

    controller = None
    dist_path = path_join(dirname(__file__), "dist")
    run_path = path_join(dirname(__file__), "run")

    server_package = NEO4J_SERVER_PACKAGE
    local_server_package = path_join(dist_path, basename(server_package)) if server_package else None

    @classmethod
    def server_version_info(cls):
        with GraphDatabase.driver(cls.bolt_uri, auth=cls.auth_token) as driver:
            with driver.session() as session:
                full_version = session.run("RETURN 1").summary().server.version
                return ServerVersion.from_str(full_version)

    @classmethod
    def at_least_version(cls, major, minor):
        return cls.server_version_info().at_least_version(major, minor)

    @classmethod
    def delete_known_hosts_file(cls):
        known_hosts = path_join(expanduser("~"), ".neo4j", "known_hosts")
        if isfile(known_hosts):
            remove(known_hosts)

    @classmethod
    def _start_server(cls, package):
        try:
            makedirs(cls.run_path)
        except OSError:
            pass
        if platform.system() == "Windows":
            controller_class = WindowsController
        else:
            controller_class = UnixController
        home = realpath(controller_class.extract(package, cls.run_path))
        cls.controller = controller_class(home, 1)
        if NEO4J_USER is None:
            cls.controller.create_user(cls.user, cls.password)
            cls.controller.set_user_role(cls.user, "admin")
        cls.controller.start()

    @classmethod
    def _stop_server(cls):
        if cls.controller is not None:
            cls.controller.stop()
            if NEO4J_USER is None:
                pass  # TODO: delete user

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    @classmethod
    def start(cls):
        if is_listening(cls.bolt_address):
            stderr.write("Using existing server listening on port {}\n".format(cls.bolt_port))
            with GraphDatabase.driver(cls.bolt_uri, auth=cls.auth_token) as driver:
                try:
                    with driver.session():
                        pass
                except AuthError as error:
                    stderr.write("{}\n".format(error))
                    exit(1)
            return
        if cls.server_package is None:
            raise RuntimeError("No Neo4j server available for %s" % cls.__name__)
        stderr.write("Using server from package {}\n".format(cls.server_package))
        package = copy_dist(cls.server_package, cls.local_server_package)
        cls._start_server(package)

    @classmethod
    def stop(cls):
        cls._stop_server()
