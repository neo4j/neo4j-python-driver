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


from os.path import dirname, join as path_join
from subprocess import check_call
from threading import Thread
from time import sleep
from unittest import TestCase


class StubTestCase(TestCase):
    """ Base class for test cases that integrate with a server.
    """

    bolt_uri = "bolt://localhost:7687"
    bolt_routing_uri = "bolt+routing://localhost:7687"

    user = "test"
    password = "test"
    auth_token = (user, password)


class StubServer(Thread):

    def __init__(self, port, script):
        super(StubServer, self).__init__()
        self.port = port
        self.script = path_join(dirname(__file__), "scripts", script)

    def run(self):
        check_call(["bolt", "stub", "-t", "3", "-l", ":{}".format(self.port), self.script])


class StubCluster(object):

    def __init__(self, servers):
        self.servers = {port: StubServer(port, script) for port, script in dict(servers).items()}

    def __enter__(self):
        self.start()

    def __exit__(self, exc_type, exc_value, traceback):
        self.wait()

    def start(self):
        for port, server in self.servers.items():
            server.start()
        sleep(0.5)

    def wait(self):
        for port, server in self.servers.items():
            server.join()
