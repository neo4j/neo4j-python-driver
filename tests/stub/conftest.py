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


from os.path import dirname, join as path_join
from subprocess import check_call
from threading import Thread
from time import sleep

from boltkit.server.stub import BoltStubService
from pytest import fixture


class StubServer(Thread):

    def __init__(self, port, script):
        super(StubServer, self).__init__()
        self.port = port
        self.script = path_join(dirname(__file__), "scripts", script)

    def run(self):
        check_call(["boltstub", str(self.port), self.script])


class LegacyStubServer(Thread):

    def __init__(self, port, script):
        super(LegacyStubServer, self).__init__()
        self.port = port
        self.script = path_join(dirname(__file__), "scripts", script)

    def run(self):
        # check_call(["boltstub", str(self.port), self.script])
        check_call(["python", "-m", "boltkit.legacy.stub", "-v", str(self.port), self.script])


class LegacyStubCluster(object):

    def __init__(self, servers):
        self.servers = {port: LegacyStubServer(port, script) for port, script in dict(servers).items()}

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


class LegacyBoltStubService(BoltStubService):

    default_base_port = 9001


class StubCluster(LegacyStubCluster):

    def __init__(self, *servers):
        scripts = [path_join(dirname(__file__), "scripts", server)
                   for server in servers]

        bss = LegacyBoltStubService.load(*scripts)
        servers2 = {port: script.filename for port, script in bss.scripts.items()}
        super().__init__(servers2)

        # def run():
        #     check_call(["bolt", "stub", "-v", "-t", "10", "-l", ":9001"] + scripts)

        # self.thread = Thread(target=run)

    # def __enter__(self):
    #     self.thread.start()
    #     sleep(0.5)

    # def __exit__(self, exc_type, exc_value, traceback):
    #     self.thread.join(3)


@fixture
def script():
    return lambda *paths: path_join(dirname(__file__), "scripts", *paths)


@fixture
def driver_info():
    """ Base class for test cases that integrate with a server.
    """
    return {
        "uri": "bolt://localhost:7687",
        "bolt_routing_uri": "bolt+routing://localhost:7687",
        "user": "test",
        "password": "test",
        "auth_token": ("test", "test")
    }
