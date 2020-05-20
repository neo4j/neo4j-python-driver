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


import subprocess
import os

from threading import Thread
from time import sleep

from boltkit.server.stub import BoltStubService
from pytest import fixture

import logging
from neo4j.debug import watch
watch("neo4j")

log = logging.getLogger("neo4j")


class StubServer():

    def __init__(self, port, script):
        self.port = port
        self.script = os.path.join(os.path.dirname(__file__), "scripts", script)

    def run(self):
        self._process = subprocess.Popen(["python", "-m", "boltkit", "stub", "-v", "-l", ":{}".format(str(self.port)), "-t", "10", self.script], stdout=subprocess.PIPE)
        # Need verbose for this to work
        line =self._process.stdout.readline()

    def wait(self):
        try:
            returncode = self._process.wait(2)
            if returncode != 0:
                print("Stubserver failed with error")
            return returncode == 0
        except subprocess.TimeoutExpired:
            print("Stubserver timeout!")
            return False

    def kill(self):
        # Kill process if not already dead
        if self._process.poll() is None:
            self._process.kill()


class StubCluster(object):

    def __init__(self, servers):
        self.servers = {port: StubServer(port, script) for port, script in dict(servers).items()}

    def __enter__(self):
        self.start()

    def __exit__(self, exc_type, exc_value, traceback):
        self.wait()

    def start(self):
        for port, server in self.servers.items():
            server.run()

    def wait(self):
        success = True
        for port, server in self.servers.items():
            if not server.wait():
                success = False
            server.kill()

        if not success:
            raise Exception("Stub server failed")


class LegacyStubServer(Thread):

    def __init__(self, port, script):
        super(LegacyStubServer, self).__init__()
        self.port = port
        self.script = os.path.join(os.path.dirname(__file__), "scripts", script)

    def run(self):
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


class DefaultBoltStubService(BoltStubService):

    default_base_port = 9001


class StubCluster(StubCluster):

    def __init__(self, *servers):
        scripts = [os.path.join(os.path.dirname(__file__), "scripts", server) for server in servers]

        bss = DefaultBoltStubService.load(*scripts)
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
        "uri_bolt": "bolt://localhost:9001",
        "uri_neo4j": "neo4j://localhost:9001",
        "user": "test",
        "password": "test",
        "auth_token": ("test", "test")
    }
