#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2016 "Neo Technology,"
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


import functools
from os import getenv, remove, rename
from os.path import isfile, dirname, join as path_join
from socket import create_connection
from subprocess import call, check_call, CalledProcessError
from threading import Thread
from time import sleep
from unittest import TestCase

from neo4j.util import Watcher
from neo4j.v1.constants import KNOWN_HOSTS

KNOWN_HOSTS_BACKUP = KNOWN_HOSTS + ".backup"


def watch(f):
    """ Decorator to enable log watching for the lifetime of a function.
    Useful for debugging unit tests, simply add `@watch` to the top of
    the test function.

    :param f: the function to decorate
    :return: a decorated function
    """

    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        watcher = Watcher("neo4j.bolt")
        watcher.watch()
        f(*args, **kwargs)
        watcher.stop()

    return wrapper


def restart_server(http_port=7474):
    try:
        check_call("%s/bin/neo4j restart" % getenv("NEO4J_HOME"), shell=True)
    except CalledProcessError as error:
        if error.returncode == 2:
            raise OSError("Another process is listening on the server port")
        elif error.returncode == 512:
            raise OSError("Another server process is already running")
        else:
            raise OSError("An error occurred while trying to start "
                          "the server [%s]" % error.returncode)
    else:
        running = False
        t = 0
        while not running and t < 30:
            try:
                s = create_connection(("localhost", http_port))
            except IOError:
                sleep(1)
                t += 1
            else:
                s.close()
                running = True
        return running


class ServerTestCase(TestCase):
    """ Base class for test cases that use a remote server.
    """

    known_hosts = KNOWN_HOSTS
    known_hosts_backup = known_hosts + ".backup"
    servers = []

    def setUp(self):
        if isfile(self.known_hosts):
            if isfile(self.known_hosts_backup):
                remove(self.known_hosts_backup)
            rename(self.known_hosts, self.known_hosts_backup)

    def tearDown(self):
        if isfile(self.known_hosts_backup):
            if isfile(self.known_hosts):
                remove(self.known_hosts)
            rename(self.known_hosts_backup, self.known_hosts)


class StubServer(Thread):

    def __init__(self, port, script):
        super(StubServer, self).__init__()
        self.port = port
        self.script = path_join(dirname(__file__), "resources", script)

    def run(self):
        check_call(["boltstub", str(self.port), self.script])


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
