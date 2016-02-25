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
from os import rename
from os.path import isfile
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
        watcher = Watcher("neo4j")
        watcher.watch()
        f(*args, **kwargs)
        watcher.stop()
    return wrapper


class ServerTestCase(TestCase):
    """ Base class for test cases that use a remote server.
    """

    known_hosts = KNOWN_HOSTS
    known_hosts_backup = known_hosts + ".backup"

    def setUp(self):
        if isfile(self.known_hosts):
            rename(self.known_hosts, self.known_hosts_backup)

    def tearDown(self):
        if isfile(self.known_hosts_backup):
            rename(self.known_hosts_backup, self.known_hosts)
