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


from neo4j.addressing import SocketAddress, resolve
from neo4j.bolt import DEFAULT_PORT, ConnectionPool, connect
from neo4j.v1.api import Driver
from neo4j.v1.security import SecurityPlan
from neo4j.v1.session import BoltSession


class DirectConnectionPool(ConnectionPool):

    def __init__(self, connector, address):
        super(DirectConnectionPool, self).__init__(connector)
        self.address = address

    def acquire(self, access_mode=None):
        resolved_addresses = resolve(self.address)
        return self.acquire_direct(resolved_addresses[0])


class DirectDriver(Driver):
    """ A :class:`.DirectDriver` is created from a ``bolt`` URI and addresses
    a single database instance. This provides basic connectivity to any
    database service topology.
    """

    def __init__(self, uri, **config):
        # We keep the address containing the host name or IP address exactly
        # as-is from the original URI. This means that every new connection
        # will carry out DNS resolution, leading to the possibility that
        # the connection pool may contain multiple IP address keys, one for
        # an old address and one for a new address.
        self.address = SocketAddress.from_uri(uri, DEFAULT_PORT)
        self.security_plan = security_plan = SecurityPlan.build(**config)
        self.encrypted = security_plan.encrypted
        pool = DirectConnectionPool(lambda a: connect(a, security_plan.ssl_context, **config), self.address)
        pool.acquire()
        Driver.__init__(self, pool, **config)

    def session(self, access_mode=None, bookmark=None):
        return BoltSession(self._pool.acquire, self._max_retry_time, access_mode=access_mode, bookmark=bookmark)
