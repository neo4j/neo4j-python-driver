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


from neo4j.addressing import SocketAddress
from neo4j.bolt.connection import DEFAULT_PORT, ConnectionPool, connect, ConnectionErrorHandler
from neo4j.config import default_config
from neo4j.v1.api import Driver, Session
from neo4j.v1.security import SecurityPlan


class DirectConnectionErrorHandler(ConnectionErrorHandler):
    """ Handler for errors in direct driver connections.
    """

    def __init__(self):
        super(DirectConnectionErrorHandler, self).__init__({}) # does not need to handle errors


class DirectConnectionPool(ConnectionPool):

    def __init__(self, connector, address, **config):
        super(DirectConnectionPool, self).__init__(connector, DirectConnectionErrorHandler(), **config)
        self.address = address

    def acquire(self, access_mode=None):
        return self.acquire_direct(self.address)


class DirectDriver(Driver):
    """ A :class:`.DirectDriver` is created from a ``bolt`` URI and addresses
    a single database instance. This provides basic connectivity to any
    database service topology.
    """

    uri_scheme = "bolt"

    def __new__(cls, uri, **config):
        cls._check_uri(uri)
        if SocketAddress.parse_routing_context(uri):
            raise ValueError("Parameters are not supported with scheme 'bolt'. Given URI: '%s'." % uri)
        instance = object.__new__(cls)
        # We keep the address containing the host name or IP address exactly
        # as-is from the original URI. This means that every new connection
        # will carry out DNS resolution, leading to the possibility that
        # the connection pool may contain multiple IP address keys, one for
        # an old address and one for a new address.
        instance.address = SocketAddress.from_uri(uri, DEFAULT_PORT)
        instance.security_plan = security_plan = SecurityPlan.build(**config)
        instance.encrypted = security_plan.encrypted

        def connector(address, error_handler):
            return connect(address, security_plan.ssl_context, error_handler, **config)

        pool = DirectConnectionPool(connector, instance.address, **config)
        pool.release(pool.acquire())
        instance._pool = pool
        instance._max_retry_time = config.get("max_retry_time", default_config["max_retry_time"])
        return instance

    def session(self, access_mode=None, **parameters):
        if "max_retry_time" not in parameters:
            parameters["max_retry_time"] = self._max_retry_time
        return Session(self._pool.acquire, access_mode, **parameters)
