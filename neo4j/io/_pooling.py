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


from collections import deque
from threading import RLock, Condition
from time import perf_counter

from neo4j.exceptions import ClientError, ServiceUnavailable


DEFAULT_MAX_CONNECTION_POOL_SIZE = 100
DEFAULT_CONNECTION_ACQUISITION_TIMEOUT = 60  # 1m


class AbstractConnectionPool:
    """ A collection of connections to one or more server addresses.
    """

    _closed = False

    def __init__(self, connector, **config):
        self.connector = connector
        self.connections = {}
        self.lock = RLock()
        self.cond = Condition(self.lock)
        self._max_connection_pool_size = config.get("max_connection_pool_size", DEFAULT_MAX_CONNECTION_POOL_SIZE)
        self._connection_acquisition_timeout = config.get("connection_acquisition_timeout", DEFAULT_CONNECTION_ACQUISITION_TIMEOUT)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def acquire_direct(self, address):
        """ Acquire a connection to a given address from the pool.
        The address supplied should always be an IP address, not
        a host name.

        This method is thread safe.
        """
        if self.closed():
            raise ServiceUnavailable("Connection pool closed")
        with self.lock:
            try:
                connections = self.connections[address]
            except KeyError:
                connections = self.connections[address] = deque()

            connection_acquisition_start_timestamp = perf_counter()
            while True:
                # try to find a free connection in pool
                for connection in list(connections):
                    if connection.closed() or connection.defunct() or connection.timedout():
                        connections.remove(connection)
                        continue
                    if not connection.in_use:
                        connection.in_use = True
                        return connection
                # all connections in pool are in-use
                infinite_connection_pool = (self._max_connection_pool_size < 0 or
                                            self._max_connection_pool_size == float("inf"))
                can_create_new_connection = infinite_connection_pool or len(connections) < self._max_connection_pool_size
                if can_create_new_connection:
                    try:
                        connection = self.connector(address)
                    except ServiceUnavailable:
                        self.remove(address)
                        raise
                    else:
                        connection.pool = self
                        connection.in_use = True
                        connections.append(connection)
                        return connection

                # failed to obtain a connection from pool because the pool is full and no free connection in the pool
                span_timeout = self._connection_acquisition_timeout - (perf_counter() - connection_acquisition_start_timestamp)
                if span_timeout > 0:
                    self.cond.wait(span_timeout)
                    # if timed out, then we throw error. This time computation is needed, as with python 2.7, we cannot
                    # tell if the condition is notified or timed out when we come to this line
                    if self._connection_acquisition_timeout <= (perf_counter() - connection_acquisition_start_timestamp):
                        raise ClientError("Failed to obtain a connection from pool within {!r}s".format(
                            self._connection_acquisition_timeout))
                else:
                    raise ClientError("Failed to obtain a connection from pool within {!r}s".format(self._connection_acquisition_timeout))

    def acquire(self, access_mode=None):
        """ Acquire a connection to a server that can satisfy a set of parameters.

        :param access_mode:
        """

    def release(self, connection):
        """ Release a connection back into the pool.
        This method is thread safe.
        """
        with self.lock:
            connection.in_use = False
            self.cond.notify_all()

    def in_use_connection_count(self, address):
        """ Count the number of connections currently in use to a given
        address.
        """
        try:
            connections = self.connections[address]
        except KeyError:
            return 0
        else:
            return sum(1 if connection.in_use else 0 for connection in connections)

    def deactivate(self, address):
        """ Deactivate an address from the connection pool, if present, closing
        all idle connection to that address
        """
        with self.lock:
            try:
                connections = self.connections[address]
            except KeyError: # already removed from the connection pool
                return
            for conn in list(connections):
                if not conn.in_use:
                    connections.remove(conn)
                    try:
                        conn.close()
                    except IOError:
                        pass
            if not connections:
                self.remove(address)

    def remove(self, address):
        """ Remove an address from the connection pool, if present, closing
        all connections to that address.
        """
        with self.lock:
            for connection in self.connections.pop(address, ()):
                try:
                    connection.close()
                except IOError:
                    pass

    def close(self):
        """ Close all connections and empty the pool.
        This method is thread safe.
        """
        if self._closed:
            return
        try:
            with self.lock:
                if not self._closed:
                    self._closed = True
                    for address in list(self.connections):
                        self.remove(address)
        except TypeError as e:
            pass

    def closed(self):
        """ Return :const:`True` if this pool is closed, :const:`False`
        otherwise.
        """
        with self.lock:
            return self._closed