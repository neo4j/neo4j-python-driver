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


class AbstractConnectionPool:
    """ A collection of connections to one or more server addresses.
    """

    _closed = False

    _default_acquire_timeout = 60  # seconds

    _default_max_size = 100

    def __init__(self, connector, **config):
        self.connector = connector
        self.connections = {}
        self.lock = RLock()
        self.cond = Condition(self.lock)
        self._max_connection_pool_size = config.get("max_connection_pool_size",
                                                    self._default_max_size)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _acquire(self, address, timeout):
        """ Acquire a connection to a given address from the pool.
        The address supplied should always be an IP address, not
        a host name.

        This method is thread safe.
        """
        t0 = perf_counter()
        if timeout is None:
            timeout = self._default_acquire_timeout

        if self.closed():
            raise ServiceUnavailable("Connection pool closed")
        with self.lock:
            try:
                connections = self.connections[address]
            except KeyError:
                connections = self.connections[address] = deque()

            def time_remaining():
                t = timeout - (perf_counter() - t0)
                return t if t > 0 else 0

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
                        connection = self.connector(address, timeout=time_remaining())
                    except ServiceUnavailable:
                        self.remove(address)
                        raise
                    else:
                        connection.pool = self
                        connection.in_use = True
                        connections.append(connection)
                        return connection

                # failed to obtain a connection from pool because the
                # pool is full and no free connection in the pool
                if time_remaining():
                    self.cond.wait(time_remaining())
                    # if timed out, then we throw error. This time
                    # computation is needed, as with python 2.7, we
                    # cannot tell if the condition is notified or
                    # timed out when we come to this line
                    if not time_remaining():
                        raise ClientError("Failed to obtain a connection from pool "
                                          "within {!r}s".format(timeout))
                else:
                    raise ClientError("Failed to obtain a connection from pool "
                                      "within {!r}s".format(timeout))

    def acquire(self, access_mode=None, timeout=None):
        """ Acquire a connection to a server that can satisfy a set of parameters.

        :param access_mode:
        :param timeout:
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