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


__all__ = [
    "__version__",
    "READ_ACCESS",
    "WRITE_ACCESS",
    "GraphDatabase",
    "Driver",
    "BoltDriver",
    "Neo4jDriver",
    "DriverError",
    "Auth",
    "AuthToken",
]

from logging import getLogger
from urllib.parse import urlparse, parse_qs

from neo4j.addressing import Address
from neo4j.api import *
from neo4j.meta import get_user_agent
from neo4j.exceptions import ConnectionExpired, ServiceUnavailable
from neo4j.meta import experimental, version as __version__


READ_ACCESS = "READ"
WRITE_ACCESS = "WRITE"


log = getLogger("neo4j")


class GraphDatabase:
    """ Accessor for :class:`.Driver` construction.
    """

    @classmethod
    def driver(cls, uri, *, auth=None, acquire_timeout=None, **config):
        """ Create a Neo4j driver that uses socket I/O and thread-based
        concurrency.

        :param uri:
        :param auth:
        :param acquire_timeout:
        :param config: connection configuration settings
        """
        parsed = urlparse(uri)
        if parsed.scheme == "bolt":
            return cls.bolt_driver(parsed.netloc, auth=auth, acquire_timeout=acquire_timeout,
                                   **config)
        elif parsed.scheme == "neo4j" or parsed.scheme == "bolt+routing":
            rc = cls._parse_routing_context(parsed.query)
            return cls.neo4j_driver(parsed.netloc, auth=auth, routing_context=rc,
                                    acquire_timeout=acquire_timeout, **config)
        else:
            raise ValueError("Unknown URI scheme {!r}".format(parsed.scheme))

    @classmethod
    async def async_driver(cls, uri, *, auth=None, loop=None, **config):
        """ Create a Neo4j driver that uses async I/O and task-based
        concurrency.
        """
        parsed = urlparse(uri)
        if parsed.scheme == "bolt":
            return await cls.async_bolt_driver(parsed.netloc, auth=auth, loop=loop, **config)
        elif parsed.scheme == "neo4j" or parsed.scheme == "bolt+routing":
            rc = cls._parse_routing_context(parsed.query)
            return await cls.async_neo4j_driver(parsed.netloc, auth=auth, routing_context=rc,
                                                loop=loop, **config)
        else:
            raise ValueError("Unknown URI scheme {!r}".format(parsed.scheme))

    @classmethod
    def bolt_driver(cls, target, *, auth=None, acquire_timeout=None, **config):
        """ Create a driver for direct Bolt server access that uses
        socket I/O and thread-based concurrency.
        """
        return BoltDriver.open(target, auth=auth, acquire_timeout=acquire_timeout, **config)

    @classmethod
    async def async_bolt_driver(cls, target, *, auth=None, loop=None, **config):
        """ Create a driver for direct Bolt server access that uses
        async I/O and task-based concurrency.
        """
        return await AsyncBoltDriver.open(target, auth=auth, loop=loop, **config)

    @classmethod
    def neo4j_driver(cls, *targets, auth=None, routing_context=None, acquire_timeout=None,
                     **config):
        """ Create a driver for routing-capable Neo4j service access
        that uses socket I/O and thread-based concurrency.
        """
        return Neo4jDriver.open(*targets, auth=auth, routing_context=routing_context,
                                acquire_timeout=acquire_timeout, **config)

    @classmethod
    async def async_neo4j_driver(cls, *targets, auth=None, loop=None, **config):
        """ Create a driver for routing-capable Neo4j service access
        that uses async I/O and task-based concurrency.
        """
        return await AsyncNeo4jDriver.open(*targets, auth=auth, loop=loop, **config)

    @classmethod
    def _parse_routing_context(cls, query):
        """ Parse the query portion of a URI to generate a routing
        context dictionary.
        """
        if not query:
            return {}

        context = {}
        parameters = parse_qs(query, True)
        for key in parameters:
            value_list = parameters[key]
            if len(value_list) != 1:
                raise ValueError("Duplicated query parameters with key '%s', "
                                 "value '%s' found in query string '%s'" % (key, value_list, query))
            value = value_list[0]
            if not value:
                raise ValueError("Invalid parameters:'%s=%s' in query string "
                                 "'%s'." % (key, value, query))
            context[key] = value
        return context


class Driver:
    """ Base class for all types of :class:`.Driver`, instances of which are
    used as the primary access point to Neo4j.

    :param uri: URI for a graph database service
    :param config: configuration and authentication details
    """

    #: Overridden by subclasses to specify the URI scheme owned by that
    #: class.
    uri_schemes = ()

    #: Connection pool
    _pool = None

    #: Indicator of driver closure.
    _closed = False

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _assert_open(self):
        if self.closed():
            raise DriverError("Driver closed")

    def session(self, **parameters):
        """ Create a new :class:`.Session` object based on this
        :class:`.Driver`.

        :param parameters: custom session parameters (see
                           :class:`.Session` for details)
        :returns: new :class:`.Session` object
        """
        raise NotImplementedError("Blocking sessions are not implemented "
                                  "for the %s class" % type(self).__name__)

    def rx_session(self, **parameters):
        raise NotImplementedError("Reactive sessions are not implemented "
                                  "for the %s class" % type(self).__name__)

    @experimental("The pipeline API is experimental and may be removed or "
                  "changed in a future release")
    def pipeline(self, **parameters):
        """ Create a new :class:`.Pipeline` objects based on this
        :class:`.Driver`.
        """
        raise NotImplementedError("Pipelines are not implemented "
                                  "for the %s class" % type(self).__name__)

    def close(self):
        """ Shut down, closing any open connections in the pool.
        """
        if not self._closed:
            self._closed = True
            if self._pool is not None:
                self._pool.close()
                self._pool = None

    def closed(self):
        """ Return :const:`True` if closed, :const:`False` otherwise.
        """
        return self._closed


class AsyncDriver:

    @classmethod
    async def open(cls, uri, **config):
        raise NotImplementedError

    def session(self, **parameters):
        raise NotImplementedError

    def rx_session(self, **parameters):
        raise NotImplementedError

    @experimental("The pipeline API is experimental and may be removed or "
                  "changed in a future release")
    def pipeline(self, **parameters):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def closed(self):
        raise NotImplementedError


class Direct:

    default_host = "localhost"
    default_port = 7687

    default_target = ":"

    @classmethod
    def parse_target(cls, target):
        """ Parse a target string to produce an address.
        """
        if not target:
            target = cls.default_target
        address = Address.parse(target, default_host=cls.default_host,
                                default_port=cls.default_port)
        return address


class Routing:

    default_host = "localhost"
    default_port = 7687

    default_targets = ": :17601 :17687"

    @classmethod
    def parse_targets(cls, *targets):
        """ Parse a sequence of target strings to produce an address
        list.
        """
        targets = " ".join(targets)
        if not targets:
            targets = cls.default_targets
        addresses = Address.parse_list(targets, default_host=cls.default_host,
                                       default_port=cls.default_port)
        return addresses


class BoltDriver(Direct, Driver):
    """ A :class:`.BoltDriver` is created from a ``bolt`` URI and addresses
    a single database machine. This may be a standalone server or could be a
    specific member of a cluster.

    Connections established by a :class:`.BoltDriver` are always made to the
    exact host and port detailed in the URI.
    """

    @classmethod
    def open(cls, target, *, auth=None, acquire_timeout=None, **config):
        address = cls.parse_target(target)
        return cls(address, auth=auth, acquire_timeout=acquire_timeout, **config)

    def __init__(self, address, *, auth=None, acquire_timeout=None, **config):
        from neo4j.io import Bolt, BoltPool
        self.address = address
        self.config = Config(**config)
        self.acquire_timeout = acquire_timeout

        def connector(addr, timeout):
            return Bolt.open(addr, auth=auth, timeout=timeout, **self.config)

        pool = BoltPool(connector, address, acquire_timeout=acquire_timeout, **self.config)
        pool.release(pool.acquire())
        self._pool = pool

    @property
    def secure(self):
        return bool(self.config.secure)

    def session(self, **parameters):
        self._assert_open()
        if "acquire_timeout" not in parameters:
            parameters["acquire_timeout"] = self.acquire_timeout
        if "max_retry_time" not in parameters:
            parameters["max_retry_time"] = self.config.max_retry_time
        from neo4j.work.blocking import Session
        return Session(self._pool.acquire, **parameters)

    def rx_session(self, **parameters):
        raise NotImplementedError("Reactive sessions are not implemented "
                                  "for the %s class" % type(self).__name__)

    def pipeline(self, **parameters):
        from neo4j.work.pipelining import Pipeline
        return Pipeline(self._pool.acquire, **parameters)


class Neo4jDriver(Routing, Driver):
    """ A :class:`.Neo4jDriver` is created from a ``neo4j`` URI. The
    routing behaviour works in tandem with Neo4j's `Causal Clustering
    <https://neo4j.com/docs/operations-manual/current/clustering/>`_ feature
    by directing read and write behaviour to appropriate cluster members.
    """

    @classmethod
    def open(cls, *targets, auth=None, routing_context=None, acquire_timeout=None, **config):
        addresses = cls.parse_targets(*targets)
        return cls(*addresses, auth=auth, routing_context=routing_context,
                   acquire_timeout=acquire_timeout, **config)

    def __init__(self, *addresses, auth=None, routing_context=None, acquire_timeout=None, **config):
        from neo4j.io import Bolt, Neo4jPool
        self.addresses = addresses
        self.config = Config(**config)
        self._max_retry_time = self.config.max_retry_time
        self.acquire_timeout = acquire_timeout

        def connector(addr, timeout):
            return Bolt.open(addr, auth=auth, timeout=timeout, **self.config)

        # TODO: pass in all addresses
        pool = Neo4jPool(connector, addresses[0], routing_context, *addresses,
                         acquire_timeout=acquire_timeout, **config)
        try:
            pool.update_routing_table()
        except Exception:
            pool.close()
            raise
        else:
            self._pool = pool

    @property
    def secure(self):
        return bool(self.config.secure)

    def session(self, **parameters):
        self._assert_open()
        if "acquire_timeout" not in parameters:
            parameters["acquire_timeout"] = self.acquire_timeout
        if "max_retry_time" not in parameters:
            parameters["max_retry_time"] = self.config.max_retry_time
        from neo4j.work.blocking import Session
        return Session(self._pool.acquire, **parameters)

    def rx_session(self, **parameters):
        raise NotImplementedError("Reactive sessions are not implemented "
                                  "for the %s class" % type(self).__name__)

    def pipeline(self, **parameters):
        raise NotImplementedError("Pipelines are not implemented "
                                  "for the %s class" % type(self).__name__)


class AsyncBoltDriver(Direct, AsyncDriver):

    pass


class AsyncNeo4jDriver(Routing, AsyncDriver):

    pass


class DriverError(Exception):
    """ Raised when an error occurs while using a driver.
    """

    def __init__(self, driver, *args):
        super(DriverError, self).__init__(*args)
        self.driver = driver
