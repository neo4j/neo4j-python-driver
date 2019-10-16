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
    "Auth",
    "AuthToken",
]

from logging import getLogger
from urllib.parse import urlparse, parse_qs

from neo4j.addressing import Address
from neo4j.api import *
from neo4j.conf import Config, PoolConfig, SessionConfig
from neo4j.exceptions import ConnectionExpired, ServiceUnavailable
from neo4j.meta import experimental, get_user_agent, version as __version__


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


class Direct:

    default_host = "localhost"
    default_port = 7687

    default_target = ":"

    def __init__(self, address):
        self._address = address

    @property
    def address(self):
        return self._address

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

    def __init__(self, initial_addresses):
        self._initial_addresses = initial_addresses

    @property
    def initial_addresses(self):
        return self._initial_addresses

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


class Driver:
    """ Base class for all types of :class:`.Driver`, instances of which are
    used as the primary access point to Neo4j.

    :param uri: URI for a graph database service
    :param config: configuration and authentication details
    """

    #: Connection pool
    _pool = None

    def __init__(self, pool):
        assert pool is not None
        self._pool = pool

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    @property
    def secure(self):
        return bool(self._pool.config.secure)

    def session(self, **config):
        """ Create a new :class:`.Session` object based on this
        :class:`.Driver`.

        :param config: custom session parameters (see
                           :class:`.Session` for details)
        :returns: new :class:`.Session` object
        """
        raise NotImplementedError("Blocking sessions are not implemented "
                                  "for the %s class" % type(self).__name__)

    @experimental("The pipeline API is experimental and may be removed or "
                  "changed in a future release")
    def pipeline(self, **config):
        """ Create a new :class:`.Pipeline` objects based on this
        :class:`.Driver`.
        """
        raise NotImplementedError("Pipelines are not implemented "
                                  "for the %s class" % type(self).__name__)

    def close(self):
        """ Shut down, closing any open connections in the pool.
        """
        self._pool.close()


class AsyncDriver:

    @classmethod
    async def open(cls, uri, **config):
        raise NotImplementedError

    def __init__(self, pool):
        self._pool = pool

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def session(self, **config):
        raise NotImplementedError

    async def close(self):
        await self._pool.close()


class BoltDriver(Direct, Driver):
    """ A :class:`.BoltDriver` is created from a ``bolt`` URI and addresses
    a single database machine. This may be a standalone server or could be a
    specific member of a cluster.

    Connections established by a :class:`.BoltDriver` are always made to the
    exact host and port detailed in the URI.
    """

    @classmethod
    def open(cls, target, *, auth=None, **config):
        from neo4j.io import BoltPool
        address = cls.parse_target(target)
        pool_config, session_config = Config.consume_chain(config, PoolConfig, SessionConfig)
        pool = BoltPool.open(address, auth=auth, **pool_config)
        return cls(pool, session_config)

    def __init__(self, pool, session_config):
        Direct.__init__(self, pool.address)
        Driver.__init__(self, pool)
        self._session_config = session_config

    def session(self, **config):
        from neo4j.work.simple import Session
        session_config = SessionConfig(self._session_config, SessionConfig.consume(config))
        return Session(self._pool, session_config)

    def pipeline(self, **config):
        from neo4j.work.pipelining import Pipeline
        return Pipeline(self._pool, **config)


class Neo4jDriver(Routing, Driver):
    """ A :class:`.Neo4jDriver` is created from a ``neo4j`` URI. The
    routing behaviour works in tandem with Neo4j's `Causal Clustering
    <https://neo4j.com/docs/operations-manual/current/clustering/>`_
    feature by directing read and write behaviour to appropriate
    cluster members.
    """

    @classmethod
    def open(cls, *targets, auth=None, routing_context=None, **config):
        from neo4j.io import Neo4jPool
        addresses = cls.parse_targets(*targets)
        pool_config, session_config = Config.consume_chain(config, PoolConfig, SessionConfig)
        pool = Neo4jPool.open(*addresses, auth=auth, routing_context=routing_context, **pool_config)
        return cls(pool, session_config)

    def __init__(self, pool, session_config):
        Routing.__init__(self, pool.routing_table.initial_routers)
        Driver.__init__(self, pool)
        self._session_config = session_config

    def session(self, **config):
        from neo4j.work.simple import Session
        session_config = SessionConfig(self._session_config, SessionConfig.consume(config))
        return Session(self._pool, session_config)

    def pipeline(self, **config):
        from neo4j.work.pipelining import Pipeline
        return Pipeline(self._pool, **config)


class AsyncBoltDriver(Direct, AsyncDriver):

    @classmethod
    async def open(cls, target, *, auth=None, loop=None, **config):
        from neo4j.aio import BoltPool
        address = cls.parse_target(target)
        pool_config, session_config = Config.consume_chain(config, PoolConfig, SessionConfig)
        pool = await BoltPool.open(address, auth=auth, loop=loop, **pool_config)
        return cls(pool, session_config)

    def __init__(self, pool, session_config):
        Direct.__init__(self, pool.address)
        AsyncDriver.__init__(self, pool)
        self._session_config = session_config

    def session(self, **config):
        raise NotImplementedError  # TODO: reactive session


class AsyncNeo4jDriver(Routing, AsyncDriver):

    @classmethod
    async def open(cls, *targets, auth=None, routing_context=None, loop=None, **config):
        from neo4j.aio import Neo4jPool
        addresses = cls.parse_targets(*targets)
        pool_config, session_config = Config.consume_chain(config, PoolConfig, SessionConfig)
        pool = await Neo4jPool.open(*addresses, auth=auth, routing_context=routing_context,
                                    loop=loop, **pool_config)
        return cls(pool, session_config)

    def __init__(self, pool, session_config):
        Routing.__init__(self, pool.routing_table.initial_routers)
        AsyncDriver.__init__(self, pool)
        self._session_config = session_config

    def session(self, **config):
        raise NotImplementedError  # TODO: reactive session
