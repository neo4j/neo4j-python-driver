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


__all__ = [
    "__version__",
    "GraphDatabase",
    "Driver",
    "BoltDriver",
    "Neo4jDriver",
    "Auth",
    "AuthToken",
]

from logging import getLogger


from neo4j.addressing import (
    Address,
    IPv4Address,
    IPv6Address,
)
from neo4j.api import (
    Auth,  # TODO: Validate naming for Auth compared to other drivers.
    AuthToken,
    basic_auth,
    kerberos_auth,
    custom_auth,
    Bookmark,
    ServerInfo,
    Version,
    READ_ACCESS,
    WRITE_ACCESS,
    SYSTEM_DATABASE,
    DEFAULT_DATABASE,
    TRUST_ALL_CERTIFICATES,
    TRUST_SYSTEM_CA_SIGNED_CERTIFICATES,
)
from neo4j.conf import (
    Config,
    PoolConfig,
    WorkspaceConfig,
    SessionConfig,
)
from neo4j.meta import (
    experimental,
    get_user_agent,
    version as __version__,
)
from neo4j.data import (
    Record,
)
from neo4j.work.simple import (
    Transaction,
    Result,
    ResultSummary,
    Query,
    Session,
    unit_of_work,
)


log = getLogger("neo4j")


class GraphDatabase:
    """ Accessor for :class:`.Driver` construction.
    """

    @classmethod
    def driver(cls, uri, *, auth=None, **config):
        """ Create a Neo4j driver that uses socket I/O and thread-based
        concurrency.

        :param uri:

            ``bolt://host[:port]``

            **Settings:** BoltDriver with no encryption.

            ``bolt+ssc://host[:port]``

            **Settings:** BoltDriver with encryption (accepts self signed certificates).

            ``bolt+s://host[:port]``

            **Settings:** BoltDriver with encryption (accepts only certificates signed by an certificate authority), full certificate checks.

            ``neo4j://host[:port][?routing_context]``

            **Settings:** Neo4jDriver with no encryption.

            ``neo4j+ssc://host[:port][?routing_context]``

            **Settings:** Neo4jDriver with encryption (accepts self signed certificates).

            ``neo4j+s://host[:port][?routing_context]``

            **Settings:** Neo4jDriver with encryption (accepts only certificates signed by an certificate authority), full certificate checks.

        :param auth:
        :param config: connection configuration settings
        """

        from neo4j.api import (
            parse_neo4j_uri,
            parse_routing_context,
            DRIVER_BOLT,
            DRIVER_NEO4j,
            SECURITY_TYPE_NOT_SECURE,
            SECURITY_TYPE_SELF_SIGNED_CERTIFICATE,
            SECURITY_TYPE_SECURE,
            URI_SCHEME_BOLT,
            URI_SCHEME_NEO4J,
            URI_SCHEME_BOLT_SELF_SIGNED_CERTIFICATE,
            URI_SCHEME_BOLT_SECURE,
            URI_SCHEME_NEO4J_SELF_SIGNED_CERTIFICATE,
            URI_SCHEME_NEO4J_SECURE,
        )
        from neo4j.conf import (
            TRUST_ALL_CERTIFICATES,
            TRUST_SYSTEM_CA_SIGNED_CERTIFICATES
        )

        driver_type, security_type, parsed = parse_neo4j_uri(uri)

        if "trust" in config.keys():
            if config.get("trust") not in [TRUST_ALL_CERTIFICATES, TRUST_SYSTEM_CA_SIGNED_CERTIFICATES]:
                from neo4j.exceptions import ConfigurationError
                raise ConfigurationError("The config setting `trust` values are {!r}".format(
                    [
                        TRUST_ALL_CERTIFICATES,
                        TRUST_SYSTEM_CA_SIGNED_CERTIFICATES,
                    ]
                ))

        if security_type in [SECURITY_TYPE_SELF_SIGNED_CERTIFICATE, SECURITY_TYPE_SECURE] and ("encrypted" in config.keys() or "trust" in config.keys()):
            from neo4j.exceptions import ConfigurationError
            raise ConfigurationError("The config settings 'encrypted' and 'trust' can only be used with the URI schemes {!r}. Use the other URI schemes {!r} for setting encryption settings.".format(
                [
                    URI_SCHEME_BOLT,
                    URI_SCHEME_NEO4J,
                ],
                [
                    URI_SCHEME_BOLT_SELF_SIGNED_CERTIFICATE,
                    URI_SCHEME_BOLT_SECURE,
                    URI_SCHEME_NEO4J_SELF_SIGNED_CERTIFICATE,
                    URI_SCHEME_NEO4J_SECURE,
                ]
            ))

        if security_type == SECURITY_TYPE_SECURE:
            config["encrypted"] = True
        elif security_type == SECURITY_TYPE_SELF_SIGNED_CERTIFICATE:
            config["encrypted"] = True
            config["trust"] = TRUST_ALL_CERTIFICATES

        if driver_type == DRIVER_BOLT:
            return cls.bolt_driver(parsed.netloc, auth=auth, **config)
        elif driver_type == DRIVER_NEO4j:
            routing_context = parse_routing_context(parsed.query)
            return cls.neo4j_driver(parsed.netloc, auth=auth, routing_context=routing_context, **config)

    @classmethod
    def bolt_driver(cls, target, *, auth=None, **config):
        """ Create a driver for direct Bolt server access that uses
        socket I/O and thread-based concurrency.
        """
        from neo4j._exceptions import BoltHandshakeError, BoltSecurityError

        try:
            return BoltDriver.open(target, auth=auth, **config)
        except (BoltHandshakeError, BoltSecurityError) as error:
            from neo4j.exceptions import ServiceUnavailable
            raise ServiceUnavailable(str(error)) from error

    @classmethod
    def neo4j_driver(cls, *targets, auth=None, routing_context=None, **config):
        """ Create a driver for routing-capable Neo4j service access
        that uses socket I/O and thread-based concurrency.
        """
        from neo4j._exceptions import BoltHandshakeError, BoltSecurityError

        try:
            return Neo4jDriver.open(*targets, auth=auth, routing_context=routing_context, **config)
        except (BoltHandshakeError, BoltSecurityError) as error:
            from neo4j.exceptions import ServiceUnavailable
            raise ServiceUnavailable(str(error)) from error


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
    def encrypted(self):
        return bool(self._pool.pool_config.encrypted)

    def session(self, **config):
        """ Create a simple session.

        :param config: session configuration
                       (see :class:`.SessionConfig` for details)
        :returns: new :class:`.Session` object
        """
        raise NotImplementedError

    @experimental("The pipeline API is experimental and may be removed or "
                  "changed in a future release")
    def pipeline(self, **config):
        """ Create a pipeline.
        """
        raise NotImplementedError

    def close(self):
        """ Shut down, closing any open connections in the pool.
        """
        self._pool.close()

    def verify_connectivity(self, **config):
        """ This verifies if the driver can connect to a remote server or a cluster
        by establishing a network connection with the remote and possibly exchanging
        a few data before closing the connection. It throws exception if fails to connect.

        Use the exception to further understand the cause of the connectivity problem.

        Note: Even if this method throws an exception, the driver still need to be closed via close() to free up all resources.
        """
        raise NotImplementedError


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
        pool_config, default_workspace_config = Config.consume_chain(config, PoolConfig, WorkspaceConfig)
        pool = BoltPool.open(address, auth=auth, **pool_config)
        return cls(pool, default_workspace_config)

    def __init__(self, pool, default_workspace_config):
        Direct.__init__(self, pool.address)
        Driver.__init__(self, pool)
        self._default_workspace_config = default_workspace_config

    def session(self, **config):
        from neo4j.work.simple import Session
        session_config = SessionConfig(self._default_workspace_config, config)
        SessionConfig.consume(config)  # Consume the config
        return Session(self._pool, session_config)

    def pipeline(self, **config):
        from neo4j.work.pipelining import Pipeline, PipelineConfig
        pipeline_config = PipelineConfig(self._default_workspace_config, config)
        PipelineConfig.consume(config)  # Consume the config
        return Pipeline(self._pool, pipeline_config)

    def verify_connectivity(self, **config):
        server_agent = None
        with self.session(**config) as session:
            result = session.run("RETURN 1 AS x")
            value = result.single().value()
            summary = result.summary()
            server_agent = summary.server.agent
        return server_agent


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
        pool_config, default_workspace_config = Config.consume_chain(config, PoolConfig, WorkspaceConfig)
        pool = Neo4jPool.open(*addresses, auth=auth, routing_context=routing_context, **pool_config)
        return cls(pool, default_workspace_config)

    def __init__(self, pool, default_workspace_config):
        Routing.__init__(self, pool.routing_table.initial_routers)
        Driver.__init__(self, pool)
        self._default_workspace_config = default_workspace_config

    def session(self, **config):
        from neo4j.work.simple import Session
        session_config = SessionConfig(self._default_workspace_config, config)
        SessionConfig.consume(config)  # Consume the config
        return Session(self._pool, session_config)

    def pipeline(self, **config):
        from neo4j.work.pipelining import Pipeline, PipelineConfig
        pipeline_config = PipelineConfig(self._default_workspace_config, config)
        PipelineConfig.consume(config)  # Consume the config
        return Pipeline(self._pool, pipeline_config)

    def get_routing_table(self):
        return self._pool.routing_table

    def verify_connectivity(self, **config):
        """
        :raise ServiceUnavailable: raised if the server does not support routing or if routing support is broken.
        """
        # TODO: Improve and update Stub Test Server to be able to test.
        return self._verify_routing_connectivity()

    def _verify_routing_connectivity(self):
        from neo4j.exceptions import ServiceUnavailable
        from neo4j._exceptions import BoltHandshakeError

        table = self.get_routing_table()
        routing_info = {}
        for ix in list(table.routers):
            try:
                routing_info[ix] = self._pool.fetch_routing_info(table.routers[0])
            except BoltHandshakeError as error:
                routing_info[ix] = None

        for key, val in routing_info.items():
            if val is not None:
                return routing_info
        raise ServiceUnavailable("Could not connect to any routing servers.")
