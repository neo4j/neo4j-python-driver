# Copyright (c) "Neo4j"
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


from .addressing import Address
from .api import READ_ACCESS
from .conf import (
    Config,
    PoolConfig,
    SessionConfig,
    WorkspaceConfig,
)
from .meta import experimental
from .work.simple import Session


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
        addresses = Address.parse_list(targets, default_host=cls.default_host, default_port=cls.default_port)
        return addresses


class Driver:
    """ Base class for all types of :class:`neo4j.Driver`, instances of which are
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
        """Create a session, see :ref:`session-construction-ref`

        :param config: session configuration key-word arguments, see :ref:`session-configuration-ref` for available key-word arguments.

        :returns: new :class:`neo4j.Session` object
        """
        raise NotImplementedError

    @experimental("The pipeline API is experimental and may be removed or changed in a future release")
    def pipeline(self, **config):
        """ Create a pipeline.
        """
        raise NotImplementedError

    def close(self):
        """ Shut down, closing any open connections in the pool.
        """
        self._pool.close()

    @experimental("The configuration may change in the future.")
    def verify_connectivity(self, **config):
        """ This verifies if the driver can connect to a remote server or a cluster
        by establishing a network connection with the remote and possibly exchanging
        a few data before closing the connection. It throws exception if fails to connect.

        Use the exception to further understand the cause of the connectivity problem.

        Note: Even if this method throws an exception, the driver still need to be closed via close() to free up all resources.
        """
        raise NotImplementedError

    @experimental("Feature support query, based on Bolt Protocol Version and Neo4j Server Version will change in the future.")
    def supports_multi_db(self):
        """ Check if the server or cluster supports multi-databases.

        :return: Returns true if the server or cluster the driver connects to supports multi-databases, otherwise false.
        :rtype: bool
        """
        with self.session() as session:
            session._connect(READ_ACCESS)
            return session._connection.supports_multiple_databases


class BoltDriver(Direct, Driver):
    """ A :class:`.BoltDriver` is created from a ``bolt`` URI and addresses
    a single database machine. This may be a standalone server or could be a
    specific member of a cluster.

    Connections established by a :class:`.BoltDriver` are always made to the
    exact host and port detailed in the URI.
    """

    @classmethod
    def open(cls, target, *, auth=None, **config):
        """
        :param target:
        :param auth:
        :param config: The values that can be specified are found in :class: `neo4j.PoolConfig` and :class: `neo4j.WorkspaceConfig`

        :return:
        :rtype: :class: `neo4j.BoltDriver`
        """
        from neo4j.io import BoltPool
        address = cls.parse_target(target)
        pool_config, default_workspace_config = Config.consume_chain(config, PoolConfig, WorkspaceConfig)
        pool = BoltPool.open(address, auth=auth, pool_config=pool_config, workspace_config=default_workspace_config)
        return cls(pool, default_workspace_config)

    def __init__(self, pool, default_workspace_config):
        Direct.__init__(self, pool.address)
        Driver.__init__(self, pool)
        self._default_workspace_config = default_workspace_config

    def session(self, **config):
        """
        :param config: The values that can be specified are found in :class: `neo4j.SessionConfig`

        :return:
        :rtype: :class: `neo4j.Session`
        """
        from neo4j.work.simple import Session
        session_config = SessionConfig(self._default_workspace_config, config)
        SessionConfig.consume(config)  # Consume the config
        return Session(self._pool, session_config)

    def pipeline(self, **config):
        from neo4j.work.pipelining import (
            Pipeline,
            PipelineConfig,
        )
        pipeline_config = PipelineConfig(self._default_workspace_config, config)
        PipelineConfig.consume(config)  # Consume the config
        return Pipeline(self._pool, pipeline_config)

    @experimental("The configuration may change in the future.")
    def verify_connectivity(self, **config):
        server_agent = None
        config["fetch_size"] = -1
        with self.session(**config) as session:
            result = session.run("RETURN 1 AS x")
            value = result.single().value()
            summary = result.consume()
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
        pool = Neo4jPool.open(*addresses, auth=auth, routing_context=routing_context, pool_config=pool_config, workspace_config=default_workspace_config)
        return cls(pool, default_workspace_config)

    def __init__(self, pool, default_workspace_config):
        Routing.__init__(self, pool.get_default_database_initial_router_addresses())
        Driver.__init__(self, pool)
        self._default_workspace_config = default_workspace_config

    def session(self, **config):
        session_config = SessionConfig(self._default_workspace_config, config)
        SessionConfig.consume(config)  # Consume the config
        return Session(self._pool, session_config)

    def pipeline(self, **config):
        from neo4j.work.pipelining import (
            Pipeline,
            PipelineConfig,
        )
        pipeline_config = PipelineConfig(self._default_workspace_config, config)
        PipelineConfig.consume(config)  # Consume the config
        return Pipeline(self._pool, pipeline_config)

    @experimental("The configuration may change in the future.")
    def verify_connectivity(self, **config):
        """
        :raise ServiceUnavailable: raised if the server does not support routing or if routing support is broken.
        """
        # TODO: Improve and update Stub Test Server to be able to test.
        return self._verify_routing_connectivity()

    def _verify_routing_connectivity(self):
        from neo4j.exceptions import (
            Neo4jError,
            ServiceUnavailable,
            SessionExpired,
        )

        table = self._pool.get_routing_table_for_default_database()
        routing_info = {}
        for ix in list(table.routers):
            try:
                routing_info[ix] = self._pool.fetch_routing_info(
                    address=table.routers[0],
                    database=self._default_workspace_config.database,
                    imp_user=self._default_workspace_config.impersonated_user,
                    bookmarks=None,
                    timeout=self._default_workspace_config
                                .connection_acquisition_timeout
                )
            except (ServiceUnavailable, SessionExpired, Neo4jError):
                routing_info[ix] = None
        for key, val in routing_info.items():
            if val is not None:
                return routing_info
        raise ServiceUnavailable("Could not connect to any routing servers.")
