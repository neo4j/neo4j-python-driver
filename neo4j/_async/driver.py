# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from .._async_compat.util import AsyncUtil
from .._conf import (
    Config,
    PoolConfig,
    SessionConfig,
    TrustAll,
    TrustStore,
    WorkspaceConfig,
)
from .._meta import (
    deprecation_warn,
    experimental,
    experimental_warn,
    unclosed_resource_warn,
)
from ..addressing import Address
from ..api import (
    READ_ACCESS,
    TRUST_ALL_CERTIFICATES,
    TRUST_SYSTEM_CA_SIGNED_CERTIFICATES,
)


class AsyncGraphDatabase:
    """Accessor for :class:`neo4j.Driver` construction.
    """

    @classmethod
    @AsyncUtil.experimental_async(
        "neo4j async is in experimental phase. It might be removed or changed "
        "at any time (including patch releases)."
    )
    def driver(cls, uri, *, auth=None, **config):
        """Create a driver.

        :param uri: the connection URI for the driver, see :ref:`async-uri-ref` for available URIs.
        :param auth: the authentication details, see :ref:`auth-ref` for available authentication details.
        :param config: driver configuration key-word arguments, see :ref:`async-driver-configuration-ref` for available key-word arguments.

        :rtype: AsyncNeo4jDriver or AsyncBoltDriver
        """

        from ..api import (
            DRIVER_BOLT,
            DRIVER_NEO4j,
            parse_neo4j_uri,
            parse_routing_context,
            SECURITY_TYPE_SECURE,
            SECURITY_TYPE_SELF_SIGNED_CERTIFICATE,
            URI_SCHEME_BOLT,
            URI_SCHEME_BOLT_SECURE,
            URI_SCHEME_BOLT_SELF_SIGNED_CERTIFICATE,
            URI_SCHEME_NEO4J,
            URI_SCHEME_NEO4J_SECURE,
            URI_SCHEME_NEO4J_SELF_SIGNED_CERTIFICATE,
        )

        driver_type, security_type, parsed = parse_neo4j_uri(uri)

        # TODO: 6.0 remove "trust" config option
        if "trust" in config.keys():
            if config["trust"] not in (TRUST_ALL_CERTIFICATES,
                                       TRUST_SYSTEM_CA_SIGNED_CERTIFICATES):
                from neo4j.exceptions import ConfigurationError
                raise ConfigurationError(
                    "The config setting `trust` values are {!r}"
                    .format(
                        [
                            TRUST_ALL_CERTIFICATES,
                            TRUST_SYSTEM_CA_SIGNED_CERTIFICATES,
                        ]
                    )
                )

        if ("trusted_certificates" in config.keys()
            and not isinstance(config["trusted_certificates"],
                               TrustStore)):
            raise ConnectionError(
                "The config setting `trusted_certificates` must be of type "
                "neo4j.TrustAll, neo4j.TrustCustomCAs, or"
                "neo4j.TrustSystemCAs but was {}".format(
                    type(config["trusted_certificates"])
                )
            )

        if (security_type in [SECURITY_TYPE_SELF_SIGNED_CERTIFICATE, SECURITY_TYPE_SECURE]
            and ("encrypted" in config.keys()
                 or "trust" in config.keys()
                 or "trusted_certificates" in config.keys()
                 or "ssl_context" in config.keys())):
            from neo4j.exceptions import ConfigurationError

            # TODO: 6.0 remove "trust" from error message
            raise ConfigurationError(
                'The config settings "encrypted", "trust", '
                '"trusted_certificates", and "ssl_context" can only be used '
                "with the URI schemes {!r}. Use the other URI schemes {!r} "
                "for setting encryption settings."
                .format(
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
                )
            )

        if security_type == SECURITY_TYPE_SECURE:
            config["encrypted"] = True
        elif security_type == SECURITY_TYPE_SELF_SIGNED_CERTIFICATE:
            config["encrypted"] = True
            config["trusted_certificates"] = TrustAll()

        if driver_type == DRIVER_BOLT:
            if parse_routing_context(parsed.query):
                deprecation_warn(
                    "Creating a direct driver (`bolt://` scheme) with routing "
                    "context (URI parameters) is deprecated. They will be "
                    "ignored. This will raise an error in a future release. "
                    'Given URI "{}"'.format(uri),
                    stack_level=2
                )
                # TODO: 6.0 - raise instead of warning
                # raise ValueError(
                #     'Routing parameters are not supported with scheme '
                #     '"bolt". Given URI "{}".'.format(uri)
                # )
            return cls.bolt_driver(parsed.netloc, auth=auth, **config)
        elif driver_type == DRIVER_NEO4j:
            routing_context = parse_routing_context(parsed.query)
            return cls.neo4j_driver(parsed.netloc, auth=auth, routing_context=routing_context, **config)

    @classmethod
    def bolt_driver(cls, target, *, auth=None, **config):
        """ Create a driver for direct Bolt server access that uses
        socket I/O and thread-based concurrency.
        """
        from .._exceptions import (
            BoltHandshakeError,
            BoltSecurityError,
        )

        try:
            return AsyncBoltDriver.open(target, auth=auth, **config)
        except (BoltHandshakeError, BoltSecurityError) as error:
            from neo4j.exceptions import ServiceUnavailable
            raise ServiceUnavailable(str(error)) from error

    @classmethod
    def neo4j_driver(cls, *targets, auth=None, routing_context=None, **config):
        """ Create a driver for routing-capable Neo4j service access
        that uses socket I/O and thread-based concurrency.
        """
        from neo4j._exceptions import (
            BoltHandshakeError,
            BoltSecurityError,
        )

        try:
            return AsyncNeo4jDriver.open(*targets, auth=auth, routing_context=routing_context, **config)
        except (BoltHandshakeError, BoltSecurityError) as error:
            from neo4j.exceptions import ServiceUnavailable
            raise ServiceUnavailable(str(error)) from error


class _Direct:

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


class _Routing:

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


class AsyncDriver:
    """ Base class for all types of :class:`neo4j.AsyncDriver`, instances of
    which are used as the primary access point to Neo4j.
    """

    #: Connection pool
    _pool = None

    #: Flag if the driver has been closed
    _closed = False

    def __init__(self, pool, default_workspace_config):
        assert pool is not None
        assert default_workspace_config is not None
        self._pool = pool
        self._default_workspace_config = default_workspace_config

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    def __del__(self):
        if not self._closed:
            unclosed_resource_warn(self)
        # TODO: 6.0 - remove this
        if not self._closed:
            if not AsyncUtil.is_async_code:
                deprecation_warn(
                    "Relying on AsyncDriver's destructor to close the session "
                    "is deprecated. Please make sure to close the session. "
                    "Use it as a context (`with` statement) or make sure to "
                    "call `.close()` explicitly. Future versions of the "
                    "driver will not close drivers automatically."
                )
                self.close()

    @property
    def encrypted(self):
        """Indicate whether the driver was configured to use encryption.

        :rtype: bool"""
        return bool(self._pool.pool_config.encrypted)

    def session(self, **config):
        """Create a session, see :ref:`async-session-construction-ref`

        :param config: session configuration key-word arguments,
            see :ref:`async-session-configuration-ref` for available key-word
            arguments.

        :returns: new :class:`neo4j.AsyncSession` object
        """
        raise NotImplementedError

    async def close(self):
        """ Shut down, closing any open connections in the pool.
        """
        await self._pool.close()
        self._closed = True

    # TODO: 6.0 - remove config argument
    async def verify_connectivity(self, **config):
        """Verify that the driver can establish a connection to the server.

        This verifies if the driver can establish a reading connection to a
        remote server or a cluster. Some data will be exchanged.

        .. note::
            Even if this method raises an exception, the driver still needs to
            be closed via :meth:`close` to free up all resources.

        :param config: accepts the same configuration key-word arguments as
            :meth:`session`.

            .. warning::
                All configuration key-word arguments are experimental.
                They might be changed or removed in any future version without
                prior notice.

        :raises DriverError: if the driver cannot connect to the remote.
            Use the exception to further understand the cause of the
            connectivity problem.

        .. versionchanged:: 5.0
            The undocumented return value has been removed.
            If you need information about the remote server, use
            :meth:`get_server_info` instead.
        """
        if config:
            experimental_warn(
                "All configuration key-word arguments to "
                "verify_connectivity() are experimental. They might be "
                "changed or removed in any future version without prior "
                "notice."
            )
        async with self.session(**config) as session:
            await session._get_server_info()

    async def get_server_info(self, **config):
        """Get information about the connected Neo4j server.

        Try to establish a working read connection to the remote server or a
        member of a cluster and exchange some data. Then return the contacted
        server's information.

        In a cluster, there is no guarantee about which server will be
        contacted.

        .. note::
            Even if this method raises an exception, the driver still needs to
            be closed via :meth:`close` to free up all resources.

        :param config: accepts the same configuration key-word arguments as
            :meth:`session`.

            .. warning::
                All configuration key-word arguments are experimental.
                They might be changed or removed in any future version without
                prior notice.

        :rtype: ServerInfo

        :raises DriverError: if the driver cannot connect to the remote.
            Use the exception to further understand the cause of the
            connectivity problem.

        .. versionadded:: 5.0
        """
        if config:
            experimental_warn(
                "All configuration key-word arguments to "
                "verify_connectivity() are experimental. They might be "
                "changed or removed in any future version without prior "
                "notice."
            )
        async with self.session(**config) as session:
            return await session._get_server_info()

    @experimental("Feature support query, based on Bolt protocol version and Neo4j server version will change in the future.")
    async def supports_multi_db(self):
        """ Check if the server or cluster supports multi-databases.

        :return: Returns true if the server or cluster the driver connects to supports multi-databases, otherwise false.
        :rtype: bool

        .. note::
            Feature support query, based on Bolt Protocol Version and Neo4j
            server version will change in the future.
        """
        async with self.session() as session:
            await session._connect(READ_ACCESS)
            return session._connection.supports_multiple_databases

    async def query(self, query, parameters=None, **kwargs):
        """
        Run a Cypher query within an managed transaction and
        all the retries policy will be applied.

        The query is sent and the result header received
        immediately and the :class:`neo4j.QueryResult`is
        fetched.

        For more usage details, see :meth:`.AsyncTransaction.query`.

        For auto-commit queries, use `AsyncSession.run`.

        For access to the neo4j.AsyncResult object,
        use `AsyncDriver.execute` and `.AsyncTransaction.run`

        :param query: cypher query
        :type query: str, neo4j.Query
        :param parameters: dictionary of parameters
        :type parameters: dict
        :param kwargs: additional keyword parameters

        :returns: a new :class:`neo4j.QueryResult` object
        :rtype: QueryResult
        """
        session_kwargs = {}
        if "database" in kwargs:
            session_kwargs["database"] = kwargs.pop("database")

        async with self.session(**session_kwargs) as session:
            return await session.query(query, parameters, **kwargs)

    async def execute(self, transaction_function, *args, **kwargs):
        """Execute a unit of work in a managed transaction.

        .. note::
            This does not necessarily imply access control, see the session
            configuration option :ref:`default-access-mode-ref`.

        This transaction will automatically be committed unless an exception
        is thrown during query execution or by the user code.
        Note, that this function perform retries and that the supplied `
        transaction_function` might get invoked more than once.

        Managed transactions should not generally be explicitly committed
        (via ``tx.commit()``).

        Example::

            async def do_cypher_tx(tx, cypher):
                records, _ = await tx.query(cypher)
                return records

            values = await driver.execute(do_cypher_tx, "RETURN 1 AS x")

        Example::

            async def do_cypher_tx(tx):
                records, _ = await tx.query("RETURN 1 AS x")
                return records

            values = await driver.execute(
                do_cypher_tx,
                database="neo4j",
                cluster_member_access=neo4j.api.CLUSTER_READERS_ACCESS
            )

        Example::

            async def get_two_tx(tx):
                result = await tx.run("UNWIND [1,2,3,4] AS x RETURN x")
                values = []
                async for record in result:
                    if len(values) >= 2:
                        break
                    values.append(record.values())
                # or shorter: values = [record.values()
                #                       for record in await result.fetch(2)]

                # discard the remaining records if there are any
                summary = await result.consume()
                # use the summary for logging etc.
                return values


            values = await driver.execute(get_two_tx)

        :param transaction_function: a function that takes a transaction as an
            argument and does work with the transaction.
            ``transaction_function(tx, *args, **kwargs)`` where ``tx`` is a
            :class:`.Transaction`.
        :param args: arguments for the ``transaction_function``
        :param kwargs: key word arguments for the ``transaction_function``
        :return: a result as returned by the given unit of work
        """
        session_kwargs = {}
        if "database" in kwargs:
            session_kwargs["database"] = kwargs.pop("database")

        async with self.session(**session_kwargs) as session:
            return await session.execute(transaction_function, *args, **kwargs)


class AsyncBoltDriver(_Direct, AsyncDriver):
    """:class:`.AsyncBoltDriver` is instantiated for ``bolt`` URIs and
    addresses a single database machine. This may be a standalone server or
    could be a specific member of a cluster.

    Connections established by a :class:`.AsyncBoltDriver` are always made to
    the exact host and port detailed in the URI.

    This class is not supposed to be instantiated externally. Use
    :meth:`AsyncGraphDatabase.driver` instead.
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
        from .io import AsyncBoltPool
        address = cls.parse_target(target)
        pool_config, default_workspace_config = Config.consume_chain(config, PoolConfig, WorkspaceConfig)
        pool = AsyncBoltPool.open(address, auth=auth, pool_config=pool_config, workspace_config=default_workspace_config)
        return cls(pool, default_workspace_config)

    def __init__(self, pool, default_workspace_config):
        _Direct.__init__(self, pool.address)
        AsyncDriver.__init__(self, pool, default_workspace_config)
        self._default_workspace_config = default_workspace_config

    def session(self, **config):
        """
        :param config: The values that can be specified are found in :class: `neo4j.SessionConfig`

        :return:
        :rtype: :class: `neo4j.AsyncSession`
        """
        from .work import AsyncSession
        session_config = SessionConfig(self._default_workspace_config, config)
        SessionConfig.consume(config)  # Consume the config
        return AsyncSession(self._pool, session_config)


class AsyncNeo4jDriver(_Routing, AsyncDriver):
    """:class:`.AsyncNeo4jDriver` is instantiated for ``neo4j`` URIs. The
    routing behaviour works in tandem with Neo4j's `Causal Clustering
    <https://neo4j.com/docs/operations-manual/current/clustering/>`_
    feature by directing read and write behaviour to appropriate
    cluster members.

    This class is not supposed to be instantiated externally. Use
    :meth:`AsyncGraphDatabase.driver` instead.
    """

    @classmethod
    def open(cls, *targets, auth=None, routing_context=None, **config):
        from .io import AsyncNeo4jPool
        addresses = cls.parse_targets(*targets)
        pool_config, default_workspace_config = Config.consume_chain(config, PoolConfig, WorkspaceConfig)
        pool = AsyncNeo4jPool.open(*addresses, auth=auth, routing_context=routing_context, pool_config=pool_config, workspace_config=default_workspace_config)
        return cls(pool, default_workspace_config)

    def __init__(self, pool, default_workspace_config):
        _Routing.__init__(self, pool.get_default_database_initial_router_addresses())
        AsyncDriver.__init__(self, pool, default_workspace_config)

    def session(self, **config):
        from .work import AsyncSession
        session_config = SessionConfig(self._default_workspace_config, config)
        SessionConfig.consume(config)  # Consume the config
        return AsyncSession(self._pool, session_config)
