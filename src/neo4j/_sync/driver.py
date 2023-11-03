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


from __future__ import annotations

import asyncio
import typing as t


if t.TYPE_CHECKING:
    import ssl
    import typing_extensions as te

    from .._api import (
        T_NotificationDisabledCategory,
        T_NotificationMinimumSeverity,
    )

from .._api import (
    RoutingControl,
    TelemetryAPI,
)
from .._async_compat.util import Util
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
    experimental_warn,
    unclosed_resource_warn,
)
from .._work import EagerResult
from ..addressing import Address
from ..api import (
    Auth,
    BookmarkManager,
    Bookmarks,
    DRIVER_BOLT,
    DRIVER_NEO4J,
    parse_neo4j_uri,
    parse_routing_context,
    READ_ACCESS,
    SECURITY_TYPE_SECURE,
    SECURITY_TYPE_SELF_SIGNED_CERTIFICATE,
    ServerInfo,
    TRUST_ALL_CERTIFICATES,
    TRUST_SYSTEM_CA_SIGNED_CERTIFICATES,
    URI_SCHEME_BOLT,
    URI_SCHEME_BOLT_SECURE,
    URI_SCHEME_BOLT_SELF_SIGNED_CERTIFICATE,
    URI_SCHEME_NEO4J,
    URI_SCHEME_NEO4J_SECURE,
    URI_SCHEME_NEO4J_SELF_SIGNED_CERTIFICATE,
    WRITE_ACCESS,
)
from ..auth_management import (
    AuthManager,
    AuthManagers,
)
from ..exceptions import Neo4jError
from .bookmark_manager import (
    Neo4jBookmarkManager,
    TBmConsumer as _TBmConsumer,
    TBmSupplier as _TBmSupplier,
)
from .work import (
    ManagedTransaction,
    Result,
    Session,
)


if t.TYPE_CHECKING:
    import ssl
    from enum import Enum

    import typing_extensions as te

    from .._api import T_RoutingControl
    from ..api import _TAuth


    class _DefaultEnum(Enum):
        default = "default"

    _default = _DefaultEnum.default

else:
    _default = object()

_T = t.TypeVar("_T")


class GraphDatabase:
    """Accessor for :class:`neo4j.Driver` construction.
    """

    if t.TYPE_CHECKING:

        @classmethod
        def driver(
            cls,
            uri: str,
            *,
            auth: t.Union[
                _TAuth,
                AuthManager,
            ] = ...,
            max_connection_lifetime: float = ...,
            max_connection_pool_size: int = ...,
            connection_timeout: float = ...,
            trust: t.Union[
                te.Literal["TRUST_ALL_CERTIFICATES"],
                te.Literal["TRUST_SYSTEM_CA_SIGNED_CERTIFICATES"]
            ] = ...,
            resolver: t.Union[
                t.Callable[[Address], t.Iterable[Address]],
                t.Callable[[Address], t.Union[t.Iterable[Address]]],
            ] = ...,
            encrypted: bool = ...,
            trusted_certificates: TrustStore = ...,
            ssl_context: ssl.SSLContext = ...,
            user_agent: str = ...,
            keep_alive: bool = ...,
            notifications_min_severity: t.Optional[
                T_NotificationMinimumSeverity
            ] = ...,
            notifications_disabled_categories: t.Optional[
                t.Iterable[T_NotificationDisabledCategory]
            ] = ...,

            # undocumented/unsupported options
            # they may be change or removed any time without prior notice
            connection_acquisition_timeout: float = ...,
            max_transaction_retry_time: float = ...,
            initial_retry_delay: float = ...,
            retry_delay_multiplier: float = ...,
            retry_delay_jitter_factor: float = ...,
            database: t.Optional[str] = ...,
            fetch_size: int = ...,
            impersonated_user: t.Optional[str] = ...,
            bookmark_manager: t.Union[BookmarkManager,
                                      BookmarkManager, None] = ...,
            telemetry_disabled: bool = ...,
        ) -> Driver:
            ...

    else:

        @classmethod
        def driver(
            cls, uri: str, *,
            auth: t.Union[
                _TAuth,
                AuthManager,
            ] = None,
            **config
        ) -> Driver:
            """Create a driver.

            :param uri: the connection URI for the driver,
                see :ref:`uri-ref` for available URIs.
            :param auth: the authentication details,
                see :ref:`auth-ref` for available authentication details.
            :param config: driver configuration key-word arguments,
                see :ref:`driver-configuration-ref` for available
                key-word arguments.
            """

            driver_type, security_type, parsed = parse_neo4j_uri(uri)

            if not isinstance(auth, AuthManager):
                auth = AuthManagers.static(auth)
            config["auth"] = auth

            # TODO: 6.0 - remove "trust" config option
            if "trust" in config.keys():
                if config["trust"] not in (
                    TRUST_ALL_CERTIFICATES,
                    TRUST_SYSTEM_CA_SIGNED_CERTIFICATES
                ):
                    from ..exceptions import ConfigurationError
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
                    "The config setting `trusted_certificates` must be of "
                    "type neo4j.TrustAll, neo4j.TrustCustomCAs, or"
                    "neo4j.TrustSystemCAs but was {}".format(
                        type(config["trusted_certificates"])
                    )
                )

            if (security_type in [SECURITY_TYPE_SELF_SIGNED_CERTIFICATE, SECURITY_TYPE_SECURE]
                and ("encrypted" in config.keys()
                     or "trust" in config.keys()
                     or "trusted_certificates" in config.keys()
                     or "ssl_context" in config.keys())):
                from ..exceptions import ConfigurationError

                # TODO: 6.0 - remove "trust" from error message
                raise ConfigurationError(
                    'The config settings "encrypted", "trust", '
                    '"trusted_certificates", and "ssl_context" can only be '
                    "used with the URI schemes {!r}. Use the other URI "
                    "schemes {!r} for setting encryption settings."
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
            _normalize_notifications_config(config)

            assert driver_type in (DRIVER_BOLT, DRIVER_NEO4J)
            if driver_type == DRIVER_BOLT:
                if parse_routing_context(parsed.query):
                    deprecation_warn(
                        "Creating a direct driver (`bolt://` scheme) with "
                        "routing context (URI parameters) is deprecated. They "
                        "will be ignored. This will raise an error in a "
                        'future release. Given URI "{}"'.format(uri),
                        stack_level=2
                    )
                    # TODO: 6.0 - raise instead of warning
                    # raise ValueError(
                    #     'Routing parameters are not supported with scheme '
                    #     '"bolt". Given URI "{}".'.format(uri)
                    # )
                return cls.bolt_driver(parsed.netloc, **config)
            # else driver_type == DRIVER_NEO4J
            routing_context = parse_routing_context(parsed.query)
            return cls.neo4j_driver(parsed.netloc,
                                    routing_context=routing_context, **config)

    @classmethod
    def bookmark_manager(
        cls,
        initial_bookmarks: t.Union[None, Bookmarks, t.Iterable[str]] = None,
        bookmarks_supplier: t.Optional[_TBmSupplier] = None,
        bookmarks_consumer: t.Optional[_TBmConsumer] = None
    ) -> BookmarkManager:
        """Create a :class:`.BookmarkManager` with default implementation.

        Basic usage example to configure sessions with the built-in bookmark
        manager implementation so that all work is automatically causally
        chained (i.e., all reads can observe all previous writes even in a
        clustered setup)::

            import neo4j


            # omitting closing the driver for brevity
            driver = neo4j.GraphDatabase.driver(...)
            bookmark_manager = neo4j.GraphDatabase.bookmark_manager(...)

            with driver.session(
                bookmark_manager=bookmark_manager
            ) as session1:
                with driver.session(
                    bookmark_manager=bookmark_manager,
                    access_mode=neo4j.READ_ACCESS
                ) as session2:
                    result1 = session1.run("<WRITE_QUERY>")
                    result1.consume()
                    # READ_QUERY is guaranteed to see what WRITE_QUERY wrote.
                    result2 = session2.run("<READ_QUERY>")
                    result2.consume()

        This is a very contrived example, and in this particular case, having
        both queries in the same session has the exact same effect and might
        even be more performant. However, when dealing with sessions spanning
        multiple threads, Tasks, processes, or even hosts, the bookmark
        manager can come in handy as sessions are not safe to be used
        concurrently.

        :param initial_bookmarks:
            The initial set of bookmarks. The returned bookmark manager will
            use this to initialize its internal bookmarks.
        :param bookmarks_supplier:
            Function which will be called every time the default bookmark
            manager's method :meth:`.BookmarkManager.get_bookmarks`
            gets called.
            The function takes no arguments and must return a
            :class:`.Bookmarks` object. The result of ``bookmarks_supplier``
            will then be concatenated with the internal set of bookmarks and
            used to configure the session in creation. It will, however, not
            update the internal set of bookmarks.
        :param bookmarks_consumer:
            Function which will be called whenever the set of bookmarks
            handled by the bookmark manager gets updated with the new
            internal bookmark set. It will receive the new set of bookmarks
            as a :class:`.Bookmarks` object and return :data:`None`.

        :returns: A default implementation of :class:`BookmarkManager`.

        .. versionadded:: 5.0

        .. versionchanged:: 5.3
            The bookmark manager no longer tracks bookmarks per database.
            This effectively changes the signature of almost all bookmark
            manager related methods:

            * ``initial_bookmarks`` is no longer a mapping from database name
              to bookmarks but plain bookmarks.
            * ``bookmarks_supplier`` no longer receives the database name as
              an argument.
            * ``bookmarks_consumer`` no longer receives the database name as
              an argument.

        .. versionchanged:: 5.8 Stabilized from experimental.
        """
        return Neo4jBookmarkManager(
            initial_bookmarks=initial_bookmarks,
            bookmarks_supplier=bookmarks_supplier,
            bookmarks_consumer=bookmarks_consumer
        )

    @classmethod
    def bolt_driver(cls, target, **config):
        """ Create a driver for direct Bolt server access that uses
        socket I/O and thread-based concurrency.
        """
        from .._exceptions import (
            BoltHandshakeError,
            BoltSecurityError,
        )

        try:
            return BoltDriver.open(target, **config)
        except (BoltHandshakeError, BoltSecurityError) as error:
            from ..exceptions import ServiceUnavailable
            raise ServiceUnavailable(str(error)) from error

    @classmethod
    def neo4j_driver(cls, *targets, routing_context=None, **config):
        """ Create a driver for routing-capable Neo4j service access
        that uses socket I/O and thread-based concurrency.
        """
        from .._exceptions import (
            BoltHandshakeError,
            BoltSecurityError,
        )

        try:
            return Neo4jDriver.open(*targets, routing_context=routing_context, **config)
        except (BoltHandshakeError, BoltSecurityError) as error:
            from ..exceptions import ServiceUnavailable
            raise ServiceUnavailable(str(error)) from error


class _Direct:

    # TODO: 6.0 - those attributes should be private
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

    # TODO: 6.0 - those attributes should be private
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
    """ Base class for all types of :class:`neo4j.Driver`, instances of
    which are used as the primary access point to Neo4j.
    """

    #: Connection pool
    _pool: t.Any = None

    #: Flag if the driver has been closed
    _closed = False

    def __init__(self, pool, default_workspace_config):
        assert pool is not None
        assert default_workspace_config is not None
        self._pool = pool
        self._default_workspace_config = default_workspace_config
        self._query_bookmark_manager = GraphDatabase.bookmark_manager()

    def __enter__(self) -> Driver:
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __del__(self):
        if not self._closed:
            unclosed_resource_warn(self)
        # TODO: 6.0 - remove this
        if not self._closed:
            if not Util.is_async_code:
                deprecation_warn(
                    "Relying on Driver's destructor to close the session "
                    "is deprecated. Please make sure to close the session. "
                    "Use it as a context (`with` statement) or make sure to "
                    "call `.close()` explicitly. Future versions of the "
                    "driver will not close drivers automatically."
                )
                self.close()

    def _check_state(self):
        if self._closed:
            # TODO: 6.0 - raise the error
            # raise DriverError("Driver closed")
            deprecation_warn(
                "Using a driver after it has been closed is deprecated. "
                "Future versions of the driver will raise an error.",
                stack_level=3
            )

    @property
    def encrypted(self) -> bool:
        """Indicate whether the driver was configured to use encryption."""
        return bool(self._pool.pool_config.encrypted)

    if t.TYPE_CHECKING:

        def session(
            self,
            *,
            connection_acquisition_timeout: float = ...,
            max_transaction_retry_time: float = ...,
            database: t.Optional[str] = ...,
            fetch_size: int = ...,
            impersonated_user: t.Optional[str] = ...,
            bookmarks: t.Union[t.Iterable[str], Bookmarks, None] = ...,
            default_access_mode: str = ...,
            bookmark_manager: t.Union[BookmarkManager,
                                      BookmarkManager, None] = ...,
            auth: _TAuth = ...,
            notifications_min_severity: t.Optional[
                T_NotificationMinimumSeverity
            ] = ...,
            notifications_disabled_categories: t.Optional[
                t.Iterable[T_NotificationDisabledCategory]
            ] = ...,

            # undocumented/unsupported options
            # they may be change or removed any time without prior notice
            initial_retry_delay: float = ...,
            retry_delay_multiplier: float = ...,
            retry_delay_jitter_factor: float = ...
        ) -> Session:
            ...

    else:

        def session(self, **config) -> Session:
            """Create a session, see :ref:`session-construction-ref`

            :param config: session configuration key-word arguments,
                see :ref:`session-configuration-ref` for available
                key-word arguments.

            :returns: new :class:`neo4j.Session` object
            """
            self._check_state()
            session_config = self._read_session_config(config)
            return self._session(session_config)

    def _session(self, session_config) -> Session:
        return Session(self._pool, session_config)

    def _read_session_config(self, config_kwargs):
        config = self._prepare_session_config(config_kwargs)
        session_config = SessionConfig(self._default_workspace_config,
                                       config)
        return session_config

    @classmethod
    def _prepare_session_config(cls, config_kwargs):
        _normalize_notifications_config(config_kwargs)
        return config_kwargs

    def close(self) -> None:
        """ Shut down, closing any open connections in the pool.
        """
        # TODO: 6.0 - NOOP if already closed
        # if self._closed:
        #     return
        try:
            self._pool.close()
        except asyncio.CancelledError:
            self._closed = True
            raise
        self._closed = True

    # overloads to work around https://github.com/python/mypy/issues/3737
    @t.overload
    def execute_query(
        self,
        query_: te.LiteralString,
        parameters_: t.Optional[t.Dict[str, t.Any]] = None,
        routing_: T_RoutingControl = RoutingControl.WRITE,
        database_: t.Optional[str] = None,
        impersonated_user_: t.Optional[str] = None,
        bookmark_manager_: t.Union[
            BookmarkManager, BookmarkManager, None
        ] = ...,
        auth_: _TAuth = None,
        result_transformer_: t.Callable[
            [Result], t.Union[EagerResult]
        ] = ...,
        **kwargs: t.Any
    ) -> EagerResult:
        ...

    @t.overload
    def execute_query(
        self,
        query_: te.LiteralString,
        parameters_: t.Optional[t.Dict[str, t.Any]] = None,
        routing_: T_RoutingControl = RoutingControl.WRITE,
        database_: t.Optional[str] = None,
        impersonated_user_: t.Optional[str] = None,
        bookmark_manager_: t.Union[
            BookmarkManager, BookmarkManager, None
        ] = ...,
        auth_: _TAuth = None,
        result_transformer_: t.Callable[
            [Result], t.Union[_T]
        ] = ...,
        **kwargs: t.Any
    ) -> _T:
        ...

    def execute_query(
        self,
        query_: te.LiteralString,
        parameters_: t.Optional[t.Dict[str, t.Any]] = None,
        routing_: T_RoutingControl = RoutingControl.WRITE,
        database_: t.Optional[str] = None,
        impersonated_user_: t.Optional[str] = None,
        bookmark_manager_: t.Union[
            BookmarkManager, BookmarkManager, None,
            te.Literal[_DefaultEnum.default]
        ] = _default,
        auth_: _TAuth = None,
        result_transformer_: t.Callable[
            [Result], t.Union[t.Any]
        ] = Result.to_eager_result,
        **kwargs: t.Any
    ) -> t.Any:
        """Execute a query in a transaction function and return all results.

        This method is a handy wrapper for lower-level driver APIs like
        sessions, transactions, and transaction functions. It is intended
        for simple use cases where there is no need for managing all possible
        options.

        The internal usage of transaction functions provides a retry-mechanism
        for appropriate errors. Furthermore, this means that queries using
        ``CALL {} IN TRANSACTIONS`` or the older ``USING PERIODIC COMMIT``
        will not work (use :meth:`Session.run` for these).

        The method is roughly equivalent to::

            def execute_query(
                query_, parameters_, routing_, database_, impersonated_user_,
                bookmark_manager_, auth_, result_transformer_, **kwargs
            ):
                def work(tx):
                    result = tx.run(query_, parameters_, **kwargs)
                    return result_transformer_(result)

                with driver.session(
                    database=database_,
                    impersonated_user=impersonated_user_,
                    bookmark_manager=bookmark_manager_,
                    auth=auth_,
                ) as session:
                    if routing_ == RoutingControl.WRITE:
                        return session.execute_write(work)
                    elif routing_ == RoutingControl.READ:
                        return session.execute_read(work)

        Usage example::

            from typing import List

            import neo4j


            def example(driver: neo4j.Driver) -> List[str]:
                \"""Get the name of all 42 year-olds.\"""
                records, summary, keys = driver.execute_query(
                    "MATCH (p:Person {age: $age}) RETURN p.name",
                    {"age": 42},
                    routing_=neo4j.RoutingControl.READ,  # or just "r"
                    database_="neo4j",
                )
                assert keys == ["p.name"]  # not needed, just for illustration
                # log_summary(summary)  # log some metadata
                return [str(record["p.name"]) for record in records]
                # or: return [str(record[0]) for record in records]
                # or even: return list(map(lambda r: str(r[0]), records))

        Another example::

            import neo4j


            def example(driver: neo4j.Driver) -> int:
                \"""Call all young people "My dear" and get their count.\"""
                record = driver.execute_query(
                    "MATCH (p:Person) WHERE p.age <= $age "
                    "SET p.nickname = 'My dear' "
                    "RETURN count(*)",
                    # optional routing parameter, as write is default
                    # routing_=neo4j.RoutingControl.WRITE,  # or just "w",
                    database_="neo4j",
                    result_transformer_=neo4j.Result.single,
                    age=15,
                )
                assert record is not None  # for typechecking and illustration
                count = record[0]
                assert isinstance(count, int)
                return count

        :param query_: cypher query to execute
        :type query_: typing.LiteralString
        :param parameters_: parameters to use in the query
        :type parameters_: typing.Optional[typing.Dict[str, typing.Any]]
        :param routing_:
            whether to route the query to a reader (follower/read replica) or
            a writer (leader) in the cluster. Default is to route to a writer.
        :type routing_: RoutingControl
        :param database_:
            database to execute the query against.

            None (default) uses the database configured on the server side.

            .. Note::
                It is recommended to always specify the database explicitly
                when possible. This allows the driver to work more efficiently,
                as it will not have to resolve the default database first.

            See also the Session config :ref:`database-ref`.
        :type database_: typing.Optional[str]
        :param impersonated_user_:
            Name of the user to impersonate.

            This means that all query will be executed in the security context
            of the impersonated user. For this, the user for which the
            :class:`Driver` has been created needs to have the appropriate
            permissions.

            See also the Session config :ref:`impersonated-user-ref`.
        :type impersonated_user_: typing.Optional[str]
        :param auth_:
            Authentication information to use for this query.

            By default, the driver configuration is used.

            See also the Session config :ref:`session-auth-ref`.
        :type auth_: typing.Tuple[typing.Any, typing.Any] | Auth | None
        :param result_transformer_:
            A function that gets passed the :class:`neo4j.Result` object
            resulting from the query and converts it to a different type. The
            result of the transformer function is returned by this method.

            .. warning::

                The transformer function must **not** return the
                :class:`neo4j.Result` itself.

            .. warning::

                N.B. the driver might retry the underlying transaction so the
                transformer might get invoked more than once (with different
                :class:`neo4j.Result` objects).
                Therefore, it needs to be idempotent (i.e., have the same
                effect, regardless if called once or many times).

            Example transformer that checks that exactly one record is in the
            result stream, then returns the record and the result summary::

                from typing import Tuple

                import neo4j


                def transformer(
                    result: neo4j.Result
                ) -> Tuple[neo4j.Record, neo4j.ResultSummary]:
                    record = result.single(strict=True)
                    summary = result.consume()
                    return record, summary

            Note that methods of :class:`neo4j.Result` that don't take
            mandatory arguments can be used directly as transformer functions.
            For example::

                import neo4j


                def example(driver: neo4j.Driver) -> neo4j.Record::
                    record = driver.execute_query(
                        "SOME QUERY",
                        result_transformer_=neo4j.Result.single
                    )


                # is equivalent to:


                def transformer(result: neo4j.Result) -> neo4j.Record:
                    return result.single()


                def example(driver: neo4j.Driver) -> neo4j.Record::
                    record = driver.execute_query(
                        "SOME QUERY",
                        result_transformer_=transformer
                    )

        :type result_transformer_:
            typing.Callable[[Result], typing.Union[T]]
        :param bookmark_manager_:
            Specify a bookmark manager to use.

            If present, the bookmark manager is used to keep the query causally
            consistent with all work executed using the same bookmark manager.

            Defaults to the driver's :attr:`.execute_query_bookmark_manager`.

            Pass :data:`None` to disable causal consistency.
        :type bookmark_manager_: BookmarkManager | BookmarkManager | None
        :param kwargs: additional keyword parameters. None of these can end
            with a single underscore. This is to avoid collisions with the
            keyword configuration parameters of this method. If you need to
            pass such a parameter, use the ``parameters_`` parameter instead.
            Parameters passed as kwargs take precedence over those passed in
            ``parameters_``.
        :type kwargs: typing.Any

        :returns: the result of the ``result_transformer_``
        :rtype: T

        .. versionadded:: 5.5

        .. versionchanged:: 5.8

            * Added ``auth_`` parameter in preview.
            * Stabilized from experimental.

        .. versionchanged:: 5.14
            Stabilized ``auth_`` parameter from preview.
        """
        self._check_state()
        invalid_kwargs = [k for k in kwargs if
                          k[-2:-1] != "_" and k[-1:] == "_"]
        if invalid_kwargs:
            raise ValueError(
                "keyword parameters must not end with a single '_'. Found: %r"
                "\nYou either misspelled an existing configuration parameter "
                "or tried to send a query parameter that is reserved. In the "
                "latter case, use the `parameters_` dictionary instead."
                % invalid_kwargs
            )
        parameters = dict(parameters_ or {}, **kwargs)

        if bookmark_manager_ is _default:
            bookmark_manager_ = self._query_bookmark_manager
        assert bookmark_manager_ is not _default

        session_config = self._read_session_config(
            {
                "database": database_,
                "impersonated_user": impersonated_user_,
                "bookmark_manager": bookmark_manager_,
                "auth": auth_,
            }
        )
        session = self._session(session_config)
        with session:
            if routing_ == RoutingControl.WRITE:
                access_mode = WRITE_ACCESS
            elif routing_ == RoutingControl.READ:
                access_mode = READ_ACCESS
            else:
                raise ValueError("Invalid routing control value: %r"
                                 % routing_)
            with session._pipelined_begin:
                return session._run_transaction(
                    access_mode, TelemetryAPI.DRIVER,
                    _work, (query_, parameters, result_transformer_), {}
                )

    @property
    def execute_query_bookmark_manager(self) -> BookmarkManager:
        """The driver's default query bookmark manager.

        This is the default :class:`BookmarkManager` used by
        :meth:`.execute_query`. This can be used to causally chain
        :meth:`.execute_query` calls and sessions. Example::

            def example(driver: neo4j.Driver) -> None:
                driver.execute_query("<QUERY 1>")
                with driver.session(
                    bookmark_manager=driver.execute_query_bookmark_manager
                ) as session:
                    # every query inside this session will be causally chained
                    # (i.e., can read what was written by <QUERY 1>)
                    session.run("<QUERY 2>")
                # subsequent execute_query calls will be causally chained
                # (i.e., can read what was written by <QUERY 2>)
                driver.execute_query("<QUERY 3>")

        .. versionadded:: 5.5

        .. versionchanged:: 5.8

            * Renamed from ``query_bookmark_manager`` to
              ``execute_query_bookmark_manager``.
            * Stabilized from experimental.
        """
        return self._query_bookmark_manager

    if t.TYPE_CHECKING:

        def verify_connectivity(
            self,
            *,
            # all arguments are experimental
            # they may be change or removed any time without prior notice
            session_connection_timeout: float = ...,
            connection_acquisition_timeout: float = ...,
            max_transaction_retry_time: float = ...,
            database: t.Optional[str] = ...,
            fetch_size: int = ...,
            impersonated_user: t.Optional[str] = ...,
            bookmarks: t.Union[t.Iterable[str], Bookmarks, None] = ...,
            default_access_mode: str = ...,
            bookmark_manager: t.Union[BookmarkManager,
                                      BookmarkManager, None] = ...,
            auth: t.Union[Auth, t.Tuple[t.Any, t.Any]] = ...,
            notifications_min_severity: t.Optional[
                T_NotificationMinimumSeverity
            ] = ...,
            notifications_disabled_categories: t.Optional[
                t.Iterable[T_NotificationDisabledCategory]
            ] = ...,

            # undocumented/unsupported options
            initial_retry_delay: float = ...,
            retry_delay_multiplier: float = ...,
            retry_delay_jitter_factor: float = ...
        ) -> None:
            ...

    else:

        def verify_connectivity(self, **config) -> None:
            """Verify that the driver can establish a connection to the server.

            This verifies if the driver can establish a reading connection to a
            remote server or a cluster. Some data will be exchanged.

            .. note::
                Even if this method raises an exception, the driver still needs
                to be closed via :meth:`close` to free up all resources.

            :param config: accepts the same configuration key-word arguments as
                :meth:`session`.

                .. warning::
                    All configuration key-word arguments are experimental.
                    They might be changed or removed in any future version
                    without prior notice.

            :raises Exception: if the driver cannot connect to the remote.
                Use the exception to further understand the cause of the
                connectivity problem.

            .. versionchanged:: 5.0
                The undocumented return value has been removed.
                If you need information about the remote server, use
                :meth:`get_server_info` instead.
            """
            self._check_state()
            if config:
                experimental_warn(
                    "All configuration key-word arguments to "
                    "verify_connectivity() are experimental. They might be "
                    "changed or removed in any future version without prior "
                    "notice."
                )
            session_config = self._read_session_config(config)
            self._get_server_info(session_config)

    if t.TYPE_CHECKING:

        def get_server_info(
            self,
            *,
            # all arguments are experimental
            # they may be change or removed any time without prior notice
            session_connection_timeout: float = ...,
            connection_acquisition_timeout: float = ...,
            max_transaction_retry_time: float = ...,
            database: t.Optional[str] = ...,
            fetch_size: int = ...,
            impersonated_user: t.Optional[str] = ...,
            bookmarks: t.Union[t.Iterable[str], Bookmarks, None] = ...,
            default_access_mode: str = ...,
            bookmark_manager: t.Union[BookmarkManager,
                                      BookmarkManager, None] = ...,
            auth: t.Union[Auth, t.Tuple[t.Any, t.Any]] = ...,
            notifications_min_severity: t.Optional[
                T_NotificationMinimumSeverity
            ] = ...,
            notifications_disabled_categories: t.Optional[
                t.Iterable[T_NotificationDisabledCategory]
            ] = ...,

            # undocumented/unsupported options
            initial_retry_delay: float = ...,
            retry_delay_multiplier: float = ...,
            retry_delay_jitter_factor: float = ...
        ) -> ServerInfo:
            ...

    else:

        def get_server_info(self, **config) -> ServerInfo:
            """Get information about the connected Neo4j server.

            Try to establish a working read connection to the remote server or
            a member of a cluster and exchange some data. Then return the
            contacted server's information.

            In a cluster, there is no guarantee about which server will be
            contacted.

            .. note::
                Even if this method raises an exception, the driver still needs
                to be closed via :meth:`close` to free up all resources.

            :param config: accepts the same configuration key-word arguments as
                :meth:`session`.

                .. warning::
                    All configuration key-word arguments are experimental.
                    They might be changed or removed in any future version
                    without prior notice.

            :raises Exception: if the driver cannot connect to the remote.
                Use the exception to further understand the cause of the
                connectivity problem.

            .. versionadded:: 5.0
            """
            self._check_state()
            if config:
                experimental_warn(
                    "All configuration key-word arguments to "
                    "get_server_info() are experimental. They might be "
                    "changed or removed in any future version without prior "
                    "notice."
                )
            session_config = self._read_session_config(config)
            return self._get_server_info(session_config)

    def supports_multi_db(self) -> bool:
        """ Check if the server or cluster supports multi-databases.

        :returns: Returns true if the server or cluster the driver connects to
            supports multi-databases, otherwise false.

        .. note::
            Feature support query based solely on the Bolt protocol version.
            The feature might still be disabled on the server side even if this
            function return :const:`True`. It just guarantees that the driver
            won't throw a :exc:`ConfigurationError` when trying to use this
            driver feature.
        """
        self._check_state()
        session_config = self._read_session_config({})
        with self._session(session_config) as session:
            session._connect(READ_ACCESS)
            assert session._connection
            return session._connection.supports_multiple_databases

    if t.TYPE_CHECKING:

        def verify_authentication(
            self,
            auth: t.Union[Auth, t.Tuple[t.Any, t.Any], None] = None,
            # all other arguments are experimental
            # they may be change or removed any time without prior notice
            session_connection_timeout: float = ...,
            connection_acquisition_timeout: float = ...,
            max_transaction_retry_time: float = ...,
            database: t.Optional[str] = ...,
            fetch_size: int = ...,
            impersonated_user: t.Optional[str] = ...,
            bookmarks: t.Union[t.Iterable[str], Bookmarks, None] = ...,
            default_access_mode: str = ...,
            bookmark_manager: t.Union[
                BookmarkManager, BookmarkManager, None
            ] = ...,

            # undocumented/unsupported options
            initial_retry_delay: float = ...,
            retry_delay_multiplier: float = ...,
            retry_delay_jitter_factor: float = ...
        ) -> bool:
            ...

    else:

        def verify_authentication(
            self,
            auth: t.Union[Auth, t.Tuple[t.Any, t.Any], None] = None,
            **config
        ) -> bool:
            """Verify that the authentication information is valid.

            Like :meth:`.verify_connectivity`, but for checking authentication.

            Try to establish a working read connection to the remote server or
            a member of a cluster and exchange some data. In a cluster, there
            is no guarantee about which server will be contacted. If the data
            exchange is successful and the authentication information is valid,
            :const:`True` is returned. Otherwise, the error will be matched
            against a list of known authentication errors. If the error is on
            that list, :const:`False` is returned indicating that the
            authentication information is invalid. Otherwise, the error is
            re-raised.

            :param auth: authentication information to verify.
                Same as the session config :ref:`auth-ref`.
            :param config: accepts the same configuration key-word arguments as
                :meth:`session`.

                .. warning::
                    All configuration key-word arguments (except ``auth``) are
                    experimental. They might be changed or removed in any
                    future version without prior notice.

            :raises Exception: if the driver cannot connect to the remote.
                Use the exception to further understand the cause of the
                connectivity problem.

            .. versionadded:: 5.8

            .. versionchanged:: 5.14 Stabilized from experimental.
            """
            self._check_state()
            if config:
                experimental_warn(
                    "All configuration key-word arguments but auth to "
                    "verify_authentication() are experimental. They might be "
                    "changed or removed in any future version without prior "
                    "notice."
                )
            if "database" not in config:
                config["database"] = "system"
            session_config = self._read_session_config(config)
            session_config = SessionConfig(session_config, {"auth": auth})
            with self._session(session_config) as session:
                try:
                    session._verify_authentication()
                except Neo4jError as exc:
                    if exc.code in (
                        "Neo.ClientError.Security.CredentialsExpired",
                        "Neo.ClientError.Security.Forbidden",
                        "Neo.ClientError.Security.TokenExpired",
                        "Neo.ClientError.Security.Unauthorized",
                    ):
                        return False
                    raise
            return True

    def supports_session_auth(self) -> bool:
        """Check if the remote supports connection re-authentication.

        :returns: Returns true if the server or cluster the driver connects to
            supports re-authentication of existing connections, otherwise
            false.

        .. note::
            Feature support query based solely on the Bolt protocol version.
            The feature might still be disabled on the server side even if this
            function return :const:`True`. It just guarantees that the driver
            won't throw a :exc:`ConfigurationError` when trying to use this
            driver feature.

        .. versionadded:: 5.8
        """
        self._check_state()
        session_config = self._read_session_config({})
        with self._session(session_config) as session:
            session._connect(READ_ACCESS)
            assert session._connection
            return session._connection.supports_re_auth

    def _get_server_info(self, session_config) -> ServerInfo:
        with self._session(session_config) as session:
            return session._get_server_info()


def _work(
    tx: ManagedTransaction,
    query: str,
    parameters: t.Dict[str, t.Any],
    transformer: t.Callable[[Result], t.Union[_T]]
) -> _T:
    res = tx.run(query, parameters)
    if transformer is Result.to_eager_result:
        return transformer(res)
    return transformer(res)


class BoltDriver(_Direct, Driver):
    """:class:`.BoltDriver` is instantiated for ``bolt`` URIs and
    addresses a single database machine. This may be a standalone server or
    could be a specific member of a cluster.

    Connections established by a :class:`.BoltDriver` are always made to
    the exact host and port detailed in the URI.

    This class is not supposed to be instantiated externally. Use
    :meth:`GraphDatabase.driver` instead.
    """

    @classmethod
    def open(cls, target, **config):
        """
        :param target:
        :param auth:
        :param config: The values that can be specified are found in :class: `neo4j.PoolConfig` and :class: `neo4j.WorkspaceConfig`

        :returns:
        :rtype: :class: `neo4j.BoltDriver`
        """
        from .io import BoltPool
        address = cls.parse_target(target)
        pool_config, default_workspace_config = Config.consume_chain(config, PoolConfig, WorkspaceConfig)
        pool = BoltPool.open(address, pool_config=pool_config, workspace_config=default_workspace_config)
        return cls(pool, default_workspace_config)

    def __init__(self, pool, default_workspace_config):
        _Direct.__init__(self, pool.address)
        Driver.__init__(self, pool, default_workspace_config)
        self._default_workspace_config = default_workspace_config


class Neo4jDriver(_Routing, Driver):
    """:class:`.Neo4jDriver` is instantiated for ``neo4j`` URIs. The
    routing behaviour works in tandem with Neo4j's `Causal Clustering
    <https://neo4j.com/docs/operations-manual/current/clustering/>`_
    feature by directing read and write behaviour to appropriate
    cluster members.

    This class is not supposed to be instantiated externally. Use
    :meth:`GraphDatabase.driver` instead.
    """

    @classmethod
    def open(cls, *targets, routing_context=None, **config):
        from .io import Neo4jPool
        addresses = cls.parse_targets(*targets)
        pool_config, default_workspace_config = Config.consume_chain(config, PoolConfig, WorkspaceConfig)
        pool = Neo4jPool.open(*addresses, routing_context=routing_context, pool_config=pool_config, workspace_config=default_workspace_config)
        return cls(pool, default_workspace_config)

    def __init__(self, pool, default_workspace_config):
        _Routing.__init__(self, [pool.address])
        Driver.__init__(self, pool, default_workspace_config)


def _normalize_notifications_config(config_kwargs):
    if config_kwargs.get("notifications_disabled_categories") is not None:
        config_kwargs["notifications_disabled_categories"] = [
            getattr(e, "value", e)
            for e in config_kwargs["notifications_disabled_categories"]
        ]
    if config_kwargs.get("notifications_min_severity") is not None:
        config_kwargs["notifications_min_severity"] = getattr(
            config_kwargs["notifications_min_severity"], "value",
            config_kwargs["notifications_min_severity"]
        )
