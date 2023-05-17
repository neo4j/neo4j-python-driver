.. _api-documentation:

#################
API Documentation
#################

*************
GraphDatabase
*************

Driver Construction
===================

The :class:`neo4j.Driver` construction is done via a ``classmethod`` on the :class:`neo4j.GraphDatabase` class.

.. autoclass:: neo4j.GraphDatabase
   :members: bookmark_manager

    .. automethod:: driver

        Driver creation example:

        .. code-block:: python

            from neo4j import GraphDatabase


            uri = "neo4j://example.com:7687"
            driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))

            driver.close()  # close the driver object


        For basic authentication, ``auth`` can be a simple tuple, for example:

        .. code-block:: python

           auth = ("neo4j", "password")

        This will implicitly create a :class:`neo4j.Auth` with ``scheme="basic"``.
        Other authentication methods are described under :ref:`auth-ref`.


        ``with`` block context example:

        .. code-block:: python

            from neo4j import GraphDatabase


            uri = "neo4j://example.com:7687"
            with GraphDatabase.driver(uri, auth=("neo4j", "password")) as driver:
                ...  # use the driver


.. _uri-ref:

URI
===

On construction, the ``scheme`` of the URI determines the type of :class:`neo4j.Driver` object created.

Available valid URIs:

+ ``bolt://host[:port]``
+ ``bolt+ssc://host[:port]``
+ ``bolt+s://host[:port]``
+ ``neo4j://host[:port][?routing_context]``
+ ``neo4j+ssc://host[:port][?routing_context]``
+ ``neo4j+s://host[:port][?routing_context]``

.. code-block:: python

    uri = "bolt://example.com:7687"

.. code-block:: python

    uri = "neo4j://example.com:7687?policy=europe"

Each supported scheme maps to a particular :class:`neo4j.Driver` subclass that implements a specific behaviour.

+------------------------+---------------------------------------------------------------------------------------------------------------------------------------+
| URI Scheme             | Driver Object and Setting                                                                                                             |
+========================+=======================================================================================================================================+
| bolt                   | :ref:`bolt-driver-ref` with no encryption.                                                                                            |
+------------------------+---------------------------------------------------------------------------------------------------------------------------------------+
| bolt+ssc               | :ref:`bolt-driver-ref` with encryption (accepts self signed certificates).                                                            |
+------------------------+---------------------------------------------------------------------------------------------------------------------------------------+
| bolt+s                 | :ref:`bolt-driver-ref` with encryption (accepts only certificates signed by a certificate authority), full certificate checks.        |
+------------------------+---------------------------------------------------------------------------------------------------------------------------------------+
| neo4j                  | :ref:`neo4j-driver-ref` with no encryption.                                                                                           |
+------------------------+---------------------------------------------------------------------------------------------------------------------------------------+
| neo4j+ssc              | :ref:`neo4j-driver-ref` with encryption (accepts self signed certificates).                                                           |
+------------------------+---------------------------------------------------------------------------------------------------------------------------------------+
| neo4j+s                | :ref:`neo4j-driver-ref` with encryption (accepts only certificates signed by a certificate authority), full certificate checks.       |
+------------------------+---------------------------------------------------------------------------------------------------------------------------------------+

.. note::

    See https://neo4j.com/docs/operations-manual/current/configuration/ports/ for Neo4j ports.


.. _auth-ref:

Auth
====

To authenticate with Neo4j the authentication details are supplied at driver creation.

The auth token is an object of the class :class:`neo4j.Auth` containing static details or :class:`neo4j.auth_management.AuthManager` object.

.. autoclass:: neo4j.Auth

.. autoclass:: neo4j.auth_management.AuthManager
    :members:

.. autoclass:: neo4j.auth_management.AuthManagers
    :members:

.. autoclass:: neo4j.auth_management.ExpiringAuth


Example:

.. code-block:: python

    import neo4j


    auth = neo4j.Auth("basic", "neo4j", "password")


Auth Token Helper Functions
---------------------------

Alternatively, one of the auth token helper functions can be used.

.. autofunction:: neo4j.basic_auth

.. autofunction:: neo4j.kerberos_auth

.. autofunction:: neo4j.bearer_auth

.. autofunction:: neo4j.custom_auth


******
Driver
******

Every Neo4j-backed application will require a driver object.

This object holds the details required to establish connections with a Neo4j database, including server URIs, credentials and other configuration.
:class:`neo4j.Driver` objects hold a connection pool from which :class:`neo4j.Session` objects can borrow connections.
Closing a driver will immediately shut down all connections in the pool.

.. note::
    Driver objects only open connections and pool them as needed. To verify that
    the driver is able to communicate with the database without executing any
    query, use :meth:`neo4j.Driver.verify_connectivity`.

.. autoclass:: neo4j.Driver()
    :members: session, execute_query_bookmark_manager, encrypted, close,
              verify_connectivity, get_server_info, verify_authentication,
              supports_session_auth, supports_multi_db

    .. method:: execute_query(query, parameters_=None,routing_=neo4j.RoutingControl.WRITE, database_=None, impersonated_user_=None, bookmark_manager_=self.execute_query_bookmark_manager, result_transformer_=Result.to_eager_result, **kwargs)

        Execute a query in a transaction function and return all results.

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
                """Get the name of all 42 year-olds."""
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
                """Call all young people "My dear" and get their count."""
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
        :type query_: typing.Optional[str]
        :param parameters_: parameters to use in the query
        :type parameters_: typing.Optional[typing.Dict[str, typing.Any]]
        :param routing_:
            whether to route the query to a reader (follower/read replica) or
            a writer (leader) in the cluster. Default is to route to a writer.
        :type routing_: neo4j.RoutingControl
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

            **This is a preview** (see :ref:`filter-warnings-ref`).
            It might be changed without following the deprecation policy.
            See also
            https://github.com/neo4j/neo4j-python-driver/wiki/preview-features

            See also the Session config :ref:`session-auth-ref`.
        :type auth_:
            typing.Union[typing.Tuple[typing.Any, typing.Any], neo4j.Auth, None]
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
            typing.Callable[[neo4j.Result], typing.Union[T]]
        :param bookmark_manager_:
            Specify a bookmark manager to use.

            If present, the bookmark manager is used to keep the query causally
            consistent with all work executed using the same bookmark manager.

            Defaults to the driver's :attr:`.execute_query_bookmark_manager`.

            Pass :data:`None` to disable causal consistency.
        :type bookmark_manager_:
            typing.Union[BookmarkManager, BookmarkManager, None]
        :param kwargs: additional keyword parameters. None of these can end
            with a single underscore. This is to avoid collisions with the
            keyword configuration parameters of this method. If you need to
            pass such a parameter, use the ``parameters_`` parameter instead.
            Parameters passed as kwargs take precedence over those passed in
            ``parameters_``.
        :type kwargs: typing.Any

        :returns: the result of the ``result_transformer``
        :rtype: T

        **This is experimental** (see :ref:`filter-warnings-ref`).
        It might be changed or removed any time even without prior notice.

        We are looking for feedback on this feature. Please let us know what
        you think about it here:
        https://github.com/neo4j/neo4j-python-driver/discussions/896

        .. versionadded:: 5.5

        .. versionchanged:: 5.8
            * Added the ``auth_`` parameter.
            * Stabilized from experimental.


.. _driver-configuration-ref:

Driver Configuration
====================

Additional configuration can be provided via the :class:`neo4j.Driver` constructor.

+ :ref:`connection-acquisition-timeout-ref`
+ :ref:`connection-timeout-ref`
+ :ref:`encrypted-ref`
+ :ref:`keep-alive-ref`
+ :ref:`max-connection-lifetime-ref`
+ :ref:`max-connection-pool-size-ref`
+ :ref:`max-transaction-retry-time-ref`
+ :ref:`resolver-ref`
+ :ref:`trust-ref`
+ :ref:`ssl-context-ref`
+ :ref:`trusted-certificates-ref`
+ :ref:`user-agent-ref`
+ :ref:`driver-notifications-min-severity-ref`
+ :ref:`driver-notifications-disabled-categories-ref`


.. _connection-acquisition-timeout-ref:

``connection_acquisition_timeout``
----------------------------------
The maximum amount of time in seconds the driver will wait to either acquire an
idle connection from the pool (including potential liveness checks) or create a
new connection when the pool is not full and all existing connection are in use.

Since this process may involve opening a new connection including handshakes,
it should be chosen larger than :ref:`connection-timeout-ref`.

:Type: ``float``
:Default: ``60.0``


.. _connection-timeout-ref:

``connection_timeout``
----------------------
The maximum amount of time in seconds to wait for a TCP connection to be
established.

This *does not* include any handshake(s), or authentication required before the
connection can be used to perform database related work.

:Type: ``float``
:Default: ``30.0``


.. _encrypted-ref:

``encrypted``
-------------
Specify whether to use an encrypted connection between the driver and server.

This setting is only available for URI schemes ``bolt://`` and ``neo4j://`` (:ref:`uri-ref`).

This setting does not have any effect if a custom ``ssl_context`` is configured.

:Type: ``bool``
:Default: ``False``


.. _keep-alive-ref:

``keep_alive``
--------------
Specify whether TCP keep-alive should be enabled.

:Type: ``bool``
:Default: ``True``

**This is experimental** (see :ref:`filter-warnings-ref`).
It might be changed or removed any time even without prior notice.


.. _max-connection-lifetime-ref:

``max_connection_lifetime``
---------------------------
The maximum duration in seconds that the driver will keep a connection for before being removed from the pool.

:Type: ``float``
:Default: ``3600``


.. _max-connection-pool-size-ref:

``max_connection_pool_size``
----------------------------
The maximum total number of connections allowed, per host (i.e. cluster nodes), to be managed by the connection pool.

:Type: ``int``
:Default: ``100``


.. _max-transaction-retry-time-ref:

``max_transaction_retry_time``
------------------------------
 The maximum amount of time in seconds that a managed transaction will retry before failing.

:Type: ``float``
:Default: ``30.0``


.. _resolver-ref:

``resolver``
------------
A custom resolver function to resolve any addresses the driver receives ahead of DNS resolution.
This function is called with an :class:`.Address` and should return an iterable of :class:`.Address` objects or values that can be used to construct :class:`.Address` objects.

If no custom resolver function is supplied, the internal resolver moves straight to regular DNS resolution.

For example:

.. code-block:: python

   import neo4j


    def custom_resolver(socket_address):
        # assert isinstance(socket_address, neo4j.Address)
        if socket_address != ("example.com", 9999):
            raise OSError(f"Unexpected socket address {socket_address!r}")

        # You can return any neo4j.Address object
        yield neo4j.Address(("localhost", 7687))  # IPv4
        yield neo4j.Address(("::1", 7687, 0, 0))  # IPv6
        yield neo4j.Address.parse("localhost:7687")
        yield neo4j.Address.parse("[::1]:7687")

        # or any tuple that can be passed to neo4j.Address(...).
        # Initially, this will be interpreted as IPv4, but DNS resolution
        # will turn it into IPv6 if appropriate.
        yield "::1", 7687
        # This will be interpreted as IPv6 directly, but DNS resolution will
        # still happen.
        yield "::1", 7687, 0, 0
        yield "127.0.0.1", 7687


   driver = neo4j.GraphDatabase.driver("neo4j://example.com:9999",
                                       auth=("neo4j", "password"),
                                       resolver=custom_resolver)


:Default: :data:`None`


.. _trust-ref:

``trust``
---------
Specify how to determine the authenticity of encryption certificates provided by the Neo4j instance on connection.

This setting is only available for URI schemes ``bolt://`` and ``neo4j://`` (:ref:`uri-ref`).

This setting does not have any effect if ``encrypted`` is set to ``False``.

:Type: ``neo4j.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES``, ``neo4j.TRUST_ALL_CERTIFICATES``

.. py:attribute:: neo4j.TRUST_ALL_CERTIFICATES

   Trust any server certificate (default). This ensures that communication
   is encrypted but does not verify the server certificate against a
   certificate authority. This option is primarily intended for use with
   the default auto-generated server certificate.

.. py:attribute:: neo4j.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES

   Trust server certificates that can be verified against the system
   certificate authority. This option is primarily intended for use with
   full certificates.

:Default: ``neo4j.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES``.

.. deprecated:: 5.0
    This configuration option is deprecated and will be removed in a future
    release. Please use :ref:`trusted-certificates-ref` instead.


.. _ssl-context-ref:

``ssl_context``
---------------
Specify a custom SSL context to use for wrapping connections.

This setting is only available for URI schemes ``bolt://`` and ``neo4j://`` (:ref:`uri-ref`).

If given, ``encrypted`` and ``trusted_certificates`` have no effect.

.. warning::
    This option may compromise your application's security if used improperly.

    Its usage is strongly discouraged and comes without any guarantees.

:Type: :class:`ssl.SSLContext` or :data:`None`
:Default: :data:`None`

.. versionadded:: 5.0


.. _trusted-certificates-ref:

``trusted_certificates``
------------------------
Specify how to determine the authenticity of encryption certificates provided by the Neo4j instance on connection.

This setting is only available for URI schemes ``bolt://`` and ``neo4j://`` (:ref:`uri-ref`).

This setting does not have any effect if ``encrypted`` is set to ``False`` or a
custom ``ssl_context`` is configured.

:Type: :class:`.TrustSystemCAs`, :class:`.TrustAll`, or :class:`.TrustCustomCAs`
:Default: :const:`neo4j.TrustSystemCAs()`

.. autoclass:: neo4j.TrustSystemCAs

.. autoclass:: neo4j.TrustAll

.. autoclass:: neo4j.TrustCustomCAs

.. versionadded:: 5.0


.. _user-agent-ref:

``user_agent``
--------------
Specify the client agent name.

:Type: ``str``
:Default: *The Python Driver will generate a user agent name.*


.. _driver-notifications-min-severity-ref:

``notifications_min_severity``
------------------------------
Set the minimum severity for notifications the server should send to the client.

Notifications are available via :attr:`.ResultSummary.notifications` and :attr:`.ResultSummary.summary_notifications`.

:data:`None` will apply the server's default setting.

.. Note::
    If configured, the server or all servers of the cluster need to support notifications filtering.
    Otherwise, the driver will raise a :exc:`.ConfigurationError` as soon as it encounters a server that does not.

:Type: :data:`None`, :class:`.NotificationMinimumSeverity`, or :class:`str`
:Default: :data:`None`

.. versionadded:: 5.7

.. seealso:: :class:`.NotificationMinimumSeverity`, session config :ref:`session-notifications-min-severity-ref`


.. _driver-notifications-disabled-categories-ref:

``notifications_disabled_categories``
-------------------------------------
Set categories of notifications the server should not send to the client.

Notifications are available via :attr:`.ResultSummary.notifications` and :attr:`.ResultSummary.summary_notifications`.

:data:`None` will apply the server's default setting.

.. Note::
    If configured, the server or all servers of the cluster need to support notifications filtering.
    Otherwise, the driver will raise a :exc:`.ConfigurationError` as soon as it encounters a server that does not.

:Type: :data:`None`, :term:`iterable` of :class:`.NotificationDisabledCategory` and/or :class:`str`
:Default: :data:`None`

.. versionadded:: 5.7

.. seealso:: :class:`.NotificationDisabledCategory`, session config :ref:`session-notifications-disabled-categories-ref`


Driver Object Lifetime
======================

For general applications, it is recommended to create one top-level :class:`neo4j.Driver` object that lives for the lifetime of the application.

For example:

.. code-block:: python

    from neo4j import GraphDatabase


    class Application:

        def __init__(self, uri, user, password)
            self.driver = GraphDatabase.driver(uri, auth=(user, password))

        def close(self):
            self.driver.close()

Connection details held by the :class:`neo4j.Driver` are immutable.
Therefore if, for example, a password is changed, a replacement :class:`neo4j.Driver` object must be created.
More than one :class:`.Driver` may be required if connections to multiple remotes, or connections as multiple users, are required,
unless when using impersonation (:ref:`impersonated-user-ref`).

:class:`neo4j.Driver` objects are thread-safe but cannot be shared across processes.
Therefore, ``multithreading`` should generally be preferred over ``multiprocessing`` for parallel database access.
If using ``multiprocessing`` however, each process will require its own :class:`neo4j.Driver` object.


.. _bolt-driver-ref:

BoltDriver
==========

URI schemes:
    ``bolt``, ``bolt+ssc``, ``bolt+s``

Will result in:

.. autoclass:: neo4j.BoltDriver


.. _neo4j-driver-ref:

Neo4jDriver
===========

URI schemes:
    ``neo4j``, ``neo4j+ssc``, ``neo4j+s``

Will result in:

.. autoclass:: neo4j.Neo4jDriver


***********************
Sessions & Transactions
***********************
All database activity is co-ordinated through two mechanisms:
**sessions** (:class:`neo4j.Session`) and **transactions**
(:class:`neo4j.Transaction`, :class:`neo4j.ManagedTransaction`).

A **session** is a logical container for any number of causally-related transactional units of work.
Sessions automatically provide guarantees of causal consistency within a clustered environment but multiple sessions can also be causally chained if required.
Sessions provide the top level of containment for database activity.
Session creation is a lightweight operation and *sessions are not thread safe*.

Connections are drawn from the :class:`neo4j.Driver` connection pool as required.

A **transaction** is a unit of work that is either committed in its entirety or is rolled back on failure.


.. _session-construction-ref:

********************
Session Construction
********************

To construct a :class:`neo4j.Session` use the :meth:`neo4j.Driver.session` method.

.. code-block:: python

    from neo4j import GraphDatabase


    with GraphDatabase(uri, auth=(user, password)) as driver:
        session = driver.session()
        try:
            result = session.run("MATCH (a:Person) RETURN a.name AS name")
            names = [record["name"] for record in result]
        finally:
            session.close()


Sessions will often be created and destroyed using a *with block context*.
This is the recommended approach as it takes care of closing the session
properly even when an exception is raised.

.. code-block:: python

    with driver.session() as session:
        result = session.run("MATCH (a:Person) RETURN a.name AS name")
        ...  # do something with the result


Sessions will often be created with some configuration settings, see :ref:`session-configuration-ref`.

.. code-block:: python

    with driver.session(database="example_database", fetch_size=100) as session:
        result = session.run("MATCH (a:Person) RETURN a.name AS name")
        ...  # do something with the result


*******
Session
*******

.. autoclass:: neo4j.Session()

    .. automethod:: close

    .. automethod:: closed

    .. automethod:: run

    .. automethod:: last_bookmarks

    .. automethod:: last_bookmark

    .. automethod:: begin_transaction

    .. automethod:: read_transaction

    .. automethod:: execute_read

    .. automethod:: write_transaction

    .. automethod:: execute_write



Query
=====

.. autoclass:: neo4j.Query



.. _session-configuration-ref:

Session Configuration
=====================

To construct a :class:`neo4j.Session` use the :meth:`neo4j.Driver.session` method. This section describes the session configuration key-word arguments.


+ :ref:`bookmarks-ref`
+ :ref:`database-ref`
+ :ref:`default-access-mode-ref`
+ :ref:`fetch-size-ref`
+ :ref:`bookmark-manager-ref`
+ :ref:`session-auth-ref`
+ :ref:`session-notifications-min-severity-ref`
+ :ref:`session-notifications-disabled-categories-ref`


.. _bookmarks-ref:

``bookmarks``
-------------
Optional :class:`neo4j.Bookmarks`. Use this to causally chain sessions.
See :meth:`Session.last_bookmarks` or :meth:`AsyncSession.last_bookmarks` for
more information.

:Default: :data:`None`

.. deprecated:: 5.0
    Alternatively, an iterable of strings can be passed. This usage is
    deprecated and will be removed in a future release. Please use a
    :class:`neo4j.Bookmarks` object instead.


.. _database-ref:

``database``
------------
Name of the database to query.

.. Note::

    The default database can be set on the Neo4j instance settings.

.. Note::

    This option has no explicit value by default, but it is recommended to set
    one if the target database is known in advance. This has the benefit of
    ensuring a consistent target database name throughout the session in a
    straightforward way and potentially simplifies driver logic as well as
    reduces network communication resulting in better performance.

    Usage of Cypher clauses like `USE` is not a replacement for this option.
    The driver does not parse any Cypher.

When no explicit name is set, the driver behavior depends on the connection
URI scheme supplied to the driver on instantiation and Bolt protocol
version.

Specifically, the following applies:

- **bolt schemes** - queries are dispatched to the server for execution
  without explicit database name supplied, meaning that the target database
  name for query execution is determined by the server. It is important to
  note that the target database may change (even within the same session),
  for instance if the user's home database is changed on the server.

- **neo4j schemes** - providing that Bolt protocol version 4.4, which was
  introduced with Neo4j server 4.4, or above is available, the driver
  fetches the user's home database name from the server on first query
  execution within the session and uses the fetched database name
  explicitly for all queries executed within the session. This ensures that
  the database name remains consistent within the given session. For
  instance, if the user's home database name is 'movies' and the server
  supplies it to the driver upon database name fetching for the session,
  all queries within that session are executed with the explicit database
  name 'movies' supplied. Any change to the user’s home database is
  reflected only in sessions created after such change takes effect. This
  behavior requires additional network communication. In clustered
  environments, it is strongly recommended to avoid a single point of
  failure. For instance, by ensuring that the connection URI resolves to
  multiple endpoints. For older Bolt protocol versions the behavior is the
  same as described for the **bolt schemes** above.


.. code-block:: python

    from neo4j import GraphDatabase


    # closing of driver and session is omitted for brevity
    driver = GraphDatabase.driver(uri, auth=(user, password))
    session = driver.session(database="system")

.. py:attribute:: neo4j.DEFAULT_DATABASE = None
    :noindex:

    This will use the default database on the Neo4j instance.

:Type: ``str``, ``neo4j.DEFAULT_DATABASE``

:Default: ``neo4j.DEFAULT_DATABASE``


.. _impersonated-user-ref:

``impersonated_user``
---------------------
Name of the user to impersonate.
This means that all actions in the session will be executed in the security
context of the impersonated user. For this, the user for which the
:class:`Driver` has been created needs to have the appropriate permissions.

.. Note::

    If configured, the server or all servers of the cluster need to support impersonation.
    Otherwise, the driver will raise :exc:`.ConfigurationError`
    as soon as it encounters a server that does not.

.. code-block:: python

    from neo4j import GraphDatabase


    # closing of driver and session is omitted for brevity
    driver = GraphDatabase.driver(uri, auth=(user, password))
    session = driver.session(impersonated_user="alice")

.. py:data:: None
   :noindex:

   Will not perform impersonation.

:Type: ``str``, None

:Default: :data:`None`


.. _default-access-mode-ref:

``default_access_mode``
-----------------------
The default access mode.

A session can be given a default access mode on construction.

This applies only in clustered environments and determines whether transactions
carried out within that session should be routed to a ``read`` or ``write``
server by default.

Transactions (see :ref:`managed-transactions-ref`) within a session override the
access mode passed to that session on construction.

.. note::
    The driver does not parse Cypher queries and cannot determine whether the
    access mode should be ``neo4j.WRITE_ACCESS`` or ``neo4j.READ_ACCESS``.
    This setting is only meant to enable the driver to perform correct routing,
    *not* for enforcing access control. This means that, depending on the server
    version and settings, the server or cluster might allow a write-statement to
    be executed even when ``neo4j.READ_ACCESS`` is chosen. This behaviour should
    not be relied upon as it can change with the server.

.. py:attribute:: neo4j.WRITE_ACCESS = "WRITE"
    :noindex:
.. py:attribute:: neo4j.READ_ACCESS = "READ"
    :noindex:

:Type: ``neo4j.WRITE_ACCESS``, ``neo4j.READ_ACCESS``

:Default: ``neo4j.WRITE_ACCESS``


.. _fetch-size-ref:

``fetch_size``
--------------
The fetch size used for requesting records from Neo4j.

:Type: ``int``
:Default: ``1000``


.. _bookmark-manager-ref:

``bookmark_manager``
--------------------
Specify a bookmark manager for the session to use. If present, the bookmark
manager is used to keep all work within the session causally consistent with
all work in other sessions using the same bookmark manager.

See :class:`.BookmarkManager` for more information.

.. warning::
    Enabling the BookmarkManager can have a negative impact on performance since
    all queries will wait for the latest changes to be propagated across the
    cluster.

    For simple use-cases, it often suffices that work within a single session
    is automatically causally consistent.

:Type: :data:`None` or :class:`.BookmarkManager`
:Default: :data:`None`

.. versionadded:: 5.0

.. versionchanged:: 5.8 stabilized from experimental


.. _session-auth-ref:

``auth``
--------
Optional :class:`neo4j.Auth` or ``(user, password)``-tuple. Use this overwrite the
authentication information for the session (user-switching).
This requires the server to support re-authentication on the protocol level. You can
check this by calling :meth:`.Driver.supports_session_auth` / :meth:`.AsyncDriver.supports_session_auth`.

It is not possible to overwrite the authentication information for the session with no authentication,
i.e., downgrade the authentication at session level.
Instead, you should create a driver with no authentication and upgrade the authentication at session level as needed.

**This is a preview** (see :ref:`filter-warnings-ref`).
It might be changed without following the deprecation policy.
See also https://github.com/neo4j/neo4j-python-driver/wiki/preview-features

:Type: :data:`None`, :class:`.Auth` or ``(user, password)``-tuple
:Default: :data:`None` - use the authentication information provided during driver creation.

.. versionadded:: 5.8


.. _session-notifications-min-severity-ref:

``notifications_min_severity``
------------------------------
Set the minimum severity for notifications the server should send to the client.

Notifications are available via :attr:`.ResultSummary.notifications` and :attr:`.ResultSummary.summary_notifications`.

:data:`None` will apply the driver's configuration setting (:ref:`driver-notifications-min-severity-ref`).

.. Note::
    If configured, the server or all servers of the cluster need to support notifications filtering.
    Otherwise, the driver will raise a :exc:`.ConfigurationError` as soon as it encounters a server that does not.

:Type: :data:`None`, :class:`.NotificationMinimumSeverity`, or :class:`str`
:Default: :data:`None`

.. versionadded:: 5.7

.. seealso:: :class:`.NotificationMinimumSeverity`


.. _session-notifications-disabled-categories-ref:

``notifications_disabled_categories``
-------------------------------------
Set categories of notifications the server should not send to the client.

Notifications are available via :attr:`.ResultSummary.notifications` and :attr:`.ResultSummary.summary_notifications`.

:data:`None` will apply the driver's configuration setting (:ref:`driver-notifications-min-severity-ref`).

.. Note::
    If configured, the server or all servers of the cluster need to support notifications filtering.
    Otherwise, the driver will raise a :exc:`.ConfigurationError` as soon as it encounters a server that does not.

:Type: :data:`None`, :term:`iterable` of :class:`.NotificationDisabledCategory` and/or :class:`str`
:Default: :data:`None`

.. versionadded:: 5.7

.. seealso:: :class:`.NotificationDisabledCategory`



***********
Transaction
***********

Neo4j supports three kinds of transaction:

+ :ref:`auto-commit-transactions-ref`
+ :ref:`explicit-transactions-ref`
+ :ref:`managed-transactions-ref`

Each has pros and cons but if in doubt, use a managed transaction with a *transaction function*.


.. _auto-commit-transactions-ref:

Auto-commit Transactions
========================
Auto-commit transactions are the simplest form of transaction, available via
:meth:`neo4j.Session.run`. These are easy to use but support only one
statement per transaction and are not automatically retried on failure.

Auto-commit transactions are also the only way to run ``PERIODIC COMMIT``
(only Neo4j 4.4 and earlier) or ``CALL {...} IN TRANSACTIONS`` (Neo4j 4.4 and
newer) statements, since those Cypher clauses manage their own transactions
internally.

Write example:

.. code-block:: python

    import neo4j


    def create_person(driver, name):
        # default_access_mode defaults to WRITE_ACCESS
        with driver.session(database="neo4j") as session:
            query = ("CREATE (n:NodeExample {name: $name, id: randomUUID()}) "
                     "RETURN n.id AS node_id")
            result = session.run(query, name=name)
            record = result.single()
            return record["node_id"]

Read example:

.. code-block:: python

    import neo4j


    def get_numbers(driver):
        numbers = []
        with driver.session(database="neo4j",
                            default_access_mode=neo4j.READ_ACCESS) as session:
            result = session.run("UNWIND [1, 2, 3] AS x RETURN x")
            for record in result:
                numbers.append(record["x"])
        return numbers


.. _explicit-transactions-ref:

Explicit Transactions (Unmanaged Transactions)
==============================================
Explicit transactions support multiple statements and must be created with an explicit :meth:`neo4j.Session.begin_transaction` call.

This creates a new :class:`neo4j.Transaction` object that can be used to run Cypher.

It also gives applications the ability to directly control ``commit`` and ``rollback`` activity.

.. autoclass:: neo4j.Transaction()

    .. automethod:: run

    .. automethod:: commit

    .. automethod:: rollback

    .. automethod:: close

    .. automethod:: closed

Closing an explicit transaction can either happen automatically at the end of a ``with`` block,
or can be explicitly controlled through the :meth:`neo4j.Transaction.commit`, :meth:`neo4j.Transaction.rollback` or :meth:`neo4j.Transaction.close` methods.

Explicit transactions are most useful for applications that need to distribute Cypher execution across multiple functions for the same transaction or that need to run multiple queries within a single transaction but without the retries provided by managed transactions.

Example:

.. code-block:: python

    import neo4j


    def transfer_to_other_bank(driver, customer_id, other_bank_id, amount):
        with driver.session(
            database="neo4j",
            # optional, defaults to WRITE_ACCESS
            default_access_mode=neo4j.WRITE_ACCESS
        ) as session:
            tx = session.begin_transaction()
            # or just use a `with` context instead of try/finally
            try:
                if not customer_balance_check(tx, customer_id, amount):
                    # give up
                    return
                other_bank_transfer_api(customer_id, other_bank_id, amount)
                # Now the money has been transferred
                # => we can't retry or rollback anymore
                try:
                    decrease_customer_balance(tx, customer_id, amount)
                    tx.commit()
                except Exception as e:
                    request_inspection(customer_id, other_bank_id, amount, e)
                    raise
            finally:
                tx.close()  # rolls back if not yet committed


    def customer_balance_check(tx, customer_id, amount):
        query = ("MATCH (c:Customer {id: $id}) "
                 "RETURN c.balance >= $amount AS sufficient")
        result = tx.run(query, id=customer_id, amount=amount)
        record = result.single(strict=True)
        return record["sufficient"]


    def other_bank_transfer_api(customer_id, other_bank_id, amount):
        ...  # make some API call to other bank


    def decrease_customer_balance(tx, customer_id, amount):
        query = ("MATCH (c:Customer {id: $id}) "
                 "SET c.balance = c.balance - $amount")
        result = tx.run(query, id=customer_id, amount=amount)
        result.consume()


    def request_inspection(customer_id, other_bank_id, amount, e):
        # manual cleanup required; log this or similar
        print("WARNING: transaction rolled back due to exception:", repr(e))
        print("customer_id:", customer_id, "other_bank_id:", other_bank_id,
              "amount:", amount)

.. _managed-transactions-ref:

Managed Transactions (`transaction functions`)
==============================================
Transaction functions are the most powerful form of transaction, providing access mode override and retry capabilities.

+ :meth:`neo4j.Session.execute_write`
+ :meth:`neo4j.Session.execute_read`

These allow a function object representing the transactional unit of work to be passed as a parameter.
This function is called one or more times, within a configurable time limit, until it succeeds.
Results should be fully consumed within the function and only aggregate or status values should be returned.
Returning a live result object would prevent the driver from correctly managing connections and would break retry guarantees.

The passed function will receive a :class:`neo4j.ManagedTransaction` object as its first parameter. For more details see :meth:`neo4j.Session.execute_write` and :meth:`neo4j.Session.execute_read`.

.. autoclass:: neo4j.ManagedTransaction()

    .. automethod:: run

Example:

.. code-block:: python

    def create_person(driver, name)
        with driver.session() as session:
            node_id = session.execute_write(create_person_tx, name)


    def create_person_tx(tx, name):
        query = ("CREATE (a:Person {name: $name, id: randomUUID()}) "
                 "RETURN a.id AS node_id")
        result = tx.run(query, name=name)
        record = result.single()
        return record["node_id"]

To exert more control over how a transaction function is carried out, the :func:`neo4j.unit_of_work` decorator can be used.

.. autofunction:: neo4j.unit_of_work


******
Result
******

Every time a query is executed, a :class:`neo4j.Result` is returned.

This provides a handle to the result of the query, giving access to the records within it as well as the result metadata.

Results also contain a buffer that automatically stores unconsumed records when results are consumed out of order.

A :class:`neo4j.Result` is attached to an active connection, through a :class:`neo4j.Session`, until all its content has been buffered or consumed.

.. autoclass:: neo4j.Result()

    .. describe:: iter(result)

    .. describe:: next(result)

    .. automethod:: keys

    .. automethod:: consume

    .. automethod:: single

    .. automethod:: fetch

    .. automethod:: peek

    .. automethod:: graph

    .. automethod:: value

    .. automethod:: values

    .. automethod:: data

    .. automethod:: to_df

    .. automethod:: to_eager_result

    .. automethod:: closed

See https://neo4j.com/docs/python-manual/current/cypher-workflow/#python-driver-type-mapping for more about type mapping.


***********
EagerResult
***********

.. autoclass:: neo4j.EagerResult
    :show-inheritance:
    :members:


Graph
=====

.. autoclass:: neo4j.graph.Graph()

    .. autoattribute:: nodes

    .. autoattribute:: relationships

    .. automethod:: relationship_type

**This is experimental** (see :ref:`filter-warnings-ref`).
It might be changed or removed any time even without prior notice.


******
Record
******

.. autoclass:: neo4j.Record()

    .. describe:: Record(iterable)

        Create a new record based on an dictionary-like iterable.
        This can be a dictionary itself, or may be a sequence of key-value pairs, each represented by a tuple.

    .. describe:: record == other

        Compare a record for equality with another value.
        The ``other`` value may be any ``Sequence`` or ``Mapping`` or both.
        If comparing with a ``Sequence`` the values are compared in order.
        If comparing with a ``Mapping`` the values are compared based on their keys.
        If comparing with a value that exhibits both traits, both comparisons must be true for the values to be considered equal.

    .. describe:: record != other

        Compare a record for inequality with another value.
        See above for comparison rules.

    .. describe:: hash(record)

        Create a hash for this record.
        This will raise a :exc:`TypeError` if any values within the record are unhashable.

    .. describe:: record[index]

        Obtain a value from the record by index.
        This will raise an :exc:`IndexError` if the specified index is out of range.

    .. describe:: record[i:j]

        Derive a sub-record based on a start and end index.
        All keys and values within those bounds will be copied across in the same order as in the original record.

    .. automethod:: keys

    .. describe:: record[key]

        Obtain a value from the record by key.
        This will raise a :exc:`KeyError` if the specified key does not exist.

    .. automethod:: get(key, default=None)

    .. automethod:: index(key)

    .. automethod:: items

    .. automethod:: value(key=0, default=None)

    .. automethod:: values

    .. automethod:: data



*************
ResultSummary
*************

.. autoclass:: neo4j.ResultSummary()
   :members:

SummaryCounters
===============

.. autoclass:: neo4j.SummaryCounters()
    :members:


ServerInfo
==========

.. autoclass:: neo4j.ServerInfo()
   :members:


SummaryNotification
===================

.. autoclass:: neo4j.SummaryNotification()
    :members:


NotificationSeverity
--------------------

.. autoclass:: neo4j.NotificationSeverity()
    :members:


NotificationCategory
--------------------

.. autoclass:: neo4j.NotificationCategory()
    :members:


SummaryNotificationPosition
---------------------------

.. autoclass:: neo4j.SummaryNotificationPosition()
    :members:



***************
Core Data Types
***************

Cypher supports a set of core data types that all map to built-in types in Python.

These include the common ``Boolean`` ``Integer`` ``Float`` and ``String`` types as well as ``List`` and ``Map`` that can hold heterogenous collections of any other type.

The core types with their general mappings are listed below:

+------------------------+---------------------------------------------------------------------------------------------------------------------------+
| Cypher Type            | Python Type                                                                                                               |
+========================+===========================================================================================================================+
| Null                   | :data:`None`                                                                                                              |
+------------------------+---------------------------------------------------------------------------------------------------------------------------+
| Boolean                | :class:`bool`                                                                                                             |
+------------------------+---------------------------------------------------------------------------------------------------------------------------+
| Integer                | :class:`int`                                                                                                              |
+------------------------+---------------------------------------------------------------------------------------------------------------------------+
| Float                  | :class:`float`                                                                                                            |
+------------------------+---------------------------------------------------------------------------------------------------------------------------+
| String                 | :class:`str`                                                                                                              |
+------------------------+---------------------------------------------------------------------------------------------------------------------------+
| Bytes :sup:`[1]`       | :class:`bytes`                                                                                                            |
+------------------------+---------------------------------------------------------------------------------------------------------------------------+
| List                   | :class:`list`                                                                                                             |
+------------------------+---------------------------------------------------------------------------------------------------------------------------+
| Map                    | :class:`dict`                                                                                                             |
+------------------------+---------------------------------------------------------------------------------------------------------------------------+

.. Note::

   1. ``Bytes`` is not an actual Cypher type but is transparently passed through when used in parameters or query results.


In reality, the actual conversions and coercions that occur as values are passed through the system are more complex than just a simple mapping.
The diagram below illustrates the actual mappings between the various layers, from driver to data store, for the core types.

.. image:: ./_images/core_type_mappings.svg
    :target: ./_images/core_type_mappings.svg


Extended Data Types
===================

The driver supports serializing more types (as parameters in).
However, they will have to be mapped to the existing Bolt types (see above) when they are sent to the server.
This means, the driver will never return these types in results.

When in doubt, you can test the type conversion like so::

    import neo4j


    with neo4j.GraphDatabase.driver(URI, auth=AUTH) as driver:
        with driver.session() as session:
            type_in = ("foo", "bar")
            result = session.run("RETURN $x", x=type_in)
            type_out = result.single()[0]
            print(type(type_out))
            print(type_out)

Which in this case would yield::

    <class 'list'>
    ['foo', 'bar']


+-----------------------------------+---------------------------------+---------------------------------------+
| Parameter Type                    | Bolt Type                       | Result Type                           |
+===================================+=================================+=======================================+
| :class:`tuple`                    | List                            | :class:`list`                         |
+-----------------------------------+---------------------------------+---------------------------------------+
| :class:`bytearray`                | Bytes                           | :class:`bytes`                        |
+-----------------------------------+---------------------------------+---------------------------------------+
| numpy\ :sup:`[2]` ``ndarray``     | (nested) List                   | (nested) :class:`list`                |
+-----------------------------------+---------------------------------+---------------------------------------+
| pandas\ :sup:`[3]` ``DataFrame``  | Map[str, List[_]] :sup:`[4]`    | :class:`dict`                         |
+-----------------------------------+---------------------------------+---------------------------------------+
| pandas ``Series``                 | List                            | :class:`list`                         |
+-----------------------------------+---------------------------------+---------------------------------------+
| pandas ``Array``                  | List                            | :class:`list`                         |
+-----------------------------------+---------------------------------+---------------------------------------+

.. Note::

   2. ``void`` and ``complexfloating`` typed numpy ``ndarray``\s are not supported.
   3. ``Period``, ``Interval``, and ``pyarrow`` pandas types are not supported.
   4. A pandas ``DataFrame`` will be serialized as Map with the column names mapping to the column values (as Lists).
       Just like with ``dict`` objects, the column names need to be :class:`str` objects.



****************
Graph Data Types
****************

Cypher queries can return entire graph structures as well as individual property values.

The graph data types detailed here model graph data returned from a Cypher query.
Graph values cannot be passed in as parameters as it would be unclear whether the entity was intended to be passed by reference or by value.
The identity or properties of that entity should be passed explicitly instead.

The driver contains a corresponding class for each of the graph types that can be returned.

=============  =================================
Cypher Type    Python Type
=============  =================================
Node           :class:`neo4j.graph.Node`
Relationship   :class:`neo4j.graph.Relationship`
Path           :class:`neo4j.graph.Path`
=============  =================================


Node
====

.. autoclass:: neo4j.graph.Node

    .. describe:: node == other

        Compares nodes for equality.

    .. describe:: node != other

        Compares nodes for inequality.

    .. describe:: hash(node)

        Computes the hash of a node.

    .. describe:: len(node)

        Returns the number of properties on a node.

    .. describe:: iter(node)

        Iterates through all properties on a node.

    .. describe:: node[key]

        Returns a node property by key.
        Raises :exc:`KeyError` if the key does not exist.

    .. describe:: key in node

        Checks whether a property key exists for a given node.

    .. autoproperty:: graph

    .. autoproperty:: id

    .. autoproperty:: element_id

    .. autoproperty:: labels

    .. automethod:: get

    .. automethod:: keys

    .. automethod:: values

    .. automethod:: items


Relationship
============

.. autoclass:: neo4j.graph.Relationship

    .. describe:: relationship == other

        Compares relationships for equality.

    .. describe:: relationship != other

        Compares relationships for inequality.

    .. describe:: hash(relationship)

        Computes the hash of a relationship.

    .. describe:: len(relationship)

        Returns the number of properties on a relationship.

    .. describe:: iter(relationship)

        Iterates through all properties on a relationship.

    .. describe:: relationship[key]

        Returns a relationship property by key.
        Raises :exc:`KeyError` if the key does not exist.

    .. describe:: key in relationship

        Checks whether a property key exists for a given relationship.

    .. describe:: type(relationship)

        Returns the type (class) of a relationship.
        Relationship objects belong to a custom subtype based on the type name in the underlying database.

    .. autoproperty:: graph

    .. autoproperty:: id

    .. autoproperty:: element_id

    .. autoproperty:: nodes

    .. autoproperty:: start_node

    .. autoproperty:: end_node

    .. autoproperty:: type

    .. automethod:: get

    .. automethod:: keys

    .. automethod:: values

    .. automethod:: items



Path
====

.. autoclass:: neo4j.graph.Path

    .. describe:: path == other

        Compares paths for equality.

    .. describe:: path != other

        Compares paths for inequality.

    .. describe:: hash(path)

        Computes the hash of a path.

    .. describe:: len(path)

        Returns the number of relationships in a path.

    .. describe:: iter(path)

        Iterates through all the relationships in a path.

    .. autoproperty:: graph

    .. autoproperty:: nodes

    .. autoproperty:: start_node

    .. autoproperty:: end_node

    .. autoproperty:: relationships


******************
Spatial Data Types
******************

.. include:: types/_spatial_overview.rst

See topic :ref:`spatial-data-types` for more details.


*******************
Temporal Data Types
*******************

.. include:: types/_temporal_overview.rst

See topic :ref:`temporal-data-types` for more details.


***************
BookmarkManager
***************

.. autoclass:: neo4j.api.BookmarkManager
    :members:


*************************
Constants, Enums, Helpers
*************************

.. autoclass:: neo4j.NotificationMinimumSeverity
    :show-inheritance:
    :members:

.. autoclass:: neo4j.NotificationDisabledCategory
    :show-inheritance:
    :members:

.. autoclass:: neo4j.RoutingControl
    :show-inheritance:
    :members:

.. autoclass:: neo4j.Address
    :show-inheritance:
    :members:

.. autoclass:: neo4j.IPv4Address()
    :show-inheritance:

.. autoclass:: neo4j.IPv6Address()
    :show-inheritance:


.. _errors-ref:

******
Errors
******


Neo4j Errors
============

Server-side errors


* :class:`neo4j.exceptions.Neo4jError`

  * :class:`neo4j.exceptions.ClientError`

    * :class:`neo4j.exceptions.CypherSyntaxError`

    * :class:`neo4j.exceptions.CypherTypeError`

    * :class:`neo4j.exceptions.ConstraintError`

    * :class:`neo4j.exceptions.AuthError`

      * :class:`neo4j.exceptions.TokenExpired`

        * :class:`neo4j.exceptions.TokenExpiredRetryable`

    * :class:`neo4j.exceptions.Forbidden`

  * :class:`neo4j.exceptions.DatabaseError`

  * :class:`neo4j.exceptions.TransientError`

    * :class:`neo4j.exceptions.DatabaseUnavailable`

    * :class:`neo4j.exceptions.NotALeader`

    * :class:`neo4j.exceptions.ForbiddenOnReadOnlyDatabase`


.. autoexception:: neo4j.exceptions.Neo4jError()
    :show-inheritance:
    :members: message, code, is_retriable, is_retryable

.. autoexception:: neo4j.exceptions.ClientError()
    :show-inheritance:

.. autoexception:: neo4j.exceptions.CypherSyntaxError()
    :show-inheritance:

.. autoexception:: neo4j.exceptions.CypherTypeError()
    :show-inheritance:

.. autoexception:: neo4j.exceptions.ConstraintError()
    :show-inheritance:

.. autoexception:: neo4j.exceptions.AuthError()
    :show-inheritance:

.. autoexception:: neo4j.exceptions.TokenExpired()
    :show-inheritance:

.. autoexception:: neo4j.exceptions.TokenExpiredRetryable()
    :show-inheritance:

.. autoexception:: neo4j.exceptions.Forbidden()
    :show-inheritance:

.. autoexception:: neo4j.exceptions.DatabaseError()
    :show-inheritance:

.. autoexception:: neo4j.exceptions.TransientError()
    :show-inheritance:

.. autoexception:: neo4j.exceptions.DatabaseUnavailable()
    :show-inheritance:

.. autoexception:: neo4j.exceptions.NotALeader()
    :show-inheritance:

.. autoexception:: neo4j.exceptions.ForbiddenOnReadOnlyDatabase()
    :show-inheritance:




Driver Errors
=============

Client-side errors


* :class:`neo4j.exceptions.DriverError`

  * :class:`neo4j.exceptions.SessionError`

  * :class:`neo4j.exceptions.TransactionError`

    * :class:`neo4j.exceptions.TransactionNestingError`

  * :class:`neo4j.exceptions.ResultError`

    * :class:`neo4j.exceptions.ResultConsumedError`

    * :class:`neo4j.exceptions.ResultNotSingleError`

  * :class:`neo4j.exceptions.BrokenRecordError`

  * :class:`neo4j.exceptions.SessionExpired`

  * :class:`neo4j.exceptions.ServiceUnavailable`

    * :class:`neo4j.exceptions.RoutingServiceUnavailable`

    * :class:`neo4j.exceptions.WriteServiceUnavailable`

    * :class:`neo4j.exceptions.ReadServiceUnavailable`

    * :class:`neo4j.exceptions.IncompleteCommit`

  * :class:`neo4j.exceptions.ConfigurationError`

    * :class:`neo4j.exceptions.AuthConfigurationError`

    * :class:`neo4j.exceptions.CertificateConfigurationError`


.. autoexception:: neo4j.exceptions.DriverError()
    :show-inheritance:
    :members: is_retryable

.. autoexception:: neo4j.exceptions.SessionError()
    :show-inheritance:
    :members: session

.. autoexception:: neo4j.exceptions.TransactionError()
    :show-inheritance:
    :members: transaction

.. autoexception:: neo4j.exceptions.TransactionNestingError()
    :show-inheritance:

.. autoexception:: neo4j.exceptions.ResultError()
    :show-inheritance:
    :members: result

.. autoexception:: neo4j.exceptions.ResultConsumedError()
    :show-inheritance:

.. autoexception:: neo4j.exceptions.ResultNotSingleError()
    :show-inheritance:

.. autoexception:: neo4j.exceptions.BrokenRecordError()
    :show-inheritance:

.. autoexception:: neo4j.exceptions.SessionExpired()
    :show-inheritance:

.. autoexception:: neo4j.exceptions.ServiceUnavailable()
    :show-inheritance:

.. autoexception:: neo4j.exceptions.RoutingServiceUnavailable()
    :show-inheritance:

.. autoexception:: neo4j.exceptions.WriteServiceUnavailable()
    :show-inheritance:

.. autoexception:: neo4j.exceptions.ReadServiceUnavailable()
    :show-inheritance:

.. autoexception:: neo4j.exceptions.IncompleteCommit()
    :show-inheritance:

.. autoexception:: neo4j.exceptions.ConfigurationError()
    :show-inheritance:

.. autoexception:: neo4j.exceptions.AuthConfigurationError()
    :show-inheritance:

.. autoexception:: neo4j.exceptions.CertificateConfigurationError()
    :show-inheritance:



Internal Driver Errors
=======================

If an internal error (BoltError), in particular a protocol error (BoltProtocolError) is surfaced please open an issue on github.

https://github.com/neo4j/neo4j-python-driver/issues

Please provide details about your running environment,

+ Operating System:
+ Python Version:
+ Python Driver Version:
+ Neo4j Version:
+ The code block with a description that produced the error:
+ The error message:


********
Warnings
********

The Python Driver uses the built-in :class:`python:DeprecationWarning` class to warn about deprecations.

The Python Driver uses the built-in :class:`python:ResourceWarning` class to warn about not properly closed resources, e.g., Drivers and Sessions.

.. note::
    Deprecation and resource warnings are not shown by default. One way of enable them is to run the Python interpreter in `development mode`_.

.. _development mode: https://docs.python.org/3/library/devmode.html#devmode


The Python Driver uses the :class:`neo4j.ExperimentalWarning` class to warn about experimental features.

.. autoclass:: neo4j.ExperimentalWarning


.. _filter-warnings-ref:

Filter Warnings
===============

This example shows how to suppress the :class:`neo4j.ExperimentalWarning` using the :func:`python:warnings.filterwarnings` function.

.. code-block:: python

    import warnings
    from neo4j import ExperimentalWarning

    ...

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=ExperimentalWarning)
        ...  # the call emitting the ExperimentalWarning

    ...

This will only mute the :class:`neo4j.ExperimentalWarning` for everything inside
the ``with``-block. This is the preferred way to mute warnings, as warnings
triggerd by new code will still be visible.

However, should you want to mute it for the entire application, use the
following code:

.. code-block:: python

    import warnings
    from neo4j import ExperimentalWarning

    warnings.filterwarnings("ignore", category=ExperimentalWarning)

    ...


.. _logging-ref:

*******
Logging
*******

The driver offers logging for debugging purposes. It is not recommended to
enable logging for anything other than debugging. For instance, if the driver is
not able to connect to the database server or if undesired behavior is observed.

There are different ways of enabling logging as listed below.

.. seealso::
    :ref:`async-logging-ref` for an improved logging experience with the async driver.

Simple Approach
===============

.. autofunction:: neo4j.debug.watch(*logger_names, level=logging.DEBUG, out=sys.stderr, colour=False)

Context Manager
===============

.. autoclass:: neo4j.debug.Watcher(*logger_names, default_level=logging.DEBUG, default_out=sys.stderr, colour=False)
    :members:
    :special-members: __enter__, __exit__

Full Control
============

.. code-block:: python

    import logging
    import sys

    # create a handler, e.g. to log to stdout
    handler = logging.StreamHandler(sys.stdout)
    # configure the handler to your liking
    handler.setFormatter(logging.Formatter(
        "[%(levelname)-8s] %(threadName)s(%(thread)d) %(asctime)s  %(message)s"
    ))
    # add the handler to the driver's logger
    logging.getLogger("neo4j").addHandler(handler)
    # make sure the logger logs on the desired log level
    logging.getLogger("neo4j").setLevel(logging.DEBUG)
    # from now on, DEBUG logging to stdout is enabled in the driver


*********
Bookmarks
*********

.. autoclass:: neo4j.Bookmarks
    :members:
    :special-members: __bool__, __add__, __iter__

.. autoclass:: neo4j.Bookmark
    :members:
