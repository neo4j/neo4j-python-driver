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
   :members: driver


Driver creation example:

.. code-block:: python

    from neo4j import GraphDatabase

    uri = "neo4j://example.com:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))

    driver.close()  # close the driver object


For basic authentication, ``auth`` can be a simple tuple, for example:

.. code-block:: python

   auth = ("neo4j", "password")

This will implicitly create a :class:`neo4j.Auth` with a ``scheme="basic"``.
Other authentication methods are described under :ref:`auth-ref`.


``with`` block context example:

.. code-block:: python

    from neo4j import GraphDatabase

    uri = "neo4j://example.com:7687"
    with GraphDatabase.driver(uri, auth=("neo4j", "password")) as driver:
        # use the driver



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

    uri = "neo4j://example.com:7687"

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

The auth token is an object of the class :class:`neo4j.Auth` containing the details.

.. autoclass:: neo4j.Auth



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

Every Neo4j-backed application will require a :class:`neo4j.Driver` object.

This object holds the details required to establish connections with a Neo4j database, including server URIs, credentials and other configuration.
:class:`neo4j.Driver` objects hold a connection pool from which :class:`neo4j.Session` objects can borrow connections.
Closing a driver will immediately shut down all connections in the pool.

.. note::
    Driver objects only open connections and pool them as needed. To verify that
    the driver is able to communicate with the database without executing any
    query, use :meth:`neo4j.Driver.verify_connectivity`.

.. autoclass:: neo4j.Driver()
   :members: session, encrypted, close, verify_connectivity, get_server_info


.. _driver-configuration-ref:

Driver Configuration
====================

Additional configuration can be provided via the :class:`neo4j.Driver` constructor.

+ :ref:`session-connection-timeout-ref`
+ :ref:`update-routing-table-timeout-ref`
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


.. _session-connection-timeout-ref:

``session_connection_timeout``
------------------------------
The maximum amount of time in seconds the session will wait when trying to
establish a usable read/write connection to the remote host.
This encompasses *everything* that needs to happen for this, including,
if necessary, updating the routing table, fetching a connection from the pool,
and, if necessary fully establishing a new connection with the reader/writer.

Since this process may involve updating the routing table, acquiring a
connection from the pool, or establishing a new connection, it should be chosen
larger than :ref:`update-routing-table-timeout-ref`,
:ref:`connection-acquisition-timeout-ref`, and :ref:`connection-timeout-ref`.

:Type: ``float``
:Default: ``120.0``

.. versionadded:: 4.4.5

.. versionchanged:: 5.0

    The default value was changed from ``float("inf")`` to ``120.0``.


.. _update-routing-table-timeout-ref:

``update_routing_table_timeout``
--------------------------------
The maximum amount of time in seconds the driver will attempt to fetch a new
routing table. This encompasses *everything* that needs to happen for this,
including fetching connections from the pool, performing handshakes, and
requesting and receiving a fresh routing table.

Since this process may involve acquiring a connection from the pool, or
establishing a new connection, it should be chosen larger than
:ref:`connection-acquisition-timeout-ref` and :ref:`connection-timeout-ref`.

This setting only has an effect for :ref:`neo4j-driver-ref`, but not for
:ref:`bolt-driver-ref` as it does no routing at all.

:Type: ``float``
:Default: ``90.0``

.. versionadded:: 4.4.5


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

This setting does not have any effect if a custom ``ssl_context`` is configured.

:Type: ``bool``
:Default: ``False``


.. _keep-alive-ref:

``keep_alive``
--------------
Specify whether TCP keep-alive should be enabled.

:Type: ``bool``
:Default: ``True``

**This is experimental.** (See :ref:`filter-warnings-ref`)


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
A custom resolver function to resolve host and port values ahead of DNS resolution.
This function is called with a 2-tuple of (host, port) and should return an iterable of 2-tuples (host, port).

If no custom resolver function is supplied, the internal resolver moves straight to regular DNS resolution.

For example:

.. code-block:: python

   from neo4j import GraphDatabase

   def custom_resolver(socket_address):
       if socket_address == ("example.com", 9999):
           yield "::1", 7687
           yield "127.0.0.1", 7687
       else:
           from socket import gaierror
           raise gaierror("Unexpected socket address %r" % socket_address)

   driver = GraphDatabase.driver("neo4j://example.com:9999",
                                 auth=("neo4j", "password"),
                                 resolver=custom_resolver)


:Default: :const:`None`


.. _trust-ref:

``trust``
---------
Specify how to determine the authenticity of encryption certificates provided by the Neo4j instance on connection.

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

If given, ``encrypted`` and ``trusted_certificates`` have no effect.

.. warning::
    This option may compromise your application's security if used improperly.

    Its usage is strongly discouraged and comes without any guarantees.

:Type: :class:`ssl.SSLContext` or :const:`None`
:Default: :const:`None`

.. versionadded:: 5.0


.. _trusted-certificates-ref:

``trusted_certificates``
------------------------
Specify how to determine the authenticity of encryption certificates provided by the Neo4j instance on connection.

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
More than one :class:`.Driver` may be required if connections to multiple databases, or connections as multiple users, are required,
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

    driver = GraphDatabase(uri, auth=(user, password))
    session = driver.session()
    result = session.run("MATCH (a:Person) RETURN a.name AS name")
    names = [record["name"] for record in result]
    session.close()
    driver.close()


Sessions will often be created and destroyed using a *with block context*.
This is the recommended approach as it takes care of closing the session
properly even when an exception is raised.

.. code-block:: python

    with driver.session() as session:
        result = session.run("MATCH (a:Person) RETURN a.name AS name")
        # do something with the result...


Sessions will often be created with some configuration settings, see :ref:`session-configuration-ref`.

.. code-block:: python

    with driver.session(database="example_database", fetch_size=100) as session:
        result = session.run("MATCH (a:Person) RETURN a.name AS name")
        # do something with the result...


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

    .. automethod:: write_transaction


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


.. _bookmarks-ref:

``bookmarks``
-------------
Optional :class:`neo4j.Bookmarks`. Use this to causally chain sessions.
See :meth:`Session.last_bookmarks` or :meth:`AsyncSession.last_bookmarks` for
more information.

.. deprecated:: 5.0
    Alternatively, an iterable of strings can be passed. This usage is
    deprecated and will be removed in a future release. Please use a
    :class:`neo4j.Bookmarks` object instead.

:Default: ``None``


.. _database-ref:

``database``
------------
Name of the database to query.

:Type: ``str``, ``neo4j.DEFAULT_DATABASE``


.. py:attribute:: neo4j.DEFAULT_DATABASE
    :noindex:

    This will use the default database on the Neo4j instance.


.. Note::

    The default database can be set on the Neo4j instance settings.

.. Note::
    It is recommended to always specify the database explicitly when possible.
    This allows the driver to work more efficiently, as it will not have to
    resolve the home database first.


.. code-block:: python

    from neo4j import GraphDatabase

    driver = GraphDatabase.driver(uri, auth=(user, password))
    session = driver.session(database="system")


:Default: ``neo4j.DEFAULT_DATABASE``


.. _impersonated-user-ref:

``impersonated_user``
---------------------
Name of the user to impersonate.
This means that all actions in the session will be executed in the security
context of the impersonated user. For this, the user for which the
:class:`Driver` has been created needs to have the appropriate permissions.

:Type: ``str``, None


.. py:data:: None
   :noindex:

   Will not perform impersonation.


.. Note::

    The server or all servers of the cluster need to support impersonation when.
    Otherwise, the driver will raise :exc:`.ConfigurationError`
    as soon as it encounters a server that does not.


.. code-block:: python

    from neo4j import GraphDatabase

    driver = GraphDatabase.driver(uri, auth=(user, password))
    session = driver.session(impersonated_user="alice")


:Default: :const:`None`


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
    access mode should be ``neo4j.ACCESS_WRITE`` or ``neo4j.ACCESS_READ``.
    This setting is only meant to enable the driver to perform correct routing,
    *not* for enforcing access control. This means that, depending on the server
    version and settings, the server or cluster might allow a write-statement to
    be executed even when ``neo4j.ACCESS_READ`` is chosen. This behaviour should
    not be relied upon as it can change with the server.

:Type: ``neo4j.WRITE_ACCESS``, ``neo4j.READ_ACCESS``
:Default: ``neo4j.WRITE_ACCESS``


.. _fetch-size-ref:

``fetch_size``
--------------
The fetch size used for requesting messages from Neo4j.

:Type: ``int``
:Default: ``1000``




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

Example:

.. code-block:: python

    import neo4j

    def create_person(driver, name):
        with driver.session(default_access_mode=neo4j.WRITE_ACCESS) as session:
            query = "CREATE (a:Person { name: $name }) RETURN id(a) AS node_id"
            result = session.run(query, name=name)
            record = result.single()
            return record["node_id"]

Example:

.. code-block:: python

    import neo4j

    def get_numbers(driver):
        numbers = []
        with driver.session(default_access_mode=neo4j.READ_ACCESS) as session:
            result = session.run("UNWIND [1, 2, 3] AS x RETURN x")
            for record in result:
                numbers.append(record["x"])
        return numbers


.. _explicit-transactions-ref:

Explicit Transactions
=====================
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

Explicit transactions are most useful for applications that need to distribute Cypher execution across multiple functions for the same transaction.

Example:

.. code-block:: python

    import neo4j

    def create_person(driver, name):
        with driver.session(default_access_mode=neo4j.WRITE_ACCESS) as session:
            tx = session.begin_transaction()
            node_id = create_person_node(tx)
            set_person_name(tx, node_id, name)
            tx.commit()

    def create_person_node(tx):
        query = "CREATE (a:Person { name: $name }) RETURN id(a) AS node_id"
        name = "default_name"
        result = tx.run(query, name=name)
        record = result.single()
        return record["node_id"]

    def set_person_name(tx, node_id, name):
        query = "MATCH (a:Person) WHERE id(a) = $id SET a.name = $name"
        result = tx.run(query, id=node_id, name=name)
        summary = result.consume()
        # use the summary for logging etc.

.. _managed-transactions-ref:

Managed Transactions (`transaction functions`)
==============================================
Transaction functions are the most powerful form of transaction, providing access mode override and retry capabilities.

+ :meth:`neo4j.Session.write_transaction`
+ :meth:`neo4j.Session.read_transaction`

These allow a function object representing the transactional unit of work to be passed as a parameter.
This function is called one or more times, within a configurable time limit, until it succeeds.
Results should be fully consumed within the function and only aggregate or status values should be returned.
Returning a live result object would prevent the driver from correctly managing connections and would break retry guarantees.

This function will receive a :class:`neo4j.ManagedTransaction` object as its first parameter.

.. autoclass:: neo4j.ManagedTransaction

    .. automethod:: run

Example:

.. code-block:: python

    def create_person(driver, name)
        with driver.session() as session:
            node_id = session.write_transaction(create_person_tx, name)

    def create_person_tx(tx, name):
        query = "CREATE (a:Person { name: $name }) RETURN id(a) AS node_id"
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

    .. automethod:: closed

See https://neo4j.com/docs/python-manual/current/cypher-workflow/#python-driver-type-mapping for more about type mapping.


Graph
=====

.. autoclass:: neo4j.graph.Graph()

    A local, self-contained graph object that acts as a container for :class:`.Node` and :class:`neo4j.Relationship` instances.
    This is typically obtained via the :meth:`neo4j.Result.graph` method.

    .. autoattribute:: nodes

    .. autoattribute:: relationships

    .. automethod:: relationship_type

**This is experimental.** (See :ref:`filter-warnings-ref`)


******
Record
******

.. autoclass:: neo4j.Record()

    A :class:`neo4j.Record` is an immutable ordered collection of key-value
    pairs. It is generally closer to a :class:`namedtuple` than to an
    :class:`OrderedDict` inasmuch as iteration of the collection will
    yield values rather than keys.

    .. describe:: Record(iterable)

        Create a new record based on an dictionary-like iterable.
        This can be a dictionary itself, or may be a sequence of key-value pairs, each represented by a tuple.

    .. describe:: record == other

        Compare a record for equality with another value.
        The ``other`` value may be any ``Sequence`` or``Mapping`` or both.
        If comparing with a``Sequence`` the values are compared in order.
        If comparing with a``Mapping`` the values are compared based on their keys.
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



***************
Core Data Types
***************

Cypher supports a set of core data types that all map to built-in types in Python.

These include the common``Boolean`` ``Integer`` ``Float`` and ``String`` types as well as ``List`` and ``Map`` that can hold heterogenous collections of any other type.

The core types with their general mappings are listed below:

+------------------------+---------------------------------------------------------------------------------------------------------------------------+
| Cypher Type            | Python Type                                                                                                               |
+========================+===========================================================================================================================+
| Null                   | :const:`None`                                                                                                             |
+------------------------+---------------------------------------------------------------------------------------------------------------------------+
| Boolean                | :class:`bool`                                                                                                             |
+------------------------+---------------------------------------------------------------------------------------------------------------------------+
| Integer                | :class:`int`                                                                                                              |
+------------------------+---------------------------------------------------------------------------------------------------------------------------+
| Float                  | :class:`float`                                                                                                            |
+------------------------+---------------------------------------------------------------------------------------------------------------------------+
| String                 | :class:`str`                                                                                                              |
+------------------------+---------------------------------------------------------------------------------------------------------------------------+
| Bytes :sup:`[1]`       | :class:`bytearray`                                                                                                        |
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

    .. autoattribute:: graph

    .. autoattribute:: id

    .. autoattribute:: element_id

    .. autoattribute:: labels

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

    .. autoattribute:: graph

    .. autoattribute:: id

    .. autoattribute:: element_id

    .. autoattribute:: nodes

    .. autoattribute:: start_node

    .. autoattribute:: end_node

    .. autoattribute:: type

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

    .. autoattribute:: graph

    .. autoattribute:: nodes

    .. autoattribute:: start_node

    .. autoattribute:: end_node

    .. autoattribute:: relationships


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


*******
Logging
*******

The driver offers logging for debugging purposes. It is not recommended to
enable logging for anything other than debugging. For instance, if the driver is
not able to connect to the database server or if undesired behavior is observed.

There are different ways of enabling logging as listed below.

Simple Approach
===============

.. autofunction:: neo4j.debug.watch(*logger_names, level=logging.DEBUG, out=sys.stderr, colour=False)

Context Manager
===============

.. autoclass:: neo4j.debug.Watcher(*logger_names, default_level=logging.DEBUG, default_out=sys.stderr, colour=False)
    :members:
    :special-members: __enter__, __exit__

Full Controll
=============

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
    # from now on, DEBUG logging to stderr is enabled in the driver


*********
Bookmarks
*********

.. autoclass:: neo4j.Bookmarks
    :members:

.. autoclass:: neo4j.Bookmark
    :members:
