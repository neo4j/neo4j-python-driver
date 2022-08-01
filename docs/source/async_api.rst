.. _async-api-documentation:

#######################
Async API Documentation
#######################

.. warning::
    The whole async API is currently in experimental phase.

    This means everything documented on this page might be removed or change
    its API at any time (including in patch releases).

    (See :ref:`filter-warnings-ref`)

.. versionadded:: 5.0

.. warning::
    There are known issue with Python 3.8 and the async driver where it
    gradually slows down. Generally, it's recommended to use the latest
    supported version of Python for best performance, stability, and security.

******************
AsyncGraphDatabase
******************

Async Driver Construction
=========================

The :class:`neo4j.AsyncDriver` construction is done via a ``classmethod`` on the :class:`neo4j.AsyncGraphDatabase` class.

.. autoclass:: neo4j.AsyncGraphDatabase
   :members: driver


Driver creation example:

.. code-block:: python

    import asyncio

    from neo4j import AsyncGraphDatabase

    async def main():
        uri = "neo4j://example.com:7687"
        driver = AsyncGraphDatabase.driver(uri, auth=("neo4j", "password"))

        await driver.close()  # close the driver object

     asyncio.run(main())


For basic authentication, ``auth`` can be a simple tuple, for example:

.. code-block:: python

   auth = ("neo4j", "password")

This will implicitly create a :class:`neo4j.Auth` with a ``scheme="basic"``.
Other authentication methods are described under :ref:`auth-ref`.

``with`` block context example:

.. code-block:: python

    import asyncio

    from neo4j import AsyncGraphDatabase

    async def main():
        uri = "neo4j://example.com:7687"
        auth = ("neo4j", "password")
        async with AsyncGraphDatabase.driver(uri, auth=auth) as driver:
            # use the driver
            ...

     asyncio.run(main())


.. _async-uri-ref:

URI
===

On construction, the ``scheme`` of the URI determines the type of :class:`neo4j.AsyncDriver` object created.

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

Each supported scheme maps to a particular :class:`neo4j.AsyncDriver` subclass that implements a specific behaviour.

+------------------------+---------------------------------------------------------------------------------------------------------------------------------------------+
| URI Scheme             | Driver Object and Setting                                                                                                                   |
+========================+=============================================================================================================================================+
| bolt                   | :ref:`async-bolt-driver-ref` with no encryption.                                                                                            |
+------------------------+---------------------------------------------------------------------------------------------------------------------------------------------+
| bolt+ssc               | :ref:`async-bolt-driver-ref` with encryption (accepts self signed certificates).                                                            |
+------------------------+---------------------------------------------------------------------------------------------------------------------------------------------+
| bolt+s                 | :ref:`async-bolt-driver-ref` with encryption (accepts only certificates signed by a certificate authority), full certificate checks.        |
+------------------------+---------------------------------------------------------------------------------------------------------------------------------------------+
| neo4j                  | :ref:`async-neo4j-driver-ref` with no encryption.                                                                                           |
+------------------------+---------------------------------------------------------------------------------------------------------------------------------------------+
| neo4j+ssc              | :ref:`async-neo4j-driver-ref` with encryption (accepts self signed certificates).                                                           |
+------------------------+---------------------------------------------------------------------------------------------------------------------------------------------+
| neo4j+s                | :ref:`async-neo4j-driver-ref` with encryption (accepts only certificates signed by a certificate authority), full certificate checks.       |
+------------------------+---------------------------------------------------------------------------------------------------------------------------------------------+

.. note::

    See https://neo4j.com/docs/operations-manual/current/configuration/ports/ for Neo4j ports.



***********
AsyncDriver
***********

Every Neo4j-backed application will require a :class:`neo4j.AsyncDriver` object.

This object holds the details required to establish connections with a Neo4j database, including server URIs, credentials and other configuration.
:class:`neo4j.AsyncDriver` objects hold a connection pool from which :class:`neo4j.AsyncSession` objects can borrow connections.
Closing a driver will immediately shut down all connections in the pool.

.. note::
    Driver objects only open connections and pool them as needed. To verify that
    the driver is able to communicate with the database without executing any
    query, use :meth:`neo4j.AsyncDriver.verify_connectivity`.

.. autoclass:: neo4j.AsyncDriver()
   :members: session, encrypted, close, verify_connectivity, get_server_info


.. _async-driver-configuration-ref:

Async Driver Configuration
==========================

:class:`neo4j.AsyncDriver` is configured exactly like :class:`neo4j.Driver`
(see :ref:`driver-configuration-ref`). The only difference is that the async
driver accepts an async custom resolver function:

.. _async-resolver-ref:

``resolver``
------------
A custom resolver function to resolve host and port values ahead of DNS resolution.
This function is called with a 2-tuple of (host, port) and should return an iterable of 2-tuples (host, port).

If no custom resolver function is supplied, the internal resolver moves straight to regular DNS resolution.

The custom resolver function can but does not have to be a coroutine.

For example:

.. code-block:: python

    from neo4j import AsyncGraphDatabase

    async def custom_resolver(socket_address):
        if socket_address == ("example.com", 9999):
            yield "::1", 7687
            yield "127.0.0.1", 7687
        else:
            from socket import gaierror
            raise gaierror("Unexpected socket address %r" % socket_address)

    # alternatively
    def custom_resolver(socket_address):
        ...

    driver = AsyncGraphDatabase.driver("neo4j://example.com:9999",
                                       auth=("neo4j", "password"),
                                       resolver=custom_resolver)


:Default: ``None``



Driver Object Lifetime
======================

For general applications, it is recommended to create one top-level :class:`neo4j.AsyncDriver` object that lives for the lifetime of the application.

For example:

.. code-block:: python

    from neo4j import AsyncGraphDatabase

    class Application:

        def __init__(self, uri, user, password)
            self.driver = AsyncGraphDatabase.driver(uri, auth=(user, password))

        async def close(self):
            await self.driver.close()

Connection details held by the :class:`neo4j.AsyncDriver` are immutable.
Therefore if, for example, a password is changed, a replacement :class:`neo4j.AsyncDriver` object must be created.
More than one :class:`.AsyncDriver` may be required if connections to multiple databases, or connections as multiple users, are required,
unless when using impersonation (:ref:`impersonated-user-ref`).

:class:`neo4j.AsyncDriver` objects are safe to be used in concurrent coroutines.
They are not thread-safe.


.. _async-bolt-driver-ref:

AsyncBoltDriver
===============

URI schemes:
    ``bolt``, ``bolt+ssc``, ``bolt+s``

Will result in:

.. autoclass:: neo4j.AsyncBoltDriver


.. _async-neo4j-driver-ref:

AsyncNeo4jDriver
================

URI schemes:
    ``neo4j``, ``neo4j+ssc``, ``neo4j+s``

Will result in:

.. autoclass:: neo4j.AsyncNeo4jDriver


*********************************
AsyncSessions & AsyncTransactions
*********************************
All database activity is co-ordinated through two mechanisms:
**sessions** (:class:`neo4j.AsyncSession`) and **transactions**
(:class:`neo4j.AsyncTransaction`, :class:`neo4j.AsyncManagedTransaction`).

A **session** is a logical container for any number of causally-related transactional units of work.
Sessions automatically provide guarantees of causal consistency within a clustered environment but multiple sessions can also be causally chained if required.
Sessions provide the top level of containment for database activity.
Session creation is a lightweight operation and *sessions are not thread safe*.

Connections are drawn from the :class:`neo4j.AsyncDriver` connection pool as required.

A **transaction** is a unit of work that is either committed in its entirety or is rolled back on failure.


.. _async-session-construction-ref:

*************************
AsyncSession Construction
*************************

To construct a :class:`neo4j.AsyncSession` use the :meth:`neo4j.AsyncDriver.session` method.

.. code-block:: python

    import asyncio

    from neo4j import AsyncGraphDatabase

    async def main():
        driver = AsyncGraphDatabase(uri, auth=(user, password))
        session = driver.session()
        result = await session.run("MATCH (a:Person) RETURN a.name AS name")
        names = [record["name"] async for record in result]
        await session.close()
        await driver.close()

    asyncio.run(main())


Sessions will often be created and destroyed using a *with block context*.
This is the recommended approach as it takes care of closing the session
properly even when an exception is raised.

.. code-block:: python

    async with driver.session() as session:
        result = await session.run("MATCH (a:Person) RETURN a.name AS name")
        # do something with the result...


Sessions will often be created with some configuration settings, see :ref:`async-session-configuration-ref`.

.. code-block:: python

    async with driver.session(database="example_database",
                              fetch_size=100) as session:
        result = await session.run("MATCH (a:Person) RETURN a.name AS name")
        # do something with the result...


************
AsyncSession
************

.. autoclass:: neo4j.AsyncSession()

    .. note::

        Some asyncio utility functions (e.g., :func:`asyncio.wait_for` and
        :func:`asyncio.shield`) will wrap work in a :class:`asyncio.Task`.
        This introduces concurrency and can lead to undefined behavior as
        :class:`AsyncSession` is not concurrency-safe.

        Consider this **wrong** example::

            async def dont_do_this(driver):
                async with driver.session() as session:
                    await asyncio.shield(session.run("RETURN 1"))

        If ``dont_do_this`` gets cancelled while waiting for ``session.run``,
        ``session.run`` itself won't get cancelled (it's shielded) so it will
        continue to use the session in another Task. Concurrently, will the
        async context manager (``async with driver.session()``) on exit clean
        up the session. That's two Tasks handling the session concurrently.
        Therefore, this yields undefined behavior.

        In this particular example, the problem could be solved by shielding
        the whole coroutine ``dont_do_this`` instead of only the
        ``session.run``. Like so::

            async def thats_better(driver):
                async def inner()
                    async with driver.session() as session:
                        await session.run("RETURN 1")

                await asyncio.shield(inner())

    .. automethod:: close

    .. automethod:: cancel

    .. automethod:: closed

    .. automethod:: run

    .. automethod:: last_bookmarks

    .. automethod:: last_bookmark

    .. automethod:: begin_transaction

    .. automethod:: read_transaction

    .. automethod:: write_transaction



.. _async-session-configuration-ref:

Session Configuration
=====================

:class:`neo4j.AsyncSession` is configured exactly like :class:`neo4j.Session`
(see :ref:`session-configuration-ref`).


****************
AsyncTransaction
****************

Neo4j supports three kinds of async transaction:

+ :ref:`async-auto-commit-transactions-ref`
+ :ref:`async-explicit-transactions-ref`
+ :ref:`async-managed-transactions-ref`

Each has pros and cons but if in doubt, use a managed transaction with a *transaction function*.


.. _async-auto-commit-transactions-ref:

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

    async def create_person(driver, name):
        async with driver.session(
            default_access_mode=neo4j.WRITE_ACCESS
        ) as session:
            query = "CREATE (a:Person { name: $name }) RETURN id(a) AS node_id"
            result = await session.run(query, name=name)
            record = await result.single()
            return record["node_id"]

Example:

.. code-block:: python

    import neo4j

    async def get_numbers(driver):
        numbers = []
        async with driver.session(
            default_access_mode=neo4j.READ_ACCESS
        ) as session:
            result = await session.run("UNWIND [1, 2, 3] AS x RETURN x")
            async for record in result:
                numbers.append(record["x"])
        return numbers


.. _async-explicit-transactions-ref:

Explicit Async Transactions
===========================
Explicit transactions support multiple statements and must be created with an explicit :meth:`neo4j.AsyncSession.begin_transaction` call.

This creates a new :class:`neo4j.AsyncTransaction` object that can be used to run Cypher.

It also gives applications the ability to directly control ``commit`` and ``rollback`` activity.

.. autoclass:: neo4j.AsyncTransaction()

    .. automethod:: run

    .. automethod:: commit

    .. automethod:: rollback

    .. automethod:: close

    .. automethod:: cancel

    .. automethod:: closed

Closing an explicit transaction can either happen automatically at the end of a ``async with`` block,
or can be explicitly controlled through the :meth:`neo4j.AsyncTransaction.commit`, :meth:`neo4j.AsyncTransaction.rollback`, :meth:`neo4j.AsyncTransaction.close` or :meth:`neo4j.AsyncTransaction.cancel` methods.

Explicit transactions are most useful for applications that need to distribute Cypher execution across multiple functions for the same transaction.

Example:

.. code-block:: python

    import neo4j

    async def create_person(driver, name):
        async with driver.session(
            default_access_mode=neo4j.WRITE_ACCESS
        ) as session:
            tx = await session.begin_transaction()
            node_id = await create_person_node(tx)
            await set_person_name(tx, node_id, name)
            await tx.commit()

    async def create_person_node(tx):
        query = "CREATE (a:Person { name: $name }) RETURN id(a) AS node_id"
        name = "default_name"
        result = await tx.run(query, name=name)
        record = await result.single()
        return record["node_id"]

    async def set_person_name(tx, node_id, name):
        query = "MATCH (a:Person) WHERE id(a) = $id SET a.name = $name"
        result = await tx.run(query, id=node_id, name=name)
        summary = await result.consume()
        # use the summary for logging etc.

.. _async-managed-transactions-ref:


Managed Async Transactions (`transaction functions`)
====================================================
Transaction functions are the most powerful form of transaction, providing access mode override and retry capabilities.

+ :meth:`neo4j.AsyncSession.write_transaction`
+ :meth:`neo4j.AsyncSession.read_transaction`

These allow a function object representing the transactional unit of work to be passed as a parameter.
This function is called one or more times, within a configurable time limit, until it succeeds.
Results should be fully consumed within the function and only aggregate or status values should be returned.
Returning a live result object would prevent the driver from correctly managing connections and would break retry guarantees.

This function will receive a :class:`neo4j.AsyncManagedTransaction` object as its first parameter.

.. autoclass:: neo4j.AsyncManagedTransaction

    .. automethod:: run

Example:

.. code-block:: python

    async def create_person(driver, name)
        async with driver.session() as session:
            node_id = await session.write_transaction(create_person_tx, name)

    async def create_person_tx(tx, name):
        query = "CREATE (a:Person { name: $name }) RETURN id(a) AS node_id"
        result = await tx.run(query, name=name)
        record = await result.single()
        return record["node_id"]

To exert more control over how a transaction function is carried out, the :func:`neo4j.unit_of_work` decorator can be used.



***********
AsyncResult
***********

Every time a query is executed, a :class:`neo4j.AsyncResult` is returned.

This provides a handle to the result of the query, giving access to the records within it as well as the result metadata.

Results also contain a buffer that automatically stores unconsumed records when results are consumed out of order.

A :class:`neo4j.AsyncResult` is attached to an active connection, through a :class:`neo4j.AsyncSession`, until all its content has been buffered or consumed.

.. autoclass:: neo4j.AsyncResult()

    .. method:: result.__aiter__()
        :async:

    .. method:: result.__anext__()
        :async:

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



******************
Async Cancellation
******************

Async Python provides a mechanism for cancelling futures
(:meth:`asyncio.Future.cancel`). The driver and its components can handle this.
However, generally, it's not advised to rely on cancellation as it forces the
driver to close affected connections to avoid leaving them in an undefined
state. This makes the driver less efficient.

The easiest way to make sure your application code's interaction with the driver
is playing nicely with cancellation is to always use the async context manager
provided by :class:`neo4j.AsyncSession` like so: ::

    async with driver.session() as session:
        ...  # do what you need to do with the session

If, for whatever reason, you need handle the session manually, you can it like
so: ::

    session = await with driver.session()
    try:
        ...  # do what you need to do with the session
    except asyncio.CancelledError:
        session.cancel()
        raise
    finally:
        # this becomes a no-op if the session has been cancelled before
        await session.close()

As mentioned above, any cancellation of I/O work will cause the driver to close
the affected connection. This will kill any :class:`neo4j.AsyncTransaction` and
:class:`neo4j.AsyncResult` objects that are attached to that connection. Hence,
after catching a :class:`asyncio.CancelledError`, you should not try to use
transactions or results created earlier. They are likely to not be valid
anymore.

Furthermore, there is no guarantee as to whether a piece of ongoing work got
successfully executed on the server side or not, when a cancellation happens:
``await transaction.commit()`` and other methods can throw
:exc:`asyncio.CancelledError` but still have managed to complete from the
server's perspective.
