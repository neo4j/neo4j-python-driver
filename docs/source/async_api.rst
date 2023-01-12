.. _async-api-documentation:

#######################
Async API Documentation
#######################

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
    :members: bookmark_manager

    .. automethod:: driver

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
                    ...  # use the driver

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

Every Neo4j-backed application will require a driver object.

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
A custom resolver function to resolve any addresses the driver receives ahead of DNS resolution.
This function is called with an :class:`.Address` and should return an iterable of :class:`.Address` objects or values that can be used to construct :class:`.Address` objects.

If no custom resolver function is supplied, the internal resolver moves straight to regular DNS resolution.

The custom resolver function can but does not have to be a coroutine.

For example:

.. code-block:: python

   import neo4j


    async def custom_resolver(socket_address):
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


    # alternatively
    def custom_resolver(socket_address):
        ...


   driver = neo4j.GraphDatabase.driver("neo4j://example.com:9999",
                                       auth=("neo4j", "password"),
                                       resolver=custom_resolver)


:Default: :data:`None`



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
More than one :class:`.AsyncDriver` may be required if connections to multiple remotes, or connections as multiple users, are required,
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
        async with AsyncGraphDatabase(uri, auth=(user, password)) as driver:
            session = driver.session()
            try:
                result = await session.run("MATCH (a:Person) RETURN a.name AS name")
                names = [record["name"] async for record in result]
            except asyncio.CancelledError:
                session.cancel()
                raise
            finally:
                await session.close()

    asyncio.run(main())


Sessions will often be created and destroyed using a *with block context*.
This is the recommended approach as it takes care of closing the session
properly even when an exception is raised.

.. code-block:: python

    async with driver.session() as session:
        result = await session.run("MATCH (a:Person) RETURN a.name AS name")
        ...  # do something with the result


Sessions will often be created with some configuration settings, see :ref:`async-session-configuration-ref`.

.. code-block:: python

    async with driver.session(database="example_database",
                              fetch_size=100) as session:
        result = await session.run("MATCH (a:Person) RETURN a.name AS name")
        ...  # do something with the result


************
AsyncSession
************

.. autoclass:: neo4j.AsyncSession()

    .. note::

        Some asyncio utility functions (e.g., :func:`asyncio.wait_for` and
        :func:`asyncio.shield`) will wrap work in a :class:`asyncio.Task`.
        This introduces concurrency and can lead to undefined behavior as
        :class:`AsyncSession` is not concurrency-safe.

        Consider this **wrong** example

        .. code-block:: python

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
        ``session.run``. Like so

        .. code-block:: python

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

    .. automethod:: execute_read

    .. automethod:: write_transaction

    .. automethod:: execute_write



.. _async-session-configuration-ref:

Session Configuration
=====================

:class:`neo4j.AsyncSession` is configured exactly like :class:`neo4j.Session`
(see :ref:`session-configuration-ref`). The only difference is the async session
accepts either a :class:`neo4j.api.BookmarkManager` object or a
:class:`neo4j.api.AsyncBookmarkManager` as bookmark manager:


.. _async-bookmark-manager-ref:

``bookmark_manager``
--------------------
Specify a bookmark manager for the driver to use. If present, the bookmark
manager is used to keep all work on the driver causally consistent.

See :class:`BookmarkManager` for more information.

.. warning::
    Enabling the BookmarkManager can have a negative impact on performance since
    all queries will wait for the latest changes to be propagated across the
    cluster.

    For simpler use-cases, sessions (:class:`.AsyncSession`) can be used to
    group a series of queries together that will be causally chained
    automatically.

:Type: :data:`None`, :class:`BookmarkManager`, or :class:`AsyncBookmarkManager`
:Default: :data:`None`

**This is experimental.** (See :ref:`filter-warnings-ref`)
It might be changed or removed any time even without prior notice.



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

Write example:

.. code-block:: python

    import neo4j


    async def create_person(driver, name):
        # default_access_mode defaults to WRITE_ACCESS
        async with driver.session(database="neo4j") as session:
            query = "CREATE (a:Person { name: $name }) RETURN id(a) AS node_id"
            result = await session.run(query, name=name)
            record = await result.single()
            return record["node_id"]

Read example:

.. code-block:: python

    import neo4j


    async def get_numbers(driver):
        numbers = []
        async with driver.session(
            database="neo4j",
            default_access_mode=neo4j.READ_ACCESS
        ) as session:
            result = await session.run("UNWIND [1, 2, 3] AS x RETURN x")
            async for record in result:
                numbers.append(record["x"])
        return numbers


.. _async-explicit-transactions-ref:

Explicit Transactions (Unmanaged Transactions)
==============================================
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

Explicit transactions are most useful for applications that need to distribute Cypher execution across multiple functions for the same transaction or that need to run multiple queries within a single transaction but without the retries provided by managed transactions.

Example:

.. code-block:: python

    import asyncio

    import neo4j


    async def transfer_to_other_bank(driver, customer_id, other_bank_id, amount):
        async with driver.session(
            database="neo4j",
            # optional, defaults to WRITE_ACCESS
            default_access_mode=neo4j.WRITE_ACCESS
        ) as session:
            tx = await session.begin_transaction()
            # or just use a `with` context instead of try/excpet/finally
            try:
                if not await customer_balance_check(tx, customer_id, amount):
                    # give up
                    return
                await other_bank_transfer_api(customer_id, other_bank_id, amount)
                # Now the money has been transferred
                # => we can't retry or rollback anymore
                try:
                    await decrease_customer_balance(tx, customer_id, amount)
                    await tx.commit()
                except Exception as e:
                    request_inspection(customer_id, other_bank_id, amount, e)
                    raise
            except asyncio.CancelledError:
                tx.cancel()
                raise
            finally:
                await tx.close()  # rolls back if not yet committed


    async def customer_balance_check(tx, customer_id, amount):
        query = ("MATCH (c:Customer {id: $id}) "
                 "RETURN c.balance >= $amount AS sufficient")
        result = await tx.run(query, id=customer_id, amount=amount)
        record = await result.single(strict=True)
        return record["sufficient"]


    async def other_bank_transfer_api(customer_id, other_bank_id, amount):
        ...  # make some API call to other bank


    async def decrease_customer_balance(tx, customer_id, amount):
        query = ("MATCH (c:Customer {id: $id}) "
                 "SET c.balance = c.balance - $amount")
        await tx.run(query, id=customer_id, amount=amount)


    def request_inspection(customer_id, other_bank_id, amount, e):
        # manual cleanup required; log this or similar
        print("WARNING: transaction rolled back due to exception:", repr(e))
        print("customer_id:", customer_id, "other_bank_id:", other_bank_id,
              "amount:", amount)

.. _async-managed-transactions-ref:


Managed Transactions (`transaction functions`)
==============================================
Transaction functions are the most powerful form of transaction, providing access mode override and retry capabilities.

+ :meth:`neo4j.AsyncSession.execute_write`
+ :meth:`neo4j.AsyncSession.execute_read`

These allow a function object representing the transactional unit of work to be passed as a parameter.
This function is called one or more times, within a configurable time limit, until it succeeds.
Results should be fully consumed within the function and only aggregate or status values should be returned.
Returning a live result object would prevent the driver from correctly managing connections and would break retry guarantees.

This function will receive a :class:`neo4j.AsyncManagedTransaction` object as its first parameter. For more details see :meth:`neo4j.AsyncSession.execute_write` and :meth:`neo4j.AsyncSession.execute_read`.

.. autoclass:: neo4j.AsyncManagedTransaction()

    .. automethod:: run

Example:

.. code-block:: python

    async def create_person(driver, name)
        async with driver.session() as session:
            node_id = await session.execute_write(create_person_tx, name)


    async def create_person_tx(tx, name):
        query = ("CREATE (a:Person {name: $name, id: randomUUID()}) "
                 "RETURN a.id AS node_id")
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


********************
AsyncBookmarkManager
********************

.. autoclass:: neo4j.api.AsyncBookmarkManager
    :members:


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


.. _async-logging-ref:

*************
Async Logging
*************

For the most parts, logging works the same way as in the synchronous driver.
See :ref:`logging-ref` for more information.

However, when following the manual approach to logging, it is recommended to
include information about the current async task in the log record.
Like so:

.. code-block:: python

    import asyncio
    import logging
    import sys

    class TaskIdFilter(logging.Filter):
        """Injecting async task id into log records."""

        def filter(self, record):
            try:
                record.taskId = id(asyncio.current_task())
            except RuntimeError:
                record.taskId = None
            return True


    # create a handler, e.g. to log to stdout
    handler = logging.StreamHandler(sys.stdout)
    # configure the handler to your liking
    handler.setFormatter(logging.Formatter(
        "[%(levelname)-8s] [Task %(taskId)-15s] %(asctime)s  %(message)s"
        # or when using threading AND asyncio
        # "[%(levelname)-8s] [Thread %(thread)d] [Task %(taskId)-15s] "
        # "%(asctime)s  %(message)s"
    ))
    # attache the filter injecting the task id to the handler
    handler.addFilter(TaskIdFilter())
    # add the handler to the driver's logger
    logging.getLogger("neo4j").addHandler(handler)
    # make sure the logger logs on the desired log level
    logging.getLogger("neo4j").setLevel(logging.DEBUG)
    # from now on, DEBUG logging to stdout is enabled in the driver
