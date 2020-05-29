#############################
Neo4j Python Driver |version|
#############################

.. warning::
    This API docs is not production ready!


The Official Neo4j Driver for Python.

Neo4j versions supported:

* Neo4j 4.0 - Using the Bolt Protocol Version 4.0
* Neo4j 3.5 - Using the Bolt Protocol Version 3

Python versions supported:

* Python 3.8
* Python 3.7
* Python 3.6
* Python 3.5

.. note::
   Python 2.7 support has been dropped.

   The previous driver `Python Driver 1.7`_ supports older versions of python,
   the **Neo4j 4.0** will work in fallback mode (using Bolt Protocol Version 3) with that driver.

*************
Quick Example
*************

.. code-block:: python

    from neo4j import GraphDatabase

    uri = "bolt://localhost:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))

    def create_friend_of(tx, name, friend):
        tx.run("CREATE (a:Person)-[:KNOWS]->(f:Person {name: $friend}) "
               "WHERE a.name = $name "
               "RETURN f.name AS friend", name=name, friend=friend)

    with driver.session() as session:
        session.write_transaction(create_friend_of, "Alice", "Bob")

    with driver.session() as session:
        session.write_transaction(create_friend_of, "Alice", "Carl")

    driver.close()

.. code-block:: python

    from neo4j import GraphDatabase

    uri = "bolt://localhost:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))

    def get_friends_of(tx, name):
        friends = []
        result = tx.run("MATCH (a:Person)-[:KNOWS]->(f) "
                             "WHERE a.name = $name "
                             "RETURN f.name AS friend", name=name):
        for record in result:
            friends.append(record["friend"])
        return friends

    with driver.session() as session:
        friends = session.read_transaction(get_friends_of, "Alice")
        for friend in friends:
            print(friend)

    driver.close()


************
Installation
************

To install the latest stable driver release, use:

.. code:: bash

    python -m pip install neo4j

.. note::

   It is always recommended to install python packages for user space in a virtual environment.


Virtual Environment
===================

To create a virtual environment named sandbox, use:

.. code:: bash

    python -m venv sandbox

To activate the virtual environment named sandbox, use:

.. code:: bash

    source sandbox/bin/activate

To deactivate the current active virtual environment, use:

.. code:: bash

    deactivate


****************
Breaking Changes
****************

Version Scheme Changes
======================

The version number have jumped from **Python Driver 1.7** to **Python Driver 4.0** to align with the Neo4j Database version scheme.


Namespace Changes
=================

.. code-block:: python

    import neo4j.v1

Have changed to

.. code-block:: python

    import neo4j


Secure Connection
=================

**Neo4j 4.0** is by default configured to use a **non secure connection**.

The driver configuration argument :code:`encrypted` is by default set to :code:`False`.

**Note:** To be able to connect to **Neo4j 3.5** set :code:`encrypted=True` to have it configured as the default for that setup.

.. code-block:: python

    from neo4j import GraphDatabase

    driver = GraphDatabase("bolt://localhost:7687", auth=("neo4j", "password"), encrypted=True)
    driver.close()


Bookmark Changes
================

Bookmarks is now a Bookmark class instead of a string.


Exceptions Changes
==================

The exceptions in :code:`neo4j.exceptions` have been updated and there is internal exceptions starting with the naming :code:`Bolt` that should be propagated into the exceptions API.


URI Scheme Changes
==================

**bolt+routing** have been renamed to **neo4j**.


Class Renaming Changes
======================

* :code:`BoltStatementResult` is now :code:`Result`
* :code:`StatementResultSummary` is now :code:`ResultSummary`
* :code:`Statement` is now :code:`Query`


Argument Renaming Changes
=========================

* :code:`statement` is now :code:`query`
* :code:`cypher` is now :code:`query`
* :code:`Session.run(cypher, ...` is now :code:`Session.run(query, ...`
* :code:`Transaction.run(statement, ...` is now :code:`Transaction.run(query, ...`
* :code:`StatementResultSummary.statement` is now :code:`ResultSummary.query`
* :code:`StatementResultSummary.statement_type` is now :code:`ResultSummary.query_type`
* :code:`StatementResultSummary.protocol_version` is now :code:`ResultSummary.server.protocol_version`


API Changes
=========================

* :code:`Result.summary()` have been replaced with :code:`Result.consume()`, this behaviour is to consume all remaining records in the buffer and returns the ResultSummary.


Dependency Changes
==================

* The dependency :code:`neobolt` have been removed.
* The dependency :code:`neotime` have been removed.


Configuration Name Changes
==========================

* :code:`max_retry_time` is now :code:`max_transaction_retry_time`


#################
API Documentation
#################

*************
GraphDatabase
*************

Driver Construction
===================

The :class:`neo4j.Driver` construction is via a `classmethod` on the :class:`neo4j.GraphDatabase` class.

.. autoclass:: neo4j.GraphDatabase
   :members: driver

See :ref:`driver-configuration-ref` for available config values.


.. code-block:: python

   from neo4j import GraphDatabase

   driver = GraphDatabase.driver(uri, auth=(user, password))

   driver.close()

URI
===

On construction, the `scheme` of the URI determines the type of :class:`neo4j.Driver` object created.

.. code-block:: python

    uri = bolt://localhost:7676

.. code-block:: python

    uri = neo4j://localhost:7676

Each supported scheme maps to a particular :class:`neo4j.Driver` subclass that implements a specific behaviour.

+------------------------+---------------------------------------------------------------------------------------------------------------------------+
| URI Scheme             | Driver Object and Setting                                                                                                 |
+========================+===========================================================================================================================+
| bolt                   | BoltDriver with no encryption.                                                                                            |
+------------------------+---------------------------------------------------------------------------------------------------------------------------+
| bolt+ssc               | BoltDriver with encryption (accepts self signed certificates).                                                            |
+------------------------+---------------------------------------------------------------------------------------------------------------------------+
| bolt+s                 | BoltDriver with encryption (accepts only certificates signed by an certificate authority), full certificate checks.       |
+------------------------+---------------------------------------------------------------------------------------------------------------------------+
| neo4j                  | Neo4jDriver with no encryption.                                                                                           |
+------------------------+---------------------------------------------------------------------------------------------------------------------------+
| neo4j+ssc              | Neo4jDriver with encryption (accepts self signed certificates).                                                           |
+------------------------+---------------------------------------------------------------------------------------------------------------------------+
| neo4j+s                | Neo4jDriver with encryption (accepts only certificates signed by an certificate authority), full certificate checks.      |
+------------------------+---------------------------------------------------------------------------------------------------------------------------+


Auth
====

An authentication token for the server.

For basic auth, this can be a simple tuple, for example:

.. code-block:: python

   auth = ("neo4j", "password")

Alternatively, one of the auth token functions can be used.

.. autofunction:: neo4j.basic_auth

.. autofunction:: neo4j.custom_auth


******
Driver
******

Every Neo4j-backed application will require a :class:`neo4j.Driver` object.

This object holds the details required to establish connections with a Neo4j database, including server URIs, credentials and other configuration.
:class:`neo4j.Driver` objects hold a connection pool from which :class:`neo4j.Session` objects can borrow connections.
Closing a driver will immediately shut down all connections in the pool.

.. autoclass:: neo4j.Driver()
   :members: session, close

See :ref:`session-configuration-ref` for available config values.


.. _driver-configuration-ref:

Driver Configuration
====================

Additional configuration, can be provided via the :class:`neo4j.Driver` constructor.


``max_connection_lifetime``
---------------------------

The maximum duration in seconds that the driver will keep a connection for before being removed from the pool.

:Type: ``float``
:Default: ``3600``


``max_connection_pool_size``
----------------------------
The maximum total number of connections allowed, per host (i.e. cluster nodes), to be managed by the connection pool.

:Type: ``int``
:Default: ``100``


``connection_timeout``
----------------------
The maximum amount of time in seconds to wait for a TCP connection to be established.

:Type: ``float``
:Default: ``30.0``


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


:Default: ``None``


``encrypted``
-------------
Specify whether to use an encrypted connection between the driver and server.

:Type: ``bool``
:Default: ``False``


``user_agent``
--------------
Specify the client agent name.

:Type: ``str``
:Default: *The Python Driver will generate a user agent name.*


``protocol_version``
--------------------
Specify a specific Bolt Protocol Version.

.. code-block:: python

   protocol_version = (4, 0)

:Type: ``tuple``
:Default: ``None``

**This is experimental.**


``init_size``
-------------
This will seed the pool with the specified number of connections.

:Type: ``int``
:Default: ``1``

**This is experimental.**


``keep_alive``
--------------
Specify whether TCP keep-alive should be enabled.

:Type: ``bool``
:Default: ``True``

**This is experimental.**


``connection_acquisition_timeout``
----------------------------------
The maximum amount of time in seconds a session will wait when requesting a connection from the connection pool.
Since the process of acquiring a connection may involve creating a new connection, ensure that the value of this configuration is higher than the configured `connection_timeout`.

:Type: ``float``
:Default: ``60.0``


``max_transaction_retry_time``
------------------------------
 The maximum amount of time in seconds that a managed transaction will retry before failing.

:Type: ``float``
:Default: ``30.0``


``initial_retry_delay``
-----------------------
Time in seconds.

:Type: ``float``
:Default: ``1.0``

**This is experimental.**


``retry_delay_multiplier``
--------------------------
Time in seconds.

:Type: ``float``
:Default: ``2.0``

**This is experimental.**


``retry_delay_jitter_factor``
-----------------------------
Time in seconds.

:Type: ``float``
:Default: ``0.2``

**This is experimental.**


``database``
------------
Name of the database to query.

:Type: ``str``, ``neo4j.DEFAULT_DATABASE``


.. py:attribute:: neo4j.DEFAULT_DATABASE

   This will use the default database on the Neo4j instance.


.. Note::

   The default database can be set on the Neo4j instance settings.


.. code-block:: python

   from neo4j import GraphDatabase
   driver = GraphDatabase.driver(uri, auth=(user, password), database="system")


:Default: ``neo4j.DEFAULT_DATABASE``


``fetch_size``
--------------
The fetch size used for requesting messages from Neo4j.

:Type: ``int``
:Default: ``1000``


``default_access_mode``
-----------------------
The default access mode.

:Type: ``neo4j.WRITE_ACCESS``, ``neo4j.READ_ACCESS``
:Default: ``neo4j.WRITE_ACCESS``


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
More than one :class:`.Driver` may be required if connections to multiple databases, or connections as multiple users, are required.

:class:`neo4j.Driver` objects are thread-safe but cannot be shared across processes.
Therefore, ``multithreading`` should generally be preferred over ``multiprocessing`` for parallel database access.
If using ``multiprocessing`` however, each process will require its own :class:`neo4j.Driver` object.


BoltDriver
==========

URI schemes:
    ``bolt``, ``bolt+ssc``, ``bolt+s``

Driver subclass:
    :class:`neo4j.BoltDriver`

..
   .. autoclass:: neo4j.BoltDriver


Neo4jDriver
===========

URI schemes:
    ``neo4j``, ``neo4j+ssc``, ``neo4j+s``

Driver subclass:
    :class:`neo4j.Neo4jDriver`

..
   .. autoclass:: neo4j.Neo4jDriver


***********************
Sessions & Transactions
***********************
All database activity is co-ordinated through two mechanisms: the :class:`neo4j.Session` and the :class:`neo4j.Transaction`.

A :class:`neo4j.Session` is a logical container for any number of causally-related transactional units of work.
Sessions automatically provide guarantees of causal consistency within a clustered environment but multiple sessions can also be causally chained if required.
Session provide the top-level of containment for database activity.
Session creation is a lightweight operation and *sessions are not thread safe*.

Connections are drawn from the :class:`neo4j.Driver` connection pool as required; an idle session will not hold onto a connection.

A :class:`neo4j.Transaction` is a unit of work that is either committed in its entirety or is rolled back on failure.

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

.. code-block:: python

    with driver.session() as session:
        result = session.run("MATCH (a:Person) RETURN a.name AS name")
        # do something with the result...

*******
Session
*******

.. class:: neo4j.Session

    .. automethod:: close

    .. automethod:: run

    .. automethod:: last_bookmark

    .. automethod:: begin_transaction

    .. automethod:: read_transaction

    .. automethod:: write_transaction


.. _session-configuration-ref:

Session Configuration
=====================

If the value is not set for the session, then the session will inherit the configured value from the driver object.

``connection_acquisition_timeout``
----------------------------------
The maximum amount of time in seconds a session will wait when requesting a connection from the connection pool.
Since the process of acquiring a connection may involve creating a new connection, ensure that the value of this configuration is higher than the configured `connection_timeout`.

:Type: ``float``
:Default: ``60.0``


``max_transaction_retry_time``
------------------------------
 The maximum amount of time in seconds that a managed transaction will retry before failing.

:Type: ``float``
:Default: ``30.0``


``initial_retry_delay``
-----------------------
Time in seconds.

:Type: ``float``
:Default: ``1.0``

**This is experimental.**


``retry_delay_multiplier``
--------------------------
Time in seconds.

:Type: ``float``
:Default: ``2.0``

**This is experimental.**


``retry_delay_jitter_factor``
-----------------------------
Time in seconds.

:Type: ``float``
:Default: ``0.2``

**This is experimental.**


``database``
------------
Name of the database to query.

:Type: ``str``, ``neo4j.DEFAULT_DATABASE``


.. py:attribute:: neo4j.DEFAULT_DATABASE
   :noindex:

   This will use the default database on the Neo4j instance.


.. Note::

   The default database can be set on the Neo4j instance settings.


.. code-block:: python

   from neo4j import GraphDatabase
   driver = GraphDatabase.driver(uri, auth=(user, password))
   session = driver.session(database="system")


:Default: ``neo4j.DEFAULT_DATABASE``


``fetch_size``
--------------
The fetch size used for requesting messages from Neo4j.

:Type: ``int``
:Default: ``1000``


``bookmarks``
-------------
An iterable containing ``neo4j.Bookmark``.

:Default: ``()``


``default_access_mode``
-----------------------
The default access mode.

:Type: ``neo4j.WRITE_ACCESS``, ``neo4j.READ_ACCESS``
:Default: ``neo4j.WRITE_ACCESS``


***********
Transaction
***********

Neo4j supports three kinds of transaction: `auto-commit transactions`, `explicit transactions` and `transaction functions`.
Each has pros and cons but if in doubt, use a transaction function.

Auto-commit Transactions
========================
Auto-commit transactions are the simplest form of transaction, available via :meth:`.Session.run`.
These are easy to use but support only one statement per transaction and are not automatically retried on failure.
Auto-commit transactions are also the only way to run ``PERIODIC COMMIT`` statements, since this Cypher clause manages its own transactions internally.

.. code-block:: python

    def create_person(driver, name):
        with driver.session() as session:
            return session.run("CREATE (a:Person {name:$name}) "
                               "RETURN id(a)", name=name).single().value()

Explicit Transactions
=====================
Explicit transactions support multiple statements and must be created with an explicit :meth:`neo4j.Session.begin_transaction` call.

This creates a new :class:`neo4j.Transaction` object that can be used to run Cypher.

It also gives applications the ability to directly control `commit` and `rollback` activity.

.. class:: neo4j.Transaction

    .. automethod:: run

    .. automethod:: closed

    .. automethod:: commit

    .. automethod:: rollback

Closing an explicit transaction can either happen automatically at the end of a ``with`` block,
or can be explicitly controlled through the :meth:`neo4j.Transaction.commit` and :meth:`neo4j.Transaction.rollback` methods.
Explicit transactions are most useful for applications that need to distribute Cypher execution across multiple functions for the same transaction.

.. code-block:: python

    def create_person(driver, name):
        with driver.session() as session:
            tx = session.begin_transaction()
            node_id = create_person_node(tx)
            set_person_name(tx, node_id, name)
            tx.commit()

    def create_person_node(tx):
        return tx.run("CREATE (a:Person)"
                      "RETURN id(a)", name=name).single().value()

    def set_person_name(tx, node_id, name):
        tx.run("MATCH (a:Person) WHERE id(a) = $id "
               "SET a.name = $name", id=node_id, name=name)

Transaction Functions
=====================
Transaction functions are the most powerful form of transaction, providing access mode override and retry capabilities.
These allow a function object representing the transactional unit of work to be passed as a parameter.
This function is called one or more times, within a configurable time limit, until it succeeds.
Results should be fully consumed within the function and only aggregate or status values should be returned.
Returning a live result object would prevent the driver from correctly managing connections and would break retry guarantees.

.. code-block:: python

    def create_person(tx, name):
        return tx.run("CREATE (a:Person {name:$name}) "
                      "RETURN id(a)", name=name).single().value()

    with driver.session() as session:
        node_id = session.write_transaction(create_person, "Alice")

To exert more control over how a transaction function is carried out, the :func:`neo4j.unit_of_work` decorator can be used.

.. autofunction:: neo4j.unit_of_work


Access modes
============

A session can be given a default `access mode` on construction.
This applies only in clustered environments and determines whether transactions carried out within that session should be routed to a `read` or `write` server by default.

Note that this mode is simply a default and not a constraint.
This means that transaction functions within a session can override the access mode passed to that session on construction.

.. note::
    The driver does not parse Cypher queries and cannot determine whether the access mode should be :code:`ACCESS_READ` or :code:`ACCESS_WRITE`.
    Since the access mode is not passed to the server, this can allow a :code:`ACCESS_WRITE` statement to be executed for a :code:`ACCESS_READ` call on a single instance.
    Clustered environments are not susceptible to this loophole as cluster roles prevent it.
    This behaviour should not be relied upon as the loophole may be closed in a future release.






******
Result
******

Every time a query is executed, a :class:`neo4j.Result` is returned.

This provides a handle to the result of the query, giving access to the records within it as well as the result metadata.

Results also contain a buffer that automatically stores unconsumed records when results are consumed out of order.

A :class:`neo4j.Result` is attached to an active connection, through a :class:`neo4j.Session`, until all its content has been buffered or consumed.

.. class:: neo4j.Result

    .. describe:: iter(result)

    .. automethod:: keys

    .. automethod:: consume

    .. automethod:: single

    .. automethod:: peek

    .. automethod:: graph

       **This is experimental.**


Graph
=====

.. class:: neo4j.graph.Graph

    A local, self-contained graph object that acts as a container for :class:`.Node` and :class:`neo4j.Relationship` instances.
    This is typically obtained via the :meth:`neo4j.Result.graph` method.

    .. autoattribute:: nodes

    .. autoattribute:: relationships

    .. automethod:: relationship_type

**This is experimental.**


******
Record
******

.. class:: neo4j.Record

    A :class:`neo4j.Record` is an immutable ordered collection of key-value
    pairs. It is generally closer to a :py:class:`namedtuple` than to a
    :py:class:`OrderedDict` inasmuch as iteration of the collection will
    yield values rather than keys.

    .. describe:: Record(iterable)

        Create a new record based on an dictionary-like iterable.
        This can be a dictionary itself, or may be a sequence of key-value pairs, each represented by a tuple.

    .. describe:: record == other

        Compare a record for equality with another value.
        The `other` value may be any `Sequence` or `Mapping`, or both.
        If comparing with a `Sequence`, the values are compared in order.
        If comparing with a `Mapping`, the values are compared based on their keys.
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

    .. describe:: record[key]

        Obtain a value from the record by key.
        This will raise a :exc:`KeyError` if the specified key does not exist.

    .. automethod:: get(key, default=None)

    .. automethod:: value(key=0, default=None)

    .. automethod:: index(key)

    .. automethod:: keys

    .. automethod:: values

    .. automethod:: items

    .. automethod:: data



*************
ResultSummary
*************

.. autoclass:: neo4j.ResultSummary
   :members:

SummaryCounters
===============

.. autoclass:: neo4j.SummaryCounters
    :members:


ServerInfo
==========

.. autoclass:: neo4j.ServerInfo
   :members:



***************
Core Data Types
***************

Cypher supports a set of core data types that all map to built-in types in Python.

These include the common `Boolean`, `Integer`, `Float` and `String` types as well as `List` and `Map` that can hold heterogenous collections of any other type.

The core types with their general mappings are listed below:

+------------------------+---------------------------------------------------------------------------------------------------------------------------+
| Cypher Type            | Python Type                                                                                                               |
+========================+===========================================================================================================================+
| Null                   | ``None``                                                                                                                  |
+------------------------+---------------------------------------------------------------------------------------------------------------------------+
| Boolean                | ``bool``                                                                                                                  |
+------------------------+---------------------------------------------------------------------------------------------------------------------------+
| Integer                | ``int``                                                                                                                   |
+------------------------+---------------------------------------------------------------------------------------------------------------------------+
| Float                  | ``float``                                                                                                                 |
+------------------------+---------------------------------------------------------------------------------------------------------------------------+
| String                 | ``str``                                                                                                                   |
+------------------------+---------------------------------------------------------------------------------------------------------------------------+
| Bytes :sup:`[1]`       | ``bytearray``                                                                                                             |
+------------------------+---------------------------------------------------------------------------------------------------------------------------+
| List                   | ``list``                                                                                                                  |
+------------------------+---------------------------------------------------------------------------------------------------------------------------+
| Map                    | ``dict``                                                                                                                  |
+------------------------+---------------------------------------------------------------------------------------------------------------------------+

.. Note::

   1. `Bytes` is not an actual Cypher type but is transparently passed through when used in parameters or query results.


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

.. class:: neo4j.graph.Node

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

    .. autoattribute:: labels

    .. automethod:: get

    .. automethod:: keys

    .. automethod:: values

    .. automethod:: items


Relationship
============

.. class:: neo4j.graph.Relationship

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

.. class:: neo4j.graph.Path

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

=============  ============================
Cypher Type    Python Type
=============  ============================
Point          :class:`neo4j.spatial.Point`
=============  ============================


Point
=====

.. autoclass:: neo4j.spatial.Point
   :members:


CartesianPoint
==============

.. autoclass:: neo4j.spatial.CartesianPoint
   :members:
   :inherited-members:


WGS84Point
==========

.. autoclass:: neo4j.spatial.WGS84Point
   :members:
   :inherited-members:



*******************
Temporal Data Types
*******************

Temporal data types are implemented by the ``neo4j.time``

These provide a set of types compliant with ISO-8601 and Cypher, which are similar to those found in the built-in ``datetime`` module.
Sub-second values are measured to nanosecond precision and the types are compatible with `pytz <http://pytz.sourceforge.net/>`_.

The table below shows the general mappings between Cypher and the temporal types provided by the driver.
In addition, the built-in temporal types can be passed as parameters and will be mapped appropriately.

=============  ============================  ==================================  ============
Cypher         Python driver type            Python built-in type                ``tzinfo``
=============  ============================  ==================================  ============
Date           :class:`neo4j.time.Date`      :class:`python:datetime.date`
Time           :class:`neo4j.time.Time`      :class:`python:datetime.time`       ``not None``
LocalTime      :class:`neo4j.time.Time`      :class:`python:datetime.time`       ``None``
DateTime       :class:`neo4j.time.DateTime`  :class:`python:datetime.datetime`   ``not None``
LocalDateTime  :class:`neo4j.time.DateTime`  :class:`python:datetime.datetime`   ``None``
Duration       :class:`neo4j.time.Duration`  :class:`python:datetime.timedelta`
=============  ============================  ==================================  ============


******
Errors
******


Connectivity errors
===================

.. class:: neo4j.exceptions.ServiceUnavailable

    Raised when a database server or service is not available.
    This may be due to incorrect configuration or could indicate a runtime failure of a database service that the driver is unable to route around.


Neo4j execution errors
=======================

.. class:: neo4j.exceptions.Neo4jError

    Raised when the Cypher engine returns an error to the client.
    There are many possible types of Cypher error, each identified by a unique `status code <https://neo4j.com/docs/status-codes/current/>`_.

    The three classifications of status code are supported by the three subclasses of :class:`.Neo4jError`, listed below:

.. autoclass:: neo4j.exceptions.ClientError

.. autoclass:: neo4j.exceptions.DatabaseError

.. autoclass:: neo4j.exceptions.TransientError


Internal Driver Errors
=======================

If users see an internal error, in particular a protocol error (BoltError*), they should open an issue on github.

https://github.com/neo4j/neo4j-python-driver/issues

Please provide details about your running environment,

Operating System:
Python Version:
Python Driver Version:
Neo4j Version:

the code block with a description that produced the error and the error message.


********
Bookmark
********

.. autoclass:: neo4j.Bookmark
    :members:

*****************
Other Information
*****************

* `Neo4j Documentation`_
* `The Neo4j Drivers Manual`_
* `Neo4j Quick Reference Card`_
* `Example Project`_
* `Driver Wiki`_ (includes change logs)
* `Migration Guide - Upgrade Neo4j drivers`_

.. _`Python Driver 1.7`: https://neo4j.com/docs/api/python-driver/1.7/
.. _`Neo4j Documentation`: https://neo4j.com/docs/
.. _`The Neo4j Drivers Manual`: https://neo4j.com/docs/driver-manual/current/
.. _`Neo4j Quick Reference Card`: https://neo4j.com/docs/cypher-refcard/current/
.. _`Example Project`: https://github.com/neo4j-examples/movies-python-bolt
.. _`Driver Wiki`: https://github.com/neo4j/neo4j-python-driver/wiki
.. _`Migration Guide - Upgrade Neo4j drivers`: https://neo4j.com/docs/migration-guide/4.0/upgrade-driver/
