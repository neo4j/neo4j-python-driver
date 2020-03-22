==============
Driver Objects
==============

Every Neo4j-backed application will require a :class:`neo4j.Driver` object.
This object holds the details required to establish connections with a Neo4j database, including server URIs, credentials and other configuration.
:class:`neo4j.Driver` objects hold a connection pool from which :class:`neo4j.Session` objects can borrow connections.
Closing a driver will immediately shut down all connections in the pool.

Construction
============

:class:`neo4j.Driver` construction can either be carried out directly or via a `classmethod` on the :class:`neo4j.GraphDatabase` class.

.. autoclass:: neo4j.GraphDatabase
   :members: driver

.. autoclass:: neo4j.Driver()
   :members: session, close, closed


URI
===

On construction, the `scheme` of the URI determines the type of :class:`neo4j.Driver` object created.

Example URI::

    uri = bolt://localhost:7676

Example URI::

    uri = neo4j://localhost:7676

Each supported scheme maps to a particular :class:`neo4j.Driver` subclass that implements a specific behaviour.

The alternative behaviours are described in the subsections below.


BoltDriver
----------

URI schemes:
    ``bolt``, ``bolt+ssc``, ``bolt+s``
Driver subclass:
    :class:`neo4j.BoltDriver`

.. autoclass:: neo4j.BoltDriver


Neo4jDriver
------------

URI schemes:
    ``neo4j``, ``neo4j+ssc``, ``neo4j+s``
Driver subclass:
    :class:`neo4j.Neo4jDriver`

.. autoclass:: neo4j.Neo4jDriver


Configuration
=============

Additional configuration, including authentication details, can be provided via the :class:`neo4j.Driver` constructor.

``auth``
--------

An authentication token for the server.
For basic auth, this can be a simple tuple, for example ``("neo4j", "password")``.
Alternatively, one of the auth token functions can be used.

.. autofunction:: neo4j.basic_auth

.. autofunction:: neo4j.custom_auth

``encrypted``
-------------

A boolean indicating whether or not TLS should be used for connections.

:Type: ``bool``
:Default: :py:const:`True`


``trust``
---------

The trust level for certificates received from the server during TLS negotiation.
This setting does not have any effect if ``encrypted`` is set to :py:const:`False`.


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

``user_agent``
--------------

A custom user agent string, if required.
The driver will generate a user agent if none is supplied.

``max_connection_lifetime``
---------------------------

The maximum time for which a connection can exist before being closed on release, instead of returned to the pool.

``max_connection_pool_size``
----------------------------

The maximum number of connections managed by the connection pool

``connection_acquisition_timeout``
----------------------------------

The maximum time to wait for a connection to be acquired from the pool.

``keep_alive``
--------------

Flag to indicate whether or not the TCP `KEEP_ALIVE` setting should be used.

``max_retry_time``
------------------

The maximum time to allow for retries to be attempted when using transaction functions.
After this time, no more retries will be attempted.
This setting does not terminate running queries.

``resolver``
------------

A custom resolver function to resolve host and port values ahead of DNS resolution.
This function is called with a 2-tuple of (host, port) and should return an iterable of tuples as would be returned from ``getaddrinfo``.
If no custom resolver function is supplied, the internal resolver moves straight to regular DNS resolution.

For example::

    def my_resolver(socket_address):
         if socket_address == ("foo", 9999):
            yield "::1", 7687
            yield "127.0.0.1", 7687
         else:
            from socket import gaierror
            raise gaierror("Unexpected socket address %r" % socket_address)

     driver = GraphDatabase.driver("neo4j://foo:9999", auth=("neo4j", "password"), resolver=my_resolver)



Object Lifetime
===============

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
