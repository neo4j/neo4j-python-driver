==============
Driver Objects
==============

Every Neo4j-backed application will require a :class:`.Driver` object.
This object holds the details required to establish connections with a Neo4j database, including server URIs, credentials and other configuration.
:class:`.Driver` objects hold a connection pool from which :class:`.Session` objects can borrow connections.
Closing a driver will immediately shut down all connections in the pool.

Construction
============

:class:`.Driver` construction can either be carried out directly or via a `classmethod` on the :class:`.GraphDatabase` class.

.. autoclass:: neo4j.v1.GraphDatabase
   :members:

.. autoclass:: neo4j.v1.Driver(uri, **config)
   :members:


URI
===

On construction, the scheme of the URI determines the type of :class:`.Driver` object created.
Each supported scheme maps to a particular :class:`.Driver` subclass that implements a specific behaviour.
The remainder of the URI should be considered subclass-specific.

The alternative behaviours are described in the subsections below.


Bolt Direct
-----------

URI scheme:
    ``bolt``
Driver subclass:
    :class:`.DirectDriver`

A Bolt :class:`.DirectDriver` is used to target a single machine.
This may be a standalone server or could be a specific member of a cluster.

Connections established by a :class:`.DirectDriver` are always made to the exact host and port detailed in the URI.

.. autoclass:: neo4j.v1.DirectDriver
   :members:
   :inherited-members:


Bolt Routing
------------

URI scheme:
    ``bolt+routing``
Driver subclass:
    :class:`.RoutingDriver`

.. autoclass:: neo4j.v1.RoutingDriver
   :members:
   :inherited-members:


Configuration
=============

Additional configuration, including authentication details, can be provided via the :class:`.Driver` constructor.

``auth``
--------

An authentication token for the server.
For basic auth, this can be a simple tuple, for example ``("neo4j", "password")``.
Alternatively, one of the auth token functions can be used.

.. autofunction:: neo4j.v1.basic_auth

.. autofunction:: neo4j.v1.custom_auth

``encrypted``
-------------

A boolean indicating whether or not TLS should be used for connections.
Defaults to :py:const:`True` if TLS is available.

``trust``
---------

The trust level for certificates received from the server during TLS negotiation.
This setting does not have any effect if ``encrypted`` is set to :py:const:`False`.

.. py:attribute:: neo4j.v1.TRUST_ALL_CERTIFICATES

   Trust any server certificate (default). This ensures that communication
   is encrypted but does not verify the server certificate against a
   certificate authority. This option is primarily intended for use with
   the default auto-generated server certificate.

.. py:attribute:: neo4j.v1.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES

   Trust server certificates that can be verified against the system
   certificate authority. This option is primarily intended for use with
   full certificates.

``der_encoded_server_certificate``
----------------------------------

The server certificate in DER format, if required.

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

``connection_timeout``
----------------------

The maximum time to wait for a new connection to be established.

``keep_alive``
--------------

Flag to indicate whether or not the TCP `KEEP_ALIVE` setting should be used.

``max_retry_time``
------------------

The maximum time to allow for retries to be attempted when using transaction functions.
After this time, no more retries will be attempted.
This setting does not terminate running queries.



Object Lifetime
===============

For general applications, it is recommended to create one top-level :class:`.Driver` object that lives for the lifetime of the application.
For example:

.. code-block:: python

    from neo4j.v1 import GraphDatabase

    class Application(object):

        def __init__(self, uri, user, password)
            self.driver = GraphDatabase.driver(uri, auth=(user, password))

        def close(self):
            self.driver.close()

Connection details held by the :class:`.Driver` are immutable.
Therefore if, for example, a password is changed, a replacement :class:`.Driver` object must be created.
More than one :class:`.Driver` may be required if connections to multiple databases, or connections as multiple users, are required.

:class:`.Driver` objects are thread-safe but cannot be shared across processes.
Therefore, ``multithreading`` should generally be preferred over ``multiprocessing`` for parallel database access.
If using ``multiprocessing`` however, each process will require its own :class:`.Driver` object.
