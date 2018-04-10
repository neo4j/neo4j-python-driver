**************
Driver Objects
**************

A `Driver` object holds the detail of a Neo4j database including server URIs, credentials and other configuration.
It also manages a pool of connections which are used to power :class:`.Session` instances.

The scheme of the URI passed to the `Driver` constructor determines the type of `Driver` object constructed.
The ``bolt`` scheme will generate a :class:`.DirectDriver` instance and the ``bolt+routing`` scheme a :class:`.RoutingDriver` instance.

`Driver` objects hold a connection pool, are thread-safe and are designed to live for the lifetime of an application.
Closing a driver will immediately shut down all connections in the pool.


.. code-block:: python

    from neo4j.v1 import GraphDatabase

    class Application(object):

        def __init__(self, uri, user, password)
            self.driver = GraphDatabase.driver(uri, auth=(user, password))

        def close(self):
            self.driver.close()


.. autoclass:: neo4j.v1.GraphDatabase
   :members:

.. autoclass:: neo4j.v1.Driver
   :members:
   :inherited-members:

.. autoclass:: neo4j.v1.DirectDriver
   :members:
   :inherited-members:

.. autoclass:: neo4j.v1.RoutingDriver
   :members:
   :inherited-members:

.. autofunction:: neo4j.v1.basic_auth

.. autofunction:: neo4j.v1.custom_auth


Trust Options
-------------
.. py:attribute:: neo4j.v1.TRUST_ALL_CERTIFICATES

   Trust any server certificate (default). This ensures that communication
   is encrypted but does not verify the server certificate against a
   certificate authority. This option is primarily intended for use with
   the default auto-generated server certificate.

.. py:attribute:: neo4j.v1.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES

   Trust server certificates that can be verified against the system
   certificate authority. This option is primarily intended for use with
   full certificates.
