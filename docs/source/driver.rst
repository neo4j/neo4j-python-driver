**************
Driver Objects
**************

A `Driver` object holds the detail of a Neo4j database including server URIs, credentials and other configuration.
It also manages a pool of connections which are used to power :class:`.Session` instances.

The scheme of the URI passed to the `Driver` constructor determines the type of `Driver` object constructed.
For example, the ``bolt`` scheme generates a :class:`.DirectDriver` instance::

    from neo4j.v1 import GraphDatabase
    uri = "bolt://localhost:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))


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

.. autoclass:: neo4j.v1.AuthToken
   :members:

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

.. NOTE:: The option :attr:`.TRUST_CUSTOM_CA_SIGNED_CERTIFICATES` is not yet available for Python

.. NOTE:: The options :attr:`.TRUST_ON_FIRST_USE` and :attr:`.TRUST_SIGNED_CERTIFICATES` are deprecated.
