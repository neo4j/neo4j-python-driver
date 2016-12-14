**************
Driver Objects
**************

A `Driver` object holds the detail of a Neo4j database including server URIs, credentials and other configuration.
It also manages a pool of connections which are used to power :class:`.Session` instances.

The scheme of the URI passed to the `Driver` constructor determines the type of `Driver` object constructed.
Two types are currently available: the :class:`.DirectDriver` and the :class:`.RoutingDriver`.
These are described in more detail below.


.. autoclass:: neo4j.v1.GraphDatabase
   :members:

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


Encryption Settings
-------------------
.. py:attribute:: neo4j.v1.ENCRYPTION_OFF
.. py:attribute:: neo4j.v1.ENCRYPTION_ON
.. py:attribute:: neo4j.v1.ENCRYPTION_DEFAULT


Trust Settings
--------------
.. py:attribute:: neo4j.v1.TRUST_ON_FIRST_USE
.. py:attribute:: neo4j.v1.TRUST_SIGNED_CERTIFICATES
.. py:attribute:: neo4j.v1.TRUST_ALL_CERTIFICATES
.. py:attribute:: neo4j.v1.TRUST_CUSTOM_CA_SIGNED_CERTIFICATES
.. py:attribute:: neo4j.v1.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES
.. py:attribute:: neo4j.v1.TRUST_DEFAULT
