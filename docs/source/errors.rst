******
Errors
******


Connectivity errors
===================

.. class:: neo4j.exceptions.ServiceUnavailable

    Raised when a database server or service is not available.
    This may be due to incorrect configuration or could indicate a runtime failure of a database service that the driver is unable to route around.

.. class:: neo4j.exceptions.SecurityError

    Raised when a security issue occurs, generally around TLS or authentication.


Cypher execution errors
=======================

.. class:: neo4j.exceptions.Neo4jError

    Raised when the Cypher engine returns an error to the client.
    There are many possible types of Cypher error, each identified by a unique `status code <https://neo4j.com/docs/developer-manual/current/reference/status-codes/>`_.

    The three classifications of status code are supported by the three subclasses of :class:`.Neo4jError`, listed below:

.. autoclass:: neo4j.exceptions.ClientError

.. autoclass:: neo4j.exceptions.DatabaseError

.. autoclass:: neo4j.exceptions.TransientError


Low-level errors
================

.. class:: neo4j.exceptions.ProtocolError

    Raised when an unexpected or unsupported protocol event occurs.
    This error generally indicates a fault with the driver or server software.
    If you receive this error, please raise a GitHub issue or a support ticket.
