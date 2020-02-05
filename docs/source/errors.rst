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