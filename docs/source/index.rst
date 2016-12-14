============================
Neo4j Bolt Driver for Python
============================

.. toctree::
   :maxdepth: 2


Session API
===========

.. autoclass:: neo4j.v1.GraphDatabase
   :members:

.. autoclass:: neo4j.v1.Driver
   :members:

.. autoclass:: neo4j.v1.Session
   :members:

.. autoclass:: neo4j.v1.Transaction
   :members:

.. autoclass:: neo4j.v1.Record
   :members:

.. autoclass:: neo4j.v1.StatementResult
   :members:


Encryption Settings
-------------------
.. py:attribute:: neo4j.v1.ENCRYPTION_OFF
.. py:attribute:: neo4j.v1.ENCRYPTION_ON
.. py:attribute:: neo4j.v1.ENCRYPTION_NON_LOCAL
.. py:attribute:: neo4j.v1.ENCRYPTION_DEFAULT


Trust Settings
--------------
.. py:attribute:: neo4j.v1.TRUST_ON_FIRST_USE
.. py:attribute:: neo4j.v1.TRUST_SIGNED_CERTIFICATES
.. py:attribute:: neo4j.v1.TRUST_DEFAULT


Query Summary Details
---------------------

.. autoclass:: neo4j.v1.summary.ResultSummary
   :members:

.. autoclass:: neo4j.v1.summary.SummaryCounters
   :members:


Exceptions
==========

.. autoclass:: neo4j.v1.ProtocolError
   :members:

.. autoclass:: neo4j.v1.CypherError
   :members:

.. autoclass:: neo4j.v1.ResultError
   :members:


Example
=======

.. code-block:: python

    from neo4j.v1 import GraphDatabase, basic_auth

    driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "password"))

    with driver.session() as session:

        with session.begin_transaction() as tx:
            session.run("MERGE (a:Person {name:'Alice'})")

        friends = ["Bob", "Carol", "Dave", "Eve", "Frank"]
        with session.begin_transaction() as tx:
            for friend in friends:
                tx.run("MATCH (a:Person {name:'Alice'}) "
                       "MERGE (a)-[:KNOWS]->(x:Person {name:{n}})", {"n": friend})

        for friend, in session.run("MATCH (a:Person {name:'Alice'})-[:KNOWS]->(x) RETURN x"):
            print('Alice says, "hello, %s"' % friend["name"])

    driver.close()


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

