============================
Neo4j Bolt Driver for Python
============================

.. toctree::
   :maxdepth: 2


Session API
===========

.. autoclass:: neo4j.GraphDatabase
   :members:

.. autoclass:: neo4j.Driver
   :members:

.. autoclass:: neo4j.Session
   :members:

.. autoclass:: neo4j.Transaction
   :members:

.. autoclass:: neo4j.Record
   :members:


Exceptions
==========

.. autoclass:: neo4j.CypherError
   :members:

.. autoclass:: neo4j.ProtocolError
   :members:


Example
=======

.. code-block:: python

    from neo4j import GraphDatabase

    driver = GraphDatabase.driver("bolt://localhost")
    session = driver.session()

    session.run("MERGE (a:Person {name:'Alice'})")

    friends = ["Bob", "Carol", "Dave", "Eve", "Frank"]
    with session.new_transaction() as tx:
        for friend in friends:
            tx.run("MATCH (a:Person {name:'Alice'}) "
                   "MERGE (a)-[:KNOWS]->(x:Person {name:{n}})", {"n": friend})
        tx.success = True

    for friend, in session.run("MATCH (a:Person {name:'Alice'})-[:KNOWS]->(x) RETURN x"):
        print('Alice says, "hello, %s"' % friend["name"])

    session.close()


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

