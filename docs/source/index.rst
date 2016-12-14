****************************
Neo4j Bolt Driver for Python
****************************

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

        for record in session.run("MATCH (a:Person {name:'Alice'})-[:KNOWS]->(friend) RETURN friend"):
            print('Alice says, "hello, %s"' % record["friend"]["name"])

    driver.close()


Contents
========

.. toctree::
   :maxdepth: 1

   driver
   session
   types


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

