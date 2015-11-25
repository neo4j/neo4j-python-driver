============================
Neo4j Bolt Driver for Python
============================

.. code:: python

    from neo4j.v1 import GraphDatabase
    driver = GraphDatabase.driver("bolt://localhost")
    session = driver.session()
    session.run("CREATE (a:Person {name:'Bob'})")
    for name, in session.run("MATCH (a:Person) RETURN a.name AS name"):
        print(name)
    session.close()


Command Line
============

.. code:: bash

    python -m neo4j "CREATE (a:Person {name:'Alice'}) RETURN a, labels(a), a.name"
