============================
Neo4j Bolt Driver for Python
============================


Installation
============

To install the latest stable version, use:

.. code:: bash

    pip install neo4j-driver

For the most up-to-date version (possibly unstable), use:

.. code:: bash

    pip install git+https://github.com/neo4j/neo4j-python-driver.git#egg=neo4j-driver


Example Usage
=============

.. code:: python

    from neo4j.v1 import GraphDatabase
    driver = GraphDatabase.driver("bolt://localhost")
    session = driver.session()
    session.run("CREATE (a:Person {name:'Bob'})")
    result = session.run("MATCH (a:Person) RETURN a.name AS name")
    for record in result:
        print(record["name"])
    session.close()


Command Line
============

.. code:: bash

    python -m neo4j "CREATE (a:Person {name:'Alice'}) RETURN a, labels(a), a.name"
