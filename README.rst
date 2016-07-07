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

    from neo4j.v1 import GraphDatabase, basic_auth
    driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "neo4j"))
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


Documentation
=============

For more information such as manual, driver API documentations, changelogs, please find them in the wiki of this repo.