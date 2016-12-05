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

    driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "password"))

    with driver.session() as session:

        with session.begin_transaction() as write_tx:
            write_tx.run("CREATE (a:Person {name:{name},age:{age}})", name="Alice", age=33)
            write_tx.run("CREATE (a:Person {name:{name},age:{age}})", name="Bob", age=44)
            write_tx.success = True

        with session.begin_transaction() as read_tx:
            result = read_tx.run("MATCH (a:Person) RETURN a.name AS name, a.age AS age")
            for record in result:
                print("%s is %d years old" % (record["name"], record["age"]))

    driver.close()


Command Line
============

.. code:: bash

    python -m neo4j "CREATE (a:Person {name:'Alice'}) RETURN a, labels(a), a.name"


Documentation
=============

For more information such as manual, driver API documentations, changelogs, please find them in the wiki of this repo.

* `Driver Wiki`_
* `Neo4j Manual`_
* `Neo4j Refcard`_
* `Sample Project Using Driver`_

.. _`Sample Project Using Driver`: https://github.com/neo4j-examples/movies-python-bolt
.. _`Driver Wiki`: https://github.com/neo4j/neo4j-python-driver/wiki
.. _`Neo4j Manual`: https://neo4j.com/docs/
.. _`Neo4j Refcard`: https://neo4j.com/docs/cypher-refcard/current/