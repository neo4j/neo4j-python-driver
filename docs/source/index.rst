********************************
Neo4j Bolt Driver |version| for Python
********************************

The Official Neo4j Driver for Python supports Neo4j 3.1 and above and requires Python version 2.7, 3.4, 3.5 or 3.6.


Quick Example
=============

.. code-block:: python

    from neo4j.v1 import GraphDatabase

    uri = "bolt://localhost:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))

    def print_friends_of(tx, name):
        for record in tx.run("MATCH (a:Person)-[:KNOWS]->(f) "
                             "WHERE a.name = {name} "
                             "RETURN f.name", name=name):
        print(record["f.name"])

    with driver.session() as session:
        session.read_transaction(print_friends_of, "Alice")


Installation
============

To install the latest stable version, use:

.. code:: bash

    pip install neo4j-driver

For the most up-to-date version (possibly unstable), use:

.. code:: bash

    pip install git+https://github.com/neo4j/neo4j-python-driver.git#egg=neo4j-driver


API Documentation
=================

.. toctree::
   :maxdepth: 1

   driver
   transactions
   results
   types/core
   types/graph
   types/spatial
   types/temporal
   errors


Other Information
=================

* `Neo4j Manual`_
* `Neo4j Quick Reference Card`_
* `Example Project`_
* `Driver Wiki`_ (includes change logs)

.. _`Neo4j Manual`: https://neo4j.com/docs/
.. _`Neo4j Quick Reference Card`: https://neo4j.com/docs/cypher-refcard/current/
.. _`Example Project`: https://github.com/neo4j-examples/movies-python-bolt
.. _`Driver Wiki`: https://github.com/neo4j/neo4j-python-driver/wiki
