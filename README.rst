****************************
Neo4j Bolt Driver for Python
****************************

The Official Neo4j Driver for Python supports Neo4j 3.0 and above and Python versions 2.7, 3.4, 3.5 and 3.6.


Quick Example
=============

.. code-block:: python

    from neo4j.v1 import GraphDatabase

    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))

    def add_friends(tx, name, friend_name):
        tx.run("MERGE (a:Person {name: $name}) "
               "MERGE (a)-[:KNOWS]->(friend:Person {name: $friend_name})",
               name=name, friend_name=friend_name)

    def print_friends(tx, name):
        for record in tx.run("MATCH (a:Person)-[:KNOWS]->(friend) WHERE a.name = $name "
                             "RETURN friend.name ORDER BY friend.name", name=name):
            print(record["friend.name"])

    with driver.session() as session:
        session.write_transaction(add_friends, "Arthur", "Guinevere")
        session.write_transaction(add_friends, "Arthur", "Lancelot")
        session.write_transaction(add_friends, "Arthur", "Merlin")
        session.read_transaction(print_friends, "Arthur")


Installation
============

To install the latest stable version, use:

.. code:: bash

    pip install neo4j-driver

For the most up-to-date version (generally unstable), use:

.. code:: bash

    pip install git+https://github.com/neo4j/neo4j-python-driver.git#egg=neo4j-driver


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
