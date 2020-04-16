Testing
****************************
Neo4j Bolt Driver for Python
****************************

The official Neo4j driver for Python supports Neo4j 3.0 and above and Python versions 2.7, 3.4, 3.5, 3.6, and 3.7.

.. note::

    Python 2 support is deprecated and will be discontinued in the 2.x series driver releases.


.. warning::

    Connecting to Neo4j 4.X requires an unencrypted connection by default.


.. code-block:: python

    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"), encrypted=False)

Quick Example
=============

.. code-block:: python

    from neo4j import GraphDatabase

    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))

    def add_friend(tx, name, friend_name):
        tx.run("MERGE (a:Person {name: $name}) "
               "MERGE (a)-[:KNOWS]->(friend:Person {name: $friend_name})",
               name=name, friend_name=friend_name)

    def print_friends(tx, name):
        for record in tx.run("MATCH (a:Person)-[:KNOWS]->(friend) WHERE a.name = $name "
                             "RETURN friend.name ORDER BY friend.name", name=name):
            print(record["friend.name"])

    with driver.session() as session:
        session.write_transaction(add_friend, "Arthur", "Guinevere")
        session.write_transaction(add_friend, "Arthur", "Lancelot")
        session.write_transaction(add_friend, "Arthur", "Merlin")
        session.read_transaction(print_friends, "Arthur")


Installation
============

To install the latest stable version, use:

.. code:: bash

    pip install neo4j

.. note::

    Installation from the ``neo4j-driver`` package on PyPI is now deprecated and will be discontinued in the 2.x series driver releases.
    Please install from the ``neo4j`` package instead.

For the most up-to-date version (generally unstable), use:

.. code:: bash

    pip install git+https://github.com/neo4j/neo4j-python-driver.git#egg=neo4j


Other Information
=================

* `Neo4j Manual`_
* `Neo4j Quick Reference Card`_
* `Example Project`_
* `Driver Wiki`_ (includes change logs)

.. _`Neo4j Manual`: https://neo4j.com/docs/developer-manual/current/drivers/
.. _`Neo4j Quick Reference Card`: https://neo4j.com/docs/cypher-refcard/current/
.. _`Example Project`: https://github.com/neo4j-examples/movies-python-bolt
.. _`Driver Wiki`: https://github.com/neo4j/neo4j-python-driver/wiki
