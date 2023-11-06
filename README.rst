****************************
Neo4j Bolt Driver for Python
****************************

This repository contains the official Neo4j driver for Python.

Starting with 5.0, the Neo4j Drivers will be moving to a monthly release
cadence. A minor version will be released on the last Friday of each month so
as to maintain versioning consistency with the core product (Neo4j DBMS) which
has also moved to a monthly cadence.

As a policy, patch versions will not be released except on rare occasions. Bug
fixes and updates will go into the latest minor version and users should
upgrade to that. Driver upgrades within a major version will never contain
breaking API changes.

See also: https://neo4j.com/developer/kb/neo4j-supported-versions/

+ Python 3.12 supported.
+ Python 3.11 supported.
+ Python 3.10 supported.
+ Python 3.9 supported.
+ Python 3.8 supported.
+ Python 3.7 supported.


Installation
============

To install the latest stable version, use:

.. code:: bash

    pip install neo4j


.. TODO: 7.0 - remove this note

.. note::

    ``neo4j-driver`` is the old name for this package. It is now deprecated and
    and will receive no further updates starting with 6.0.0. Make sure to
    install ``neo4j`` as shown above.


Quick Example
=============

.. code-block:: python

    from neo4j import GraphDatabase, RoutingControl


    URI = "neo4j://localhost:7687"
    AUTH = ("neo4j", "password")


    def add_friend(driver, name, friend_name):
        driver.execute_query(
            "MERGE (a:Person {name: $name}) "
            "MERGE (friend:Person {name: $friend_name}) "
            "MERGE (a)-[:KNOWS]->(friend)",
            name=name, friend_name=friend_name, database_="neo4j",
        )


    def print_friends(driver, name):
        records, _, _ = driver.execute_query(
            "MATCH (a:Person)-[:KNOWS]->(friend) WHERE a.name = $name "
            "RETURN friend.name ORDER BY friend.name",
            name=name, database_="neo4j", routing_=RoutingControl.READ,
        )
        for record in records:
            print(record["friend.name"])


    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        add_friend(driver, "Arthur", "Guinevere")
        add_friend(driver, "Arthur", "Lancelot")
        add_friend(driver, "Arthur", "Merlin")
        print_friends(driver, "Arthur")


Further Information
===================

* `The Neo4j Operations Manual`_ (docs on how to run a Neo4j server)
* `The Neo4j Python Driver Manual`_ (good introduction to this driver)
* `Python Driver API Documentation`_ (full API documentation for this driver)
* `Neo4j Cypher Cheat Sheet`_ (summary of Cypher syntax - Neo4j's graph query language)
* `Example Project`_ (small web application using this driver)
* `GraphAcademy`_ (interactive, free online trainings for Neo4j)
* `Driver Wiki`_ (includes change logs)
* `Neo4j Migration Guide`_

.. _`The Neo4j Operations Manual`: https://neo4j.com/docs/operations-manual/current/
.. _`The Neo4j Python Driver Manual`: https://neo4j.com/docs/python-manual/current/
.. _`Python Driver API Documentation`: https://neo4j.com/docs/api/python-driver/current/
.. _`Neo4j Cypher Cheat Sheet`: https://neo4j.com/docs/cypher-cheat-sheet/
.. _`Example Project`: https://github.com/neo4j-examples/movies-python-bolt
.. _`GraphAcademy`: https://graphacademy.neo4j.com/categories/python/
.. _`Driver Wiki`: https://github.com/neo4j/neo4j-python-driver/wiki
.. _`Neo4j Migration Guide`: https://neo4j.com/docs/migration-guide/current/
