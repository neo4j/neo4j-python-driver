****************************
Neo4j Bolt Driver for Python
****************************

This repository contains the official Neo4j driver for Python.
Each driver release (from 4.0 upwards) is built specifically to work with a corresponding Neo4j release, i.e. that with the same `major.minor` version number.
These drivers will also be compatible with the previous Neo4j release, although new server features will not be available.

+ Python 3.9 supported.
+ Python 3.8 supported.
+ Python 3.7 supported.
+ Python 3.6 supported.

Python 2.7 support has been dropped as of the Neo4j 4.0 release.


Installation
============

To install the latest stable version, use:

.. code:: bash

    pip install neo4j


Quick Example
=============

.. code-block:: python

    from neo4j import GraphDatabase

    driver = GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j", "password"))

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

    driver.close()


Connection Settings Breaking Change
===================================

+ The driver’s default configuration for encrypted is now false (meaning that driver will only attempt plain text connections by default).

+ Connections to encrypted services (such as Neo4j Aura) should now explicitly be set to encrypted.

+ When encryption is explicitly enabled, the default trust mode is to trust the CAs that are trusted by operating system and use hostname verification.

+ This means that encrypted connections to servers holding self-signed certificates will now fail on certificate verification by default.

+ Using the new `neo4j+ssc` scheme will allow to connect to servers holding self-signed certificates and not use hostname verification.

+ The `neo4j://` scheme replaces `bolt+routing://` and can be used for both clustered and single-instance configurations with Neo4j 4.0.



See, https://neo4j.com/docs/migration-guide/4.0/upgrade-driver/#upgrade-driver-breakingchanges


See, https://neo4j.com/docs/driver-manual/current/client-applications/#driver-connection-uris for changes in default security settings between 3.x and 4.x


Connecting with Python Driver 4.x to Neo4j 3.5
----------------------------------------------

Using the Python Driver 4.x and connecting to Neo4j 3.5 with default connection settings for Neo4j 3.5.

.. code-block:: python

    # the preferred form

    driver = GraphDatabase.driver("neo4j+ssc://localhost:7687", auth=("neo4j", "password"))

    # is equivalent to

    driver = GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j", "password"), encrypted=True, trust=False)


Connecting with Python Driver 1.7 to Neo4j 4.x
----------------------------------------------

Using the Python Driver 1.7 and connecting to Neo4j 4.x with default connection settings for Neo4j 4.x.

.. code-block:: python

    driver = GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j", "password"), encrypted=False)



Other Information
=================

* `The Neo4j Operations Manual`_
* `The Neo4j Drivers Manual`_
* `Python Driver API Documentation`_
* `Neo4j Cypher Refcard`_
* `Example Project`_
* `Driver Wiki`_ (includes change logs)
* `Neo4j 4.0 Migration Guide`_

.. _`The Neo4j Operations Manual`: https://neo4j.com/docs/operations-manual/current/
.. _`The Neo4j Drivers Manual`: https://neo4j.com/docs/driver-manual/current/
.. _`Python Driver API Documentation`: https://neo4j.com/docs/api/python-driver/current/
.. _`Neo4j Cypher Refcard`: https://neo4j.com/docs/cypher-refcard/current/
.. _`Example Project`: https://github.com/neo4j-examples/movies-python-bolt
.. _`Driver Wiki`: https://github.com/neo4j/neo4j-python-driver/wiki
.. _`Neo4j 4.0 Migration Guide`: https://neo4j.com/docs/migration-guide/4.0/
