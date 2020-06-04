#############################
Neo4j Python Driver |version|
#############################

The Official Neo4j Driver for Python.

Neo4j versions supported:

* Neo4j 4.0
* Neo4j 3.5

Python versions supported:

* Python 3.8
* Python 3.7
* Python 3.6
* Python 3.5

.. note::
    Python 2.7 support has been dropped.

    The previous driver `Python Driver 1.7`_ supports older versions of python, the **Neo4j 4.0** will work in fallback mode with that driver.


******
Topics
******

+ :ref:`api-documentation`

+ :ref:`temporal-data-types`

+ :ref:`breaking-changes`


.. toctree::
   :hidden:

   api.rst
   temporal_types.rst
   breaking_changes.rst


*************
Quick Example
*************

.. code-block:: python

    from neo4j import GraphDatabase

    uri = "neo4j://localhost:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))

    def create_friend_of(tx, name, friend):
        tx.run("CREATE (a:Person)-[:KNOWS]->(f:Person {name: $friend}) "
               "WHERE a.name = $name "
               "RETURN f.name AS friend", name=name, friend=friend)

    with driver.session() as session:
        session.write_transaction(create_friend_of, "Alice", "Bob")

    with driver.session() as session:
        session.write_transaction(create_friend_of, "Alice", "Carl")

    driver.close()

.. code-block:: python

    from neo4j import GraphDatabase

    uri = "neo4j://localhost:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))

    def get_friends_of(tx, name):
        friends = []
        result = tx.run("MATCH (a:Person)-[:KNOWS]->(f) "
                             "WHERE a.name = $name "
                             "RETURN f.name AS friend", name=name):
        for record in result:
            friends.append(record["friend"])
        return friends

    with driver.session() as session:
        friends = session.read_transaction(get_friends_of, "Alice")
        for friend in friends:
            print(friend)

    driver.close()


************
Installation
************

To install the latest stable release, use:

.. code:: bash

    python -m pip install neo4j


To install the latest pre-release, use:

.. code:: bash

    python -m pip install --pre neo4j


.. note::

   It is always recommended to install python packages for user space in a virtual environment.


Virtual Environment
===================

To create a virtual environment named sandbox, use:

.. code:: bash

    python -m venv sandbox

To activate the virtual environment named sandbox, use:

.. code:: bash

    source sandbox/bin/activate

To deactivate the current active virtual environment, use:

.. code:: bash

    deactivate


*****************
Other Information
*****************

* `Neo4j Documentation`_
* `The Neo4j Drivers Manual`_
* `Neo4j Quick Reference Card`_
* `Example Project`_
* `Driver Wiki`_ (includes change logs)
* `Migration Guide - Upgrade Neo4j drivers`_

.. _`Python Driver 1.7`: https://neo4j.com/docs/api/python-driver/1.7/
.. _`Neo4j Documentation`: https://neo4j.com/docs/
.. _`The Neo4j Drivers Manual`: https://neo4j.com/docs/driver-manual/current/
.. _`Neo4j Quick Reference Card`: https://neo4j.com/docs/cypher-refcard/current/
.. _`Example Project`: https://github.com/neo4j-examples/movies-python-bolt
.. _`Driver Wiki`: https://github.com/neo4j/neo4j-python-driver/wiki
.. _`Migration Guide - Upgrade Neo4j drivers`: https://neo4j.com/docs/migration-guide/4.0/upgrade-driver/
