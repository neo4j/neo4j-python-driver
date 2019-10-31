**************************************
Neo4j Bolt Driver |version| for Python
**************************************

The Official Neo4j Driver for Python supports Neo4j 3.2 and above and requires Python version 2.7 or 3.4+.
Note that support for Python 2.7 will be removed in the 2.0 driver.


Quick Example
=============

.. code-block:: python

    from neo4j import GraphDatabase

    uri = "bolt://localhost:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))

    def print_friends_of(tx, name):
        for record in tx.run("MATCH (a:Person)-[:KNOWS]->(f) "
                             "WHERE a.name = {name} "
                             "RETURN f.name", name=name):
        print(record["f.name"])

    with driver.session() as session:
        session.read_transaction(print_friends_of, "Alice")


.. note::

    While imports from ``neo4j.v1`` still work, these will be removed in the 2.0 driver.
    It is therefore recommended to change all imports from ``neo4j.v1`` to ``neo4j``.


Installation
============

To install the latest stable driver release, use:

.. code:: bash

    pip install neo4j

.. note::

    The driver is currently released under two package names on `PyPI <https://pypi.org/>`_: ``neo4j`` and ``neo4j-driver``.
    Installing from ``neo4j`` is recommended since ``neo4j-driver`` will be removed in a future release.


API Documentation
=================

.. toctree::
   :maxdepth: 1

   aio


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
