.. _breaking-changes:

****************
Breaking Changes
****************

Version Scheme Changes
======================

The version number has jumped from **Python Driver 1.7** to **Python Driver 4.0** to align with the Neo4j Database version scheme.


Namespace Changes
=================

.. code-block:: python

    import neo4j.v1

Has changed to

.. code-block:: python

    import neo4j


Secure Connection
=================

**Neo4j 4.0** is by default configured to use a **unsecured connection**.

The driver configuration argument :code:`encrypted` is by default set to :code:`False`.

**Note:** To be able to connect to **Neo4j 3.5** set :code:`encrypted=True` to have it configured as the default for that setup.

.. code-block:: python

    from neo4j import GraphDatabase

    driver = GraphDatabase("bolt://localhost:7687", auth=("neo4j", "password"), encrypted=True)
    driver.close()


Bookmark Changes
================

Introduced :class:`neo4j.Bookmark`


Exceptions Changes
==================

The exceptions in :code:`neo4j.exceptions` have been updated and there are internal exceptions starting with the naming :code:`Bolt` that should be propagated into the exceptions API.

See :ref:`errors-ref` for more about errors.

URI Scheme Changes
==================

**bolt+routing** has been renamed to **neo4j**.


Class Renaming Changes
======================

* :code:`BoltStatementResult` is now :code:`Result`
* :code:`StatementResultSummary` is now :code:`ResultSummary`
* :code:`Statement` is now :code:`Query`


Argument Renaming Changes
=========================

* :code:`statement` is now :code:`query`
* :code:`cypher` is now :code:`query`
* :code:`Session.run(cypher, ...` is now :code:`Session.run(query, ...`
* :code:`Transaction.run(statement, ...` is now :code:`Transaction.run(query, ...`
* :code:`StatementResultSummary.statement` is now :code:`ResultSummary.query`
* :code:`StatementResultSummary.statement_type` is now :code:`ResultSummary.query_type`
* :code:`StatementResultSummary.protocol_version` is now :code:`ResultSummary.server.protocol_version`


API Changes
=========================

* :code:`Result.summary()` have been replaced with :code:`Result.consume()`, this behaviour is to consume all remaining records in the buffer and returns the ResultSummary.
* :code:`Session.write_transaction` and :code:`Session.read_transaction` will start the retry timer after the first failed attempt.

Dependency Changes
==================

* The dependency :code:`neobolt` has been removed.
* The dependency :code:`neotime` has been removed.
* The :code:`pytz` is now a dependency.

Configuration Name Changes
==========================

* :code:`max_retry_time` is now :code:`max_transaction_retry_time`
