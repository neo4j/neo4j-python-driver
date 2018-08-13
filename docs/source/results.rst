*****************
Consuming Results
*****************

Every time Cypher is executed, a :class:`.BoltStatementResult` is returned.
This provides a handle to the result of the query, giving access to the records within it as well as the result metadata.

Each result consists of header metadata, zero or more :class:`.Record` objects and footer metadata (the summary).
Results also contain a buffer that automatically stores unconsumed records when results are consumed out of order.
A :class:`.BoltStatementResult` is attached to an active connection, through a :class:`.Session`, until all its content has been buffered or consumed.

.. autoclass:: neo4j.BoltStatementResult
   :inherited-members:
   :members:

.. autoclass:: neo4j.Record
   :members:


Summary Details
---------------

.. autoclass:: neo4j.BoltStatementResultSummary
   :members:

.. autoclass:: neo4j.SummaryCounters
   :members:
