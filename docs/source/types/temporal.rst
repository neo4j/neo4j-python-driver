===================
Temporal Data Types
===================

Temporal data types are implemented by the ``neo4j.time``

These provide a set of types compliant with ISO-8601 and Cypher, which are similar to those found in the built-in ``datetime`` module.
Sub-second values are measured to nanosecond precision and the types are compatible with `pytz <http://pytz.sourceforge.net/>`_.

The table below shows the general mappings between Cypher and the temporal types provided by the driver.
In addition, the built-in temporal types can be passed as parameters and will be mapped appropriately.

=============  ============================  ==================================  ============
Cypher         Python driver type            Python built-in type                ``tzinfo``
=============  ============================  ==================================  ============
Date           :class:`neo4j.time:Date`      :class:`python:datetime.date`
Time           :class:`neo4j.time:Time`      :class:`python:datetime.time`       ``not None``
LocalTime      :class:`neo4j.time:Time`      :class:`python:datetime.time`       ``None``
DateTime       :class:`neo4j.time:DateTime`  :class:`python:datetime.datetime`   ``not None``
LocalDateTime  :class:`neo4j.time:DateTime`  :class:`python:datetime.datetime`   ``None``
Duration       :class:`neo4j.time:Duration`  :class:`python:datetime.timedelta`
=============  ============================  ==================================  ============
