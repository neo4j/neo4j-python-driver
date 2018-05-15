.. py:currentmodule:: neotime

===================
Temporal Data Types
===================

Temporal data types are implemented by the `neotime <http://neotime.readthedocs.io/en/latest/>`_ package.
These provide a set of types compliant with ISO-8601 and Cypher, which are similar to those found in the built-in ``datetime`` module.
Sub-second values are measured to nanosecond precision and the types are compatible with `pytz <http://pytz.sourceforge.net/>`_.

The table below shows the general mappings between Cypher and the temporal types provided by the driver.
In addition, the built-in temporal types can be passed as parameters and will be mapped appropriately.

=============  =========================  ==================================  ============
Cypher         Python driver type         Python built-in type                ``tzinfo``
=============  =========================  ==================================  ============
Date           :class:`neotime:Date`      :class:`python:datetime.date`
Time           :class:`neotime:Time`      :class:`python:datetime.time`       ``not None``
LocalTime      :class:`neotime:Time`      :class:`python:datetime.time`       ``None``
DateTime       :class:`neotime:DateTime`  :class:`python:datetime.datetime`   ``not None``
LocalDateTime  :class:`neotime:DateTime`  :class:`python:datetime.datetime`   ``None``
Duration       :class:`neotime:Duration`  :class:`python:datetime.timedelta`
=============  =========================  ==================================  ============
