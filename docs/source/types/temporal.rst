.. _temporal-data-types:

*******************
Temporal Data Types
*******************

.. include:: _temporal_overview.rst


Constants
=========

.. autodata:: neo4j.time.MIN_YEAR

.. autodata:: neo4j.time.MAX_YEAR


Date
====

.. autoclass:: neo4j.time.Date

Class methods
-------------

.. automethod:: neo4j.time.Date.today

.. automethod:: neo4j.time.Date.utc_today

.. automethod:: neo4j.time.Date.from_timestamp

.. automethod:: neo4j.time.Date.utc_from_timestamp

.. automethod:: neo4j.time.Date.from_ordinal

.. automethod:: neo4j.time.Date.parse

.. automethod:: neo4j.time.Date.from_native

.. automethod:: neo4j.time.Date.from_clock_time

.. automethod:: neo4j.time.Date.is_leap_year

.. automethod:: neo4j.time.Date.days_in_year

.. automethod:: neo4j.time.Date.days_in_month


Class attributes
----------------

.. autoattribute:: neo4j.time.Date.min

.. autoattribute:: neo4j.time.Date.max

.. autoattribute:: neo4j.time.Date.resolution


Instance attributes
-------------------

.. autoattribute:: neo4j.time.Date.year

.. autoattribute:: neo4j.time.Date.month

.. autoattribute:: neo4j.time.Date.day

.. autoattribute:: neo4j.time.Date.year_month_day

.. autoattribute:: neo4j.time.Date.year_week_day

.. autoattribute:: neo4j.time.Date.year_day


Operations
----------

.. automethod:: neo4j.time.Date.__hash__

.. automethod:: neo4j.time.Date.__eq__

.. automethod:: neo4j.time.Date.__ne__

.. automethod:: neo4j.time.Date.__lt__

.. automethod:: neo4j.time.Date.__gt__

.. automethod:: neo4j.time.Date.__le__

.. automethod:: neo4j.time.Date.__ge__

.. automethod:: neo4j.time.Date.__add__

.. automethod:: neo4j.time.Date.__sub__

Instance methods
----------------

.. automethod:: neo4j.time.Date.replace

.. automethod:: neo4j.time.Date.time_tuple

.. automethod:: neo4j.time.Date.to_ordinal

.. automethod:: neo4j.time.Date.to_clock_time

.. automethod:: neo4j.time.Date.to_native

.. automethod:: neo4j.time.Date.weekday

.. automethod:: neo4j.time.Date.iso_weekday

.. automethod:: neo4j.time.Date.iso_calendar

.. automethod:: neo4j.time.Date.iso_format

.. automethod:: neo4j.time.Date.__repr__

.. automethod:: neo4j.time.Date.__str__

.. automethod:: neo4j.time.Date.__format__



Special values
--------------

.. autodata:: neo4j.time.ZeroDate


Time
====

.. autoclass:: neo4j.time.Time

Class methods
-------------

.. automethod:: neo4j.time.Time.now

.. automethod:: neo4j.time.Time.utc_now

.. automethod:: neo4j.time.Time.from_ticks

.. automethod:: neo4j.time.Time.from_native

.. automethod:: neo4j.time.Time.from_clock_time


Class attributes
----------------

.. autoattribute:: neo4j.time.Time.min

.. autoattribute:: neo4j.time.Time.max

.. autoattribute:: neo4j.time.Time.resolution


Instance attributes
-------------------

.. autoattribute:: neo4j.time.Time.ticks

.. autoattribute:: neo4j.time.Time.hour

.. autoattribute:: neo4j.time.Time.minute

.. autoattribute:: neo4j.time.Time.second

.. autoattribute:: neo4j.time.Time.nanosecond

.. autoattribute:: neo4j.time.Time.hour_minute_second_nanosecond

.. autoattribute:: neo4j.time.Time.tzinfo


Operations
----------

.. automethod:: neo4j.time.Time.__hash__

.. automethod:: neo4j.time.Time.__eq__

.. automethod:: neo4j.time.Time.__ne__

.. automethod:: neo4j.time.Time.__lt__

.. automethod:: neo4j.time.Time.__gt__

.. automethod:: neo4j.time.Time.__le__

.. automethod:: neo4j.time.Time.__ge__


Instance methods
----------------

.. automethod:: neo4j.time.Time.replace

.. automethod:: neo4j.time.Time.utc_offset

.. automethod:: neo4j.time.Time.dst

.. automethod:: neo4j.time.Time.tzname

.. automethod:: neo4j.time.Time.to_clock_time

.. automethod:: neo4j.time.Time.to_native

.. automethod:: neo4j.time.Time.iso_format

.. automethod:: neo4j.time.Time.__repr__

.. automethod:: neo4j.time.Time.__str__

.. automethod:: neo4j.time.Time.__format__


Special values
--------------

.. autodata:: neo4j.time.Midnight

.. autodata:: neo4j.time.Midday


DateTime
========

.. autoclass:: neo4j.time.DateTime


Class methods
-------------

.. automethod:: neo4j.time.DateTime.now

.. automethod:: neo4j.time.DateTime.utc_now

.. automethod:: neo4j.time.DateTime.from_timestamp

.. automethod:: neo4j.time.DateTime.utc_from_timestamp

.. automethod:: neo4j.time.DateTime.from_ordinal

.. automethod:: neo4j.time.DateTime.combine

.. automethod:: neo4j.time.DateTime.from_native

.. automethod:: neo4j.time.DateTime.from_clock_time


Class attributes
----------------

.. autoattribute:: neo4j.time.DateTime.min

.. autoattribute:: neo4j.time.DateTime.max

.. autoattribute:: neo4j.time.DateTime.resolution


Instance attributes
-------------------

.. autoattribute:: neo4j.time.DateTime.year

.. autoattribute:: neo4j.time.DateTime.month

.. autoattribute:: neo4j.time.DateTime.day

.. autoattribute:: neo4j.time.DateTime.year_month_day

.. autoattribute:: neo4j.time.DateTime.year_week_day

.. autoattribute:: neo4j.time.DateTime.year_day

.. autoattribute:: neo4j.time.DateTime.hour

.. autoattribute:: neo4j.time.DateTime.minute

.. autoattribute:: neo4j.time.DateTime.second

.. autoattribute:: neo4j.time.DateTime.nanosecond

.. autoattribute:: neo4j.time.DateTime.tzinfo

.. autoattribute:: neo4j.time.DateTime.hour_minute_second_nanosecond


Operations
----------

.. automethod:: neo4j.time.DateTime.__hash__

.. automethod:: neo4j.time.DateTime.__eq__

.. automethod:: neo4j.time.DateTime.__ne__

.. automethod:: neo4j.time.DateTime.__lt__

.. automethod:: neo4j.time.DateTime.__gt__

.. automethod:: neo4j.time.DateTime.__le__

.. automethod:: neo4j.time.DateTime.__ge__

.. automethod:: neo4j.time.DateTime.__add__

.. automethod:: neo4j.time.DateTime.__sub__


Instance methods
----------------

.. automethod:: neo4j.time.DateTime.date

.. automethod:: neo4j.time.DateTime.time

.. automethod:: neo4j.time.DateTime.timetz

.. automethod:: neo4j.time.DateTime.replace

.. automethod:: neo4j.time.DateTime.as_timezone

.. automethod:: neo4j.time.DateTime.utc_offset

.. automethod:: neo4j.time.DateTime.dst

.. automethod:: neo4j.time.DateTime.tzname

.. automethod:: neo4j.time.DateTime.to_ordinal

.. automethod:: neo4j.time.DateTime.to_clock_time

.. automethod:: neo4j.time.DateTime.to_native

.. automethod:: neo4j.time.DateTime.weekday

.. automethod:: neo4j.time.DateTime.iso_weekday

.. automethod:: neo4j.time.DateTime.iso_calendar

.. automethod:: neo4j.time.DateTime.iso_format

.. automethod:: neo4j.time.DateTime.__repr__

.. automethod:: neo4j.time.DateTime.__str__

.. automethod:: neo4j.time.DateTime.__format__


Special values
--------------

.. autodata:: neo4j.time.Never

.. autodata:: neo4j.time.UnixEpoch


Duration
========

.. autoclass:: neo4j.time.Duration


Class attributes
----------------

.. autoattribute:: neo4j.time.Duration.min

.. autoattribute:: neo4j.time.Duration.max


Instance attributes
-------------------

.. autoattribute:: neo4j.time.Duration.months

.. autoattribute:: neo4j.time.Duration.days

.. autoattribute:: neo4j.time.Duration.seconds

.. autoattribute:: neo4j.time.Duration.nanoseconds

.. autoattribute:: neo4j.time.Duration.years_months_days

.. autoattribute:: neo4j.time.Duration.hours_minutes_seconds_nanoseconds


Operations
----------

.. automethod:: neo4j.time.Duration.__bool__

.. automethod:: neo4j.time.Duration.__add__

.. automethod:: neo4j.time.Duration.__sub__

.. automethod:: neo4j.time.Duration.__mul__

.. automethod:: neo4j.time.Duration.__truediv__

.. automethod:: neo4j.time.Duration.__floordiv__

.. automethod:: neo4j.time.Duration.__mod__

.. automethod:: neo4j.time.Duration.__divmod__

.. automethod:: neo4j.time.Duration.__pos__

.. automethod:: neo4j.time.Duration.__neg__

.. automethod:: neo4j.time.Duration.__abs__

.. automethod:: neo4j.time.Duration.__repr__

.. automethod:: neo4j.time.Duration.__str__


Instance methods
----------------

.. automethod:: neo4j.time.Duration.iso_format
