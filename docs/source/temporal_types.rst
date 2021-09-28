.. _temporal-data-types:

*******************
Temporal Data Types
*******************

.. include:: types/temporal.rst


Constants
=========

.. autodata:: neo4j.time.MIN_YEAR

.. autodata:: neo4j.time.MAX_YEAR


Date
====

A :class:`neo4j.time.Date` object represents a date in the `proleptic Gregorian Calendar <https://en.wikipedia.org/wiki/Proleptic_Gregorian_calendar>`_.

Years between `0001` and `9999` are supported, with additional support for the "zero date" used in some contexts.

Each date is based on a proleptic Gregorian ordinal, which models 1 Jan 0001 as `day 1` and counts each subsequent day up to, and including, 31 Dec 9999.
The standard `year`, `month` and `day` value of each date is also available.

Internally, the day of the month is always stored as-is, with the exception of the last three days of that month.
These are always stored as -1, -2 and -3 (counting from the last day).
This system allows some temporal arithmetic (particularly adding or subtracting months) to produce a more desirable outcome than would otherwise be produced.
Externally, the day number is always the same as would be written on a calendar.


Constructors and other class methods
------------------------------------

.. class:: neo4j.time.Date(year, month, day)

    Construct a new :class:`neo4j.time.Date` object.
    All arguments are required and should be integers.
    For general dates, the following ranges are supported:

    =========  ========================  ===================================
    Argument   Minimum                   Maximum
    ---------  ------------------------  -----------------------------------
    ``year``   :attr:`.MIN_YEAR` (0001)  :attr:`.MAX_YEAR` (9999)
    ``month``  1                         12
    ``day``    1                         :attr:`Date.days_in_month(year, month) <Date.days_in_month>`
    =========  ========================  ===================================

    A zero date can also be acquired by passing all zeroes to the :class:`neo4j.time.Date` constructor or by using the :attr:`.ZeroDate` constant.

.. classmethod:: Date.today()

    :raises OverflowError: if the timestamp is out of the range of values supported by the platform C localtime() function. It’s common for this to be restricted to years from 1970 through 2038.

.. classmethod:: Date.utc_today()

    Return the current :class:`.Date` according to UTC.

.. classmethod:: Date.from_timestamp(timestamp, tz=None)

    :raises OverflowError: if the timestamp is out of the range of values supported by the platform C localtime() function. It’s common for this to be restricted to years from 1970 through 2038.

.. classmethod:: Date.utc_from_timestamp(timestamp)

.. classmethod:: Date.from_ordinal(ordinal)

    Construct and return a :class:`.Date` from a proleptic Gregorian ordinal.
    This is simply an integer value that corresponds to a day, starting with `1` for 1 Jan 0001.

.. classmethod:: Date.parse(s)

.. classmethod:: Date.from_native(date)

.. classmethod:: Date.from_clock_time(cls, t, epoch):

.. classmethod:: Date.is_leap_year(year)

    Return a `bool` value that indicates whether or not `year` is a leap year.

.. classmethod:: Date.days_in_year(year)

    Return the number of days in `year`.

.. classmethod:: Date.days_in_month(year, month)

    Return the number of days in `month` of `year`.


Class attributes
----------------

.. attribute:: Date.min

.. attribute:: Date.max

.. attribute:: Date.resolution


Instance attributes
-------------------

.. attribute:: d.year

.. attribute:: d.month

.. attribute:: d.day

.. attribute:: d.year_month_day

.. attribute:: d.year_week_day

.. attribute:: d.year_day

    Return a 2-tuple of year and day number.
    This is the number of the day relative to the start of the year, with `1 Jan` corresponding to `1`.


Operations
----------


Instance methods
----------------

.. method:: d.replace(year=self.year, month=self.month, day=self.day)

    Return a :class:`.Date` with one or more components replaced with new values.

.. method:: d.time_tuple()

.. method:: d.to_ordinal()

.. method:: d.weekday()

.. method:: d.iso_weekday()

.. method:: d.iso_calendar()

.. method:: d.iso_format()

.. method:: d.__repr__()

.. method:: d.__str__()

.. method:: d.__format__()


Special values
--------------

.. attribute:: ZeroDate

    A :class:`neo4j.time.Date` instance set to `0000-00-00`.
    This has an ordinal value of `0`.


Time
====

The :class:`neo4j.time.Time` class is a nanosecond-precision drop-in replacement for the standard library :class:`datetime.time` class.

A high degree of API compatibility with the standard library classes is provided.

:class:`neo4j.time.Time` objects introduce the concept of `ticks`.
This is simply a count of the number of seconds since midnight, in many ways analogous to the :class:`neo4j.time.Date` ordinal.
`Ticks` values can be fractional, with a minimum value of `0` and a maximum of `86399.999999999`.


Constructors and other class methods
------------------------------------

.. class:: neo4j.time.Time(hour, minute, second, tzinfo=None)

.. classmethod:: Time.now()

    :raises OverflowError: if the timestamp is out of the range of values supported by the platform C localtime() function. It’s common for this to be restricted to years from 1970 through 2038.

.. classmethod:: Time.utc_now()

.. classmethod:: Time.from_ticks(ticks)

.. classmethod:: Time.from_native(time)

.. classmethod:: Time.from_clock_time(t, epoch)


Class attributes
----------------

.. attribute:: Time.min

.. attribute:: Time.max

.. attribute:: Time.resolution


Instance attributes
-------------------

.. attribute:: t.ticks

.. attribute:: t.hour

.. attribute:: t.minute

.. attribute:: t.second

.. attribute:: t.hour_minute_second

.. attribute:: t.tzinfo


Operations
----------

.. describe:: hash(t)

.. describe:: t1 == t2

.. describe:: t1 != t2

.. describe:: t1 < t2

.. describe:: t1 > t2

.. describe:: t1 <= t2

.. describe:: t1 >= t2

.. describe:: t1 + timedelta -> t2
              t1 + duration -> t2

.. describe:: t1 - timedelta -> t2
              t1 - duration -> t2

.. describe:: t1 - t2 -> timedelta


Instance methods
----------------

.. method:: t.replace(hour=self.hour, minute=self.minute, second=self.second, tzinfo=self.tzinfo)

    Return a :class:`.Time` with one or more components replaced with new values.

.. method:: t.utc_offset()

.. method:: t.dst()

.. method:: t.tzname()

.. method:: t.iso_format()

.. method:: t.__repr__()

.. method:: t.__str__()

.. method:: t.__format__()


Special values
--------------

.. attribute:: Midnight

    A :class:`.Time` instance set to `00:00:00`.
    This has a :attr:`ticks <.time.ticks>` value of `0`.

.. attribute:: Midday

    A :class:`.Time` instance set to `12:00:00`.
    This has a :attr:`ticks <.time.ticks>` value of `43200`.


LocalTime
---------

When tzinfo is set to ``None``



DateTime
========

The :class:`neo4j.time.DateTime` class is a nanosecond-precision drop-in replacement for the standard library :class:`datetime.datetime` class.

As such, it contains both :class:`neo4j.time.Date` and :class:`neo4j.time.Time` information and draws functionality from those individual classes.

A :class:`.DateTime` object is fully compatible with the Python time zone library `pytz <http://pytz.sourceforge.net/>`_.
Functions such as `normalize` and `localize` can be used in the same way as they are with the standard library classes.


Constructors and other class methods
------------------------------------

.. autoclass:: neo4j.time.DateTime(year, month, day, hour=0, minute=0, second=0.0, tzinfo=None)

.. classmethod:: DateTime.now()

    :raises OverflowError: if the timestamp is out of the range of values supported by the platform C localtime() function. It’s common for this to be restricted to years from 1970 through 2038.

.. classmethod:: DateTime.utc_now()

.. classmethod:: DateTime.from_timestamp(timestamp, tz=None)

    :raises OverflowError: if the timestamp is out of the range of values supported by the platform C localtime() function. It’s common for this to be restricted to years from 1970 through 2038.

.. classmethod:: DateTime.utc_from_timestamp(timestamp)

.. classmethod:: DateTime.from_ordinal(ordinal)

.. classmethod:: DateTime.combine(date, time)

..
    NotImplementedError
    .. classmethod:: DateTime.parse(timestamp, tz=None)

.. classmethod:: DateTime.from_native(datetime)

.. classmethod:: DateTime.from_clock_time(t, epoch)


Class attributes
----------------

.. attribute:: DateTime.min

.. attribute:: DateTime.max

.. attribute:: DateTime.resolution


Instance attributes
-------------------

.. attribute:: dt.year

.. attribute:: dt.month

.. attribute:: dt.day

.. attribute:: dt.year_month_day

.. attribute:: dt.year_week_day

.. attribute:: dt.year_day

.. attribute:: dt.hour

.. attribute:: dt.minute

.. attribute:: dt.second

.. attribute:: dt.tzinfo

.. attribute:: dt.hour_minute_second


Operations
----------

.. describe:: hash(dt)

.. describe:: dt1 == dt2

.. describe:: dt1 != dt2

.. describe:: dt1 < dt2

.. describe:: dt1 > dt2

.. describe:: dt1 <= dt2

.. describe:: dt1 >= dt2

.. describe:: dt1 + timedelta -> dt2
              dt1 + duration -> dt2

.. describe:: dt1 - timedelta -> dt2
              dt1 - duration -> dt2

.. describe:: dt1 - dt2 -> timedelta


Instance methods
----------------

.. method:: dt.date()

.. method:: dt.time()

.. method:: dt.timetz()

.. method:: dt.replace(year=self.year, month=self.month, day=self.day, hour=self.hour, minute=self.minute, second=self.second, tzinfo=self.tzinfo)

    Return a :class:`.DateTime` with one or more components replaced with new values.

.. method:: dt.as_timezone()

.. method:: dt.utc_offset()

.. method:: dt.dst()

.. method:: dt.tzname()

.. method:: dt.time_tuple()

.. method:: dt.utc_time_tuple()

.. method:: dt.to_ordinal()

.. method:: dt.weekday()

.. method:: dt.iso_weekday()

.. method:: dt.iso_calendar()

.. method:: dt.iso_format()

.. method:: dt.__repr__()

.. method:: dt.__str__()

.. method:: dt.__format__()


Special values
--------------

.. attribute:: Never

    A :class:`.DateTime` instance set to `0000-00-00T00:00:00`.
    This has a :class:`.Date` component equal to :attr:`.ZeroDate` and a :class:`.Time` component equal to :attr:`.Midnight`.

.. attribute:: UnixEpoch

    A :class:`.DateTime` instance set to `1970-01-01T00:00:00`.


LocalDateTime
-------------

When tzinfo is set to ``None``



Duration
========

A :class:`neo4j.time.Duration` represents the difference between two points in time.
Duration objects store a composite value of `months`, `days` and `seconds`.
Unlike :class:`datetime.timedelta` however, days and seconds are never interchanged
and are applied separately in calculations.

.. class:: neo4j.time.Duration(years=0, months=0, weeks=0, days=0, hours=0, minutes=0, seconds=0, subseconds=0, milliseconds=0, microseconds=0, nanoseconds=0)

    All arguments are optional and default to zero.

.. attribute:: Duration.min

    The lowest duration value possible.

.. attribute:: Duration.max

    The highest duration value possible.


Instance methods and attributes
-------------------------------

A :class:`neo4j.time.Duration` stores four primary instance attributes internally: ``months``, ``days``, ``seconds`` and ``subseconds``.
These are maintained as individual values and are immutable.
Each of these four attributes can carry its own sign, with the exception of ``subseconds``, which must have the same sign as ``seconds``.
This structure allows the modelling of durations such as `3 months minus 2 days`.

Two additional secondary attributes are available, each returning a 3-tuple of derived values.
These are ``years_months_days`` and ``hours_minutes_seconds``.

The primary instance attributes and their permitted ranges are listed below.

==============  ========================================================
Attribute       Value
--------------  --------------------------------------------------------
``months``      Between -(2\ :sup:`63`) and (2\ :sup:`63` - 1) inclusive
``days``        Between -(2\ :sup:`63`) and (2\ :sup:`63` - 1) inclusive
``seconds``     Between -(2\ :sup:`63`) and (2\ :sup:`63` - 1) inclusive
``subseconds``  Between -0.999,999,999 and +0.999,999,999 inclusive
==============  ========================================================


Operations
----------

:class:`neo4j.time.Duration` objects support a number of operations. These are listed below.

========================  ====================================================================================================================================================================================
Operation                 Result
------------------------  ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
``d1 + d2``               A ``Duration`` representing the sum of ``d1`` and ``d2`` .
``d1 - d2``               A ``Duration`` representing the difference between ``d1`` and ``d2`` .
``d1 * i``                A ``Duration`` representing ``d1`` times ``i``, where ``i`` is an ``int``.
``d1 * f``                A ``Duration`` representing ``d1`` times ``f``, where ``f`` is a ``float``.
``d1 / i``                A ``Duration`` representing ``d1`` divided by ``i``, where ``i`` is an ``int``. Month and day attributes are rounded to the nearest integer, using round-half-to-even.
``d1 / f``                A ``Duration`` representing ``d1`` divided by ``f``, where ``f`` is a ``float``. Month and day attributes are rounded to the nearest integer, using round-half-to-even.
``d1 // i``               A ``Duration`` representing the floor after ``d1`` is divided by ``i``, where ``i`` is an ``int``.
``d1 % i``                A ``Duration`` representing the remainder after ``d1`` is divided by ``i``, where ``i`` is an ``int``.
``divmod(d1, i)``         A pair of ``Duration`` objects representing the floor and remainder after ``d1`` is divided by ``i``, where ``i`` is an ``int``.
``+d1``                   A ``Duration`` identical to ``d1`` .
``-d1``                   A ``Duration`` that is the inverse of ``d1``. Equivalent to ``Duration(months=-d1.months, days=-d1.days, seconds=-d1.seconds, subseconds=-d1.subseconds)``.
``abs(d1)``               A ``Duration`` equal to the absolute value of ``d1``. Equivalent to ``Duration(months=abs(d1.months), days=abs(d1.days), seconds=abs(d1.seconds), subseconds=abs(d1.subseconds))``.
``str(d1)``
``repr(d1)``
``bool(d1)``              :const:`True` if any attribute is non-zero, :const:`False` otherwise.
``tuple(d1)``             A 4-tuple of ``(months: int, days: int, seconds: int, subseconds: float)``.
========================  ====================================================================================================================================================================================

