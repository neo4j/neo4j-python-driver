# Copyright (c) "Neo4j"
# Neo4j Sweden AB [http://neo4j.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# python -m pytest tests/integration/examples/test_temporal_types_example.py -s -v


def _echo(tx, x):
    return tx.run("RETURN $x AS fieldName", x=x).single()


def test_datetime(driver):
    # isort: off
    # tag::temporal-types-datetime-import[]
    from datetime import datetime

    from neo4j.time import DateTime
    import pytz
    # end::temporal-types-datetime-import[]
    # isort: on

    # tag::temporal-types-datetime[]
    # Create datetimes to be used as query parameters
    # Python's builtin datetimes works as well. However, they don't support
    # the full feature-set of Neo4j's durations: it has no nanosecond precision.
    py_dt = datetime(2021, month=11, day=2, hour=7, minute=47, microsecond=4)
    py_dt = pytz.timezone("US/Eastern").localize(py_dt)

    # A DateTime can be created from a native datetime
    dt = DateTime.from_native(py_dt)
    # or directly
    dt = DateTime(year=2021, month=11, day=2, hour=7, minute=47,
                  nanosecond=4123)
    dt = pytz.timezone("US/Eastern").localize(dt)
    # end::temporal-types-datetime[]

    in_dt = dt  # stored for later assertions

    with driver.session() as session:
        record = session.read_transaction(_echo, dt)

    # tag::temporal-types-datetime[]

    # Reading a DateTime from a record
    dt = record.get("fieldName")  # type: DateTime
    str(dt)  # '2021-11-02T07:47:09.232260000-04:00'

    # converting DateTime to native datetime (lossy)
    native = dt.to_native()  # type: datetime
    # end::temporal-types-datetime[]

    assert isinstance(dt, DateTime)
    assert str(dt) == "2021-11-02T07:47:00.000004123-04:00"
    assert dt == in_dt
    assert isinstance(native, datetime)
    assert native == py_dt

    with driver.session() as session:
        record = session.read_transaction(_echo, py_dt)

    dt = record.get("fieldName")
    assert isinstance(dt, DateTime)
    assert dt == in_dt.to_native()


def test_date(driver):
    # isort: off
    # tag::temporal-types-date-import[]
    from datetime import date

    from neo4j.time import Date
    # end::temporal-types-date-import[]
    # isort: on

    # tag::temporal-types-date[]
    # Create dates to be used as query parameters
    # Python's builtin dates works as well.
    py_d = date(year=2021, month=11, day=2)

    # A Date can be created from a native date
    d = Date.from_native(py_d)
    # or directly
    d = Date(year=2021, month=11, day=2)
    # end::temporal-types-date[]

    assert d == Date.from_native(py_d)

    in_d = d  # stored for later assertions

    with driver.session() as session:
        record = session.read_transaction(_echo, d)

    # tag::temporal-types-date[]

    # Reading a Date from a record
    d = record.get("fieldName")  # type: Date
    str(d)  # '2021-11-02'

    # converting Date to native date
    native = d.to_native()  # type: date
    # end::temporal-types-date[]

    assert isinstance(d, Date)
    assert str(d) == "2021-11-02"
    assert d == in_d
    assert isinstance(native, date)
    assert native == py_d

    with driver.session() as session:
        record = session.read_transaction(_echo, py_d)

    d = record.get("fieldName")
    assert isinstance(d, Date)
    assert d == in_d.to_native()


def test_time(driver):
    # isort: off
    # tag::temporal-types-time-import[]
    from datetime import time

    from neo4j.time import Time
    import pytz
    # end::temporal-types-time-import[]
    # isort: on

    # tag::temporal-types-time[]
    # Create datetimes to be used as query parameters
    # Python's builtin datetimes works as well. However, they don't support
    # the full feature-set of Neo4j's durations: it has no nanosecond precision.
    py_t = time(hour=7, minute=47, microsecond=4, tzinfo=pytz.FixedOffset(-240))

    # A Time can be created from a native time
    t = Time.from_native(py_t)
    # or directly
    t = Time(hour=7, minute=47, nanosecond=4123, tzinfo=pytz.FixedOffset(-240))
    # end::temporal-types-time[]

    in_t = t  # stored for later assertions

    with driver.session() as session:
        record = session.read_transaction(_echo, t)

    # tag::temporal-types-time[]

    # Reading a Time from a record
    t = record.get("fieldName")  # type: Time
    str(t)  # 'T07:47:09.232260000-04:00'

    # converting Time to native time (lossy)
    native = t.to_native()  # type: time
    # end::temporal-types-time[]

    assert isinstance(t, Time)
    assert str(t) == "07:47:00.000004123-04:00"
    assert t == in_t
    assert isinstance(native, time)
    assert native == py_t

    with driver.session() as session:
        record = session.read_transaction(_echo, py_t)

    t = record.get("fieldName")
    assert isinstance(t, Time)
    assert t == in_t.to_native()


def test_local_datetime(driver):
    # isort: off
    # tag::temporal-types-local-datetime-import[]
    from datetime import datetime

    from neo4j.time import DateTime
    # end::temporal-types-local-datetime-import[]
    # isort: on

    # tag::temporal-types-local-datetime[]
    # Create datetimes to be used as query parameters
    # Python's builtin datetimes works as well. However, they don't support
    # the full feature-set of Neo4j's durations: it has no nanosecond precision.
    py_dt = datetime(2021, month=11, day=2, hour=7, minute=47, microsecond=4)

    # A DateTime can be created from a native datetime
    dt = DateTime.from_native(py_dt)
    # or directly
    dt = DateTime(year=2021, month=11, day=2, hour=7, minute=47,
                  nanosecond=4123)
    # end::temporal-types-local-datetime[]

    in_dt = dt  # stored for later assertions

    with driver.session() as session:
        record = session.read_transaction(_echo, dt)

    # tag::temporal-types-local-datetime[]

    # Reading a DateTime from a record
    dt = record.get("fieldName")  # type: DateTime
    str(dt)  # '2021-11-02T07:47:09.232260000'

    # converting DateTime to native datetime (lossy)
    native = dt.to_native()  # type: datetime
    # end::temporal-types-local-datetime[]

    assert isinstance(dt, DateTime)
    assert str(dt) == "2021-11-02T07:47:00.000004123"
    assert dt == in_dt
    assert isinstance(native, datetime)
    assert native == py_dt

    with driver.session() as session:
        record = session.read_transaction(_echo, py_dt)

    dt = record.get("fieldName")
    assert isinstance(dt, DateTime)
    assert dt == in_dt.to_native()


def test_local_time(driver):
    # isort: off
    # tag::temporal-types-local-time-import[]
    from datetime import time

    from neo4j.time import Time
    # end::temporal-types-local-time-import[]
    # isort: on

    # tag::temporal-types-local-time[]
    # Create datetimes to be used as query parameters
    # Python's builtin datetimes works as well. However, they don't support
    # the full feature-set of Neo4j's durations: it has no nanosecond precision.
    py_t = time(hour=7, minute=47, microsecond=4)

    # A Time can be created from a native time
    t = Time.from_native(py_t)
    # or directly
    t = Time(hour=7, minute=47, nanosecond=4123)
    # end::temporal-types-local-time[]

    in_t = t  # stored for later assertions

    with driver.session() as session:
        record = session.read_transaction(_echo, t)

    # tag::temporal-types-local-time[]

    # Reading a Time from a record
    t = record.get("fieldName")  # type: Time
    str(t)  # 'T07:47:09.232260000'

    # converting Time to native time (lossy)
    native = t.to_native()  # type: time
    # end::temporal-types-local-time[]

    assert isinstance(t, Time)
    assert str(t) == "07:47:00.000004123"
    assert t == in_t
    assert isinstance(native, time)
    assert native == py_t

    with driver.session() as session:
        record = session.read_transaction(_echo, py_t)

    t = record.get("fieldName")
    assert isinstance(t, Time)
    assert t == in_t.to_native()


def test_duration_example(driver):
    # isort: off
    # tag::temporal-types-duration-import[]
    from datetime import timedelta

    from neo4j.time import Duration
    # end::temporal-types-duration-import[]
    # isort: on

    # tag::temporal-types-duration[]
    # Creating durations to be used as query parameters
    duration = Duration(years=1, days=2, seconds=3, nanoseconds=4)
    # Python's builtin timedeltas works as well. However, they don't support
    # the full feature-set of Neo4j's durations,
    # e.g., no nanoseconds and no months.
    py_duration = timedelta(days=2, seconds=3, microseconds=4)
    # end::temporal-types-duration[]

    in_duration = duration  # stored for later assertions

    with driver.session() as session:
        record = session.read_transaction(_echo, duration)

    # tag::temporal-types-duration[]

    # Reading a Duration from a record
    duration = record.get("fieldName")  # type: Duration
    str(duration)  # 'P1Y2DT3.000000004S'
    # end::temporal-types-duration[]

    assert isinstance(duration, Duration)
    assert str(duration) == 'P1Y2DT3.000000004S'
    assert duration == in_duration

    with driver.session() as session:
        record = session.read_transaction(_echo, py_duration)

    duration = record.get("fieldName")
    assert isinstance(duration, Duration)
    assert str(duration) == 'P2DT3.000004S'
    assert Duration() + py_duration == duration
