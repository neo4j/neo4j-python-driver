#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2018 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from math import isnan
from unittest import SkipTest

from pytz import FixedOffset, timezone, utc

from neo4j.exceptions import CypherTypeError
from neo4j.types.graph import Node, Relationship, Path
from neo4j.types.spatial import CartesianPoint, WGS84Point
from neo4j.types.temporal import Duration, Date, Time, DateTime

from test.integration.tools import DirectIntegrationTestCase


def run_and_rollback(tx, statement, **parameters):
    result = tx.run(statement, **parameters)
    value = result.single().value()
    tx.success = False
    return value


class CoreTypeOutputTestCase(DirectIntegrationTestCase):

    def test_null(self):
        with self.driver.session() as session:
            result = session.run("RETURN null")
            self.assertIs(result.single().value(), None)

    def test_boolean(self):
        with self.driver.session() as session:
            result = session.run("RETURN true")
            self.assertIs(result.single().value(), True)

    def test_integer(self):
        with self.driver.session() as session:
            result = session.run("RETURN 123456789")
            self.assertEqual(result.single().value(), 123456789)

    def test_float(self):
        with self.driver.session() as session:
            result = session.run("RETURN 3.1415926")
            self.assertEqual(result.single().value(), 3.1415926)

    def test_float_nan(self):
        not_a_number = float("NaN")
        with self.driver.session() as session:
            result = session.run("WITH $x AS x RETURN x", x=not_a_number)
            self.assertTrue(isnan(result.single().value()))

    def test_float_positive_infinity(self):
        infinity = float("+Inf")
        with self.driver.session() as session:
            result = session.run("WITH $x AS x RETURN x", x=infinity)
            self.assertEqual(result.single().value(), infinity)

    def test_float_negative_infinity(self):
        infinity = float("-Inf")
        with self.driver.session() as session:
            result = session.run("WITH $x AS x RETURN x", x=infinity)
            self.assertEqual(result.single().value(), infinity)

    def test_string(self):
        with self.driver.session() as session:
            result = session.run("RETURN 'hello, world'")
            self.assertEqual(result.single().value(), "hello, world")

    def test_bytes(self):
        with self.driver.session() as session:
            data = bytearray([0x00, 0x33, 0x66, 0x99, 0xCC, 0xFF])
            try:
                value = session.write_transaction(run_and_rollback, "CREATE (a {x:$x}) RETURN a.x", x=data)
            except TypeError:
                raise SkipTest("Bytes not supported in this server version")
            self.assertEqual(value, data)

    def test_list(self):
        with self.driver.session() as session:
            result = session.run("RETURN ['one', 'two', 'three']")
            self.assertEqual(result.single().value(), ["one", "two", "three"])

    def test_map(self):
        with self.driver.session() as session:
            result = session.run("RETURN {one: 'eins', two: 'zwei', three: 'drei'}")
            self.assertEqual(result.single().value(), {"one": "eins", "two": "zwei", "three": "drei"})

    def test_non_string_map_keys(self):
        with self.driver.session() as session:
            with self.assertRaises(TypeError):
                _ = session.run("RETURN $x", x={1: 'eins', 2: 'zwei', 3: 'drei'})


class GraphTypeOutputTestCase(DirectIntegrationTestCase):

    def test_node(self):
        with self.driver.session() as session:
            a = session.write_transaction(run_and_rollback, "CREATE (a:Person {name:'Alice'}) RETURN a")
            self.assertIsInstance(a, Node)
            self.assertEqual(a.labels, {"Person"})
            self.assertEqual(dict(a), {"name": "Alice"})

    def test_relationship(self):
        with self.driver.session() as session:
            a, b, r = session.write_transaction(
                run_and_rollback, "CREATE (a)-[r:KNOWS {since:1999}]->(b) RETURN [a, b, r]")
            self.assertIsInstance(r, Relationship)
            self.assertEqual(r.type, "KNOWS")
            self.assertEqual(dict(r), {"since": 1999})
            self.assertEqual(r.start_node, a)
            self.assertEqual(r.end_node, b)

    def test_path(self):
        with self.driver.session() as session:
            a, b, c, ab, bc, p = session.write_transaction(
                run_and_rollback, "CREATE p=(a)-[ab:X]->(b)-[bc:X]->(c) RETURN [a, b, c, ab, bc, p]")
            self.assertIsInstance(p, Path)
            self.assertEqual(len(p), 2)
            self.assertEqual(p.nodes, (a, b, c))
            self.assertEqual(p.relationships, (ab, bc))
            self.assertEqual(p.start_node, a)
            self.assertEqual(p.end_node, c)


class SpatialTypeInputTestCase(DirectIntegrationTestCase):

    def test_cartesian_point(self):
        self.assert_supports_spatial_types()
        with self.driver.session() as session:
            result = session.run("CYPHER runtime=interpreted WITH $point AS point "
                                 "RETURN point.x, point.y",
                                 point=CartesianPoint((1.23, 4.56)))
            x, y = result.single()
            self.assertEqual(x, 1.23)
            self.assertEqual(y, 4.56)

    def test_cartesian_3d_point(self):
        self.assert_supports_spatial_types()
        with self.driver.session() as session:
            result = session.run("CYPHER runtime=interpreted WITH $point AS point "
                                 "RETURN point.x, point.y, point.z",
                                 point=CartesianPoint((1.23, 4.56, 7.89)))
            x, y, z = result.single()
            self.assertEqual(x, 1.23)
            self.assertEqual(y, 4.56)
            self.assertEqual(z, 7.89)

    def test_wgs84_point(self):
        self.assert_supports_spatial_types()
        with self.driver.session() as session:
            result = session.run("CYPHER runtime=interpreted WITH $point AS point "
                                 "RETURN point.latitude, point.longitude",
                                 point=WGS84Point((1.23, 4.56)))
            latitude, longitude = result.single()
            self.assertEqual(longitude, 1.23)
            self.assertEqual(latitude, 4.56)

    def test_wgs84_3d_point(self):
        self.assert_supports_spatial_types()
        with self.driver.session() as session:
            result = session.run("CYPHER runtime=interpreted WITH $point AS point "
                                 "RETURN point.latitude, point.longitude, point.height",
                                 point=WGS84Point((1.23, 4.56, 7.89)))
            latitude, longitude, height = result.single()
            self.assertEqual(longitude, 1.23)
            self.assertEqual(latitude, 4.56)
            self.assertEqual(height, 7.89)

    def test_point_array(self):
        self.assert_supports_temporal_types()
        with self.driver.session() as session:
            data = [WGS84Point((1.23, 4.56)), WGS84Point((9.87, 6.54))]
            value = session.write_transaction(run_and_rollback, "CREATE (a {x:$x}) RETURN a.x", x=data)
            self.assertEqual(value, data)


class SpatialTypeOutputTestCase(DirectIntegrationTestCase):

    def test_cartesian_point(self):
        self.assert_supports_spatial_types()
        with self.driver.session() as session:
            result = session.run("RETURN point({x:3, y:4})")
            value = result.single().value()
            self.assertIsInstance(value, CartesianPoint)
            self.assertEqual(value.x, 3.0)
            self.assertEqual(value.y, 4.0)
            with self.assertRaises(AttributeError):
                _ = value.z

    def test_cartesian_3d_point(self):
        self.assert_supports_spatial_types()
        with self.driver.session() as session:
            result = session.run("RETURN point({x:3, y:4, z:5})")
            value = result.single().value()
            self.assertIsInstance(value, CartesianPoint)
            self.assertEqual(value.x, 3.0)
            self.assertEqual(value.y, 4.0)
            self.assertEqual(value.z, 5.0)

    def test_wgs84_point(self):
        self.assert_supports_spatial_types()
        with self.driver.session() as session:
            result = session.run("RETURN point({latitude:3, longitude:4})")
            value = result.single().value()
            self.assertIsInstance(value, WGS84Point)
            self.assertEqual(value.latitude, 3.0)
            self.assertEqual(value.y, 3.0)
            self.assertEqual(value.longitude, 4.0)
            self.assertEqual(value.x, 4.0)
            with self.assertRaises(AttributeError):
                _ = value.height
            with self.assertRaises(AttributeError):
                _ = value.z

    def test_wgs84_3d_point(self):
        self.assert_supports_spatial_types()
        with self.driver.session() as session:
            result = session.run("RETURN point({latitude:3, longitude:4, height:5})")
            value = result.single().value()
            self.assertIsInstance(value, WGS84Point)
            self.assertEqual(value.latitude, 3.0)
            self.assertEqual(value.y, 3.0)
            self.assertEqual(value.longitude, 4.0)
            self.assertEqual(value.x, 4.0)
            self.assertEqual(value.height, 5.0)
            self.assertEqual(value.z, 5.0)


class TemporalTypeInputTestCase(DirectIntegrationTestCase):

    def test_native_date(self):
        from datetime import date
        self.assert_supports_temporal_types()
        with self.driver.session() as session:
            result = session.run("CYPHER runtime=interpreted WITH $x AS x "
                                 "RETURN x.year, x.month, x.day",
                                 x=date(1976, 6, 13))
            year, month, day = result.single()
            self.assertEqual(year, 1976)
            self.assertEqual(month, 6)
            self.assertEqual(day, 13)

    def test_date(self):
        self.assert_supports_temporal_types()
        with self.driver.session() as session:
            result = session.run("CYPHER runtime=interpreted WITH $x AS x "
                                 "RETURN x.year, x.month, x.day",
                                 x=Date(1976, 6, 13))
            year, month, day = result.single()
            self.assertEqual(year, 1976)
            self.assertEqual(month, 6)
            self.assertEqual(day, 13)

    def test_date_array(self):
        self.assert_supports_temporal_types()
        with self.driver.session() as session:
            data = [DateTime.now().date(), Date(1976, 6, 13)]
            value = session.write_transaction(run_and_rollback, "CREATE (a {x:$x}) RETURN a.x", x=data)
            self.assertEqual(value, data)

    def test_native_time(self):
        from datetime import time
        self.assert_supports_temporal_types()
        with self.driver.session() as session:
            result = session.run("CYPHER runtime=interpreted WITH $x AS x "
                                 "RETURN x.hour, x.minute, x.second, x.nanosecond",
                                 x=time(12, 34, 56, 789012))
            hour, minute, second, nanosecond = result.single()
            self.assertEqual(hour, 12)
            self.assertEqual(minute, 34)
            self.assertEqual(second, 56)
            self.assertEqual(nanosecond, 789012000)

    def test_whole_second_time(self):
        self.assert_supports_temporal_types()
        with self.driver.session() as session:
            result = session.run("CYPHER runtime=interpreted WITH $x AS x "
                                 "RETURN x.hour, x.minute, x.second",
                                 x=Time(12, 34, 56))
            hour, minute, second = result.single()
            self.assertEqual(hour, 12)
            self.assertEqual(minute, 34)
            self.assertEqual(second, 56)

    def test_nanosecond_resolution_time(self):
        self.assert_supports_temporal_types()
        with self.driver.session() as session:
            result = session.run("CYPHER runtime=interpreted WITH $x AS x "
                                 "RETURN x.hour, x.minute, x.second, x.nanosecond",
                                 x=Time(12, 34, 56.789012345))
            hour, minute, second, nanosecond = result.single()
            self.assertEqual(hour, 12)
            self.assertEqual(minute, 34)
            self.assertEqual(second, 56)
            self.assertEqual(nanosecond, 789012345)

    def test_time_with_numeric_time_offset(self):
        self.assert_supports_temporal_types()
        with self.driver.session() as session:
            result = session.run("CYPHER runtime=interpreted WITH $x AS x "
                                 "RETURN x.hour, x.minute, x.second, x.nanosecond, x.offset",
                                 x=Time(12, 34, 56.789012345, tzinfo=FixedOffset(90)))
            hour, minute, second, nanosecond, offset = result.single()
            self.assertEqual(hour, 12)
            self.assertEqual(minute, 34)
            self.assertEqual(second, 56)
            self.assertEqual(nanosecond, 789012345)
            self.assertEqual(offset, "+01:30")

    def test_time_array(self):
        self.assert_supports_temporal_types()
        with self.driver.session() as session:
            data = [Time(12, 34, 56), Time(10, 0, 0)]
            value = session.write_transaction(run_and_rollback, "CREATE (a {x:$x}) RETURN a.x", x=data)
            self.assertEqual(value, data)

    def test_native_datetime(self):
        from datetime import datetime
        self.assert_supports_temporal_types()
        with self.driver.session() as session:
            result = session.run("CYPHER runtime=interpreted WITH $x AS x "
                                 "RETURN x.year, x.month, x.day, "
                                 "       x.hour, x.minute, x.second, x.nanosecond",
                                 x=datetime(1976, 6, 13, 12, 34, 56, 789012))
            year, month, day, hour, minute, second, nanosecond = result.single()
            self.assertEqual(year, 1976)
            self.assertEqual(month, 6)
            self.assertEqual(day, 13)
            self.assertEqual(hour, 12)
            self.assertEqual(minute, 34)
            self.assertEqual(second, 56)
            self.assertEqual(nanosecond, 789012000)

    def test_whole_second_datetime(self):
        self.assert_supports_temporal_types()
        with self.driver.session() as session:
            result = session.run("CYPHER runtime=interpreted WITH $x AS x "
                                 "RETURN x.year, x.month, x.day, "
                                 "       x.hour, x.minute, x.second",
                                 x=DateTime(1976, 6, 13, 12, 34, 56))
            year, month, day, hour, minute, second = result.single()
            self.assertEqual(year, 1976)
            self.assertEqual(month, 6)
            self.assertEqual(day, 13)
            self.assertEqual(hour, 12)
            self.assertEqual(minute, 34)
            self.assertEqual(second, 56)

    def test_nanosecond_resolution_datetime(self):
        self.assert_supports_temporal_types()
        with self.driver.session() as session:
            result = session.run("CYPHER runtime=interpreted WITH $x AS x "
                                 "RETURN x.year, x.month, x.day, "
                                 "       x.hour, x.minute, x.second, x.nanosecond",
                                 x=DateTime(1976, 6, 13, 12, 34, 56.789012345))
            year, month, day, hour, minute, second, nanosecond = result.single()
            self.assertEqual(year, 1976)
            self.assertEqual(month, 6)
            self.assertEqual(day, 13)
            self.assertEqual(hour, 12)
            self.assertEqual(minute, 34)
            self.assertEqual(second, 56)
            self.assertEqual(nanosecond, 789012345)

    def test_datetime_with_numeric_time_offset(self):
        self.assert_supports_temporal_types()
        with self.driver.session() as session:
            result = session.run("CYPHER runtime=interpreted WITH $x AS x "
                                 "RETURN x.year, x.month, x.day, "
                                 "       x.hour, x.minute, x.second, x.nanosecond, x.offset",
                                 x=DateTime(1976, 6, 13, 12, 34, 56.789012345, tzinfo=FixedOffset(90)))
            year, month, day, hour, minute, second, nanosecond, offset = result.single()
            self.assertEqual(year, 1976)
            self.assertEqual(month, 6)
            self.assertEqual(day, 13)
            self.assertEqual(hour, 12)
            self.assertEqual(minute, 34)
            self.assertEqual(second, 56)
            self.assertEqual(nanosecond, 789012345)
            self.assertEqual(offset, "+01:30")

    def test_datetime_with_named_time_zone(self):
        self.assert_supports_temporal_types()
        with self.driver.session() as session:
            input_value = timezone("US/Pacific").localize(DateTime(1976, 6, 13, 12, 34, 56.789012345))
            result = session.run("CYPHER runtime=interpreted WITH $x AS x "
                                 "RETURN x.year, x.month, x.day, "
                                 "       x.hour, x.minute, x.second, x.nanosecond, x.timezone",
                                 x=input_value)
            year, month, day, hour, minute, second, nanosecond, tz = result.single()
            self.assertEqual(year, input_value.year)
            self.assertEqual(month, input_value.month)
            self.assertEqual(day, input_value.day)
            self.assertEqual(hour, input_value.hour)
            self.assertEqual(minute, input_value.minute)
            self.assertEqual(second, int(input_value.second))
            self.assertEqual(nanosecond, int (1000000000 * input_value.second) % 1000000000)
            self.assertEqual(tz, input_value.tzinfo.zone)

    def test_datetime_array(self):
        self.assert_supports_temporal_types()
        with self.driver.session() as session:
            data = [DateTime(2018, 4, 6, 13, 4, 42.516120), DateTime(1976, 6, 13)]
            value = session.write_transaction(run_and_rollback, "CREATE (a {x:$x}) RETURN a.x", x=data)
            self.assertEqual(value, data)

    def test_duration(self):
        self.assert_supports_temporal_types()
        with self.driver.session() as session:
            result = session.run("CYPHER runtime=interpreted WITH $x AS x "
                                 "RETURN x.months, x.days, x.seconds, x.microsecondsOfSecond",
                                 x=Duration(years=1, months=2, days=3, hours=4, minutes=5, seconds=6.789012))
            months, days, seconds, microseconds = result.single()
            self.assertEqual(months, 14)
            self.assertEqual(days, 3)
            self.assertEqual(seconds, 14706)
            self.assertEqual(microseconds, 789012)

    def test_duration_array(self):
        self.assert_supports_temporal_types()
        with self.driver.session() as session:
            data = [Duration(1, 2, 3, 4, 5, 6), Duration(9, 8, 7, 6, 5, 4)]
            value = session.write_transaction(run_and_rollback, "CREATE (a {x:$x}) RETURN a.x", x=data)
            self.assertEqual(value, data)

    def test_timedelta(self):
        from datetime import timedelta
        self.assert_supports_temporal_types()
        with self.driver.session() as session:
            result = session.run("CYPHER runtime=interpreted WITH $x AS x "
                                 "RETURN x.months, x.days, x.seconds, x.microsecondsOfSecond",
                                 x=timedelta(days=3, hours=4, minutes=5, seconds=6.789012))
            months, days, seconds, microseconds = result.single()
            self.assertEqual(months, 0)
            self.assertEqual(days, 3)
            self.assertEqual(seconds, 14706)
            self.assertEqual(microseconds, 789012)

    def test_mixed_array(self):
        self.assert_supports_temporal_types()
        with self.driver.session() as session:
            data = [Date(1976, 6, 13), Duration(9, 8, 7, 6, 5, 4)]
            with self.assertRaises(CypherTypeError):
                _ = session.write_transaction(run_and_rollback, "CREATE (a {x:$x}) RETURN a.x", x=data)


class TemporalTypeOutputTestCase(DirectIntegrationTestCase):

    def test_date(self):
        self.assert_supports_temporal_types()
        with self.driver.session() as session:
            result = session.run("RETURN date('1976-06-13')")
            value = result.single().value()
            self.assertIsInstance(value, Date)
            self.assertEqual(value, Date(1976, 6, 13))

    def test_whole_second_time(self):
        self.assert_supports_temporal_types()
        with self.driver.session() as session:
            result = session.run("RETURN time('12:34:56')")
            value = result.single().value()
            self.assertIsInstance(value, Time)
            self.assertEqual(value, Time(12, 34, 56, tzinfo=FixedOffset(0)))

    def test_nanosecond_resolution_time(self):
        self.assert_supports_temporal_types()
        with self.driver.session() as session:
            result = session.run("RETURN time('12:34:56.789012345')")
            value = result.single().value()
            self.assertIsInstance(value, Time)
            self.assertEqual(value, Time(12, 34, 56.789012345, tzinfo=FixedOffset(0)))

    def test_time_with_numeric_time_offset(self):
        self.assert_supports_temporal_types()
        with self.driver.session() as session:
            result = session.run("RETURN time('12:34:56.789012345+0130')")
            value = result.single().value()
            self.assertIsInstance(value, Time)
            self.assertEqual(value, Time(12, 34, 56.789012345, tzinfo=FixedOffset(90)))

    def test_whole_second_localtime(self):
        self.assert_supports_temporal_types()
        with self.driver.session() as session:
            result = session.run("RETURN localtime('12:34:56')")
            value = result.single().value()
            self.assertIsInstance(value, Time)
            self.assertEqual(value, Time(12, 34, 56))

    def test_nanosecond_resolution_localtime(self):
        self.assert_supports_temporal_types()
        with self.driver.session() as session:
            result = session.run("RETURN localtime('12:34:56.789012345')")
            value = result.single().value()
            self.assertIsInstance(value, Time)
            self.assertEqual(value, Time(12, 34, 56.789012345))

    def test_whole_second_datetime(self):
        self.assert_supports_temporal_types()
        with self.driver.session() as session:
            result = session.run("RETURN datetime('1976-06-13T12:34:56')")
            value = result.single().value()
            self.assertIsInstance(value, DateTime)
            self.assertEqual(value, DateTime(1976, 6, 13, 12, 34, 56, tzinfo=utc))

    def test_nanosecond_resolution_datetime(self):
        self.assert_supports_temporal_types()
        with self.driver.session() as session:
            result = session.run("RETURN datetime('1976-06-13T12:34:56.789012345')")
            value = result.single().value()
            self.assertIsInstance(value, DateTime)
            self.assertEqual(value, DateTime(1976, 6, 13, 12, 34, 56.789012345, tzinfo=utc))

    def test_datetime_with_numeric_time_offset(self):
        self.assert_supports_temporal_types()
        with self.driver.session() as session:
            result = session.run("RETURN datetime('1976-06-13T12:34:56.789012345+01:30')")
            value = result.single().value()
            self.assertIsInstance(value, DateTime)
            self.assertEqual(value, DateTime(1976, 6, 13, 12, 34, 56.789012345, tzinfo=FixedOffset(90)))

    def test_datetime_with_named_time_zone(self):
        self.assert_supports_temporal_types()
        with self.driver.session() as session:
            result = session.run("RETURN datetime('1976-06-13T12:34:56.789012345[Europe/London]')")
            value = result.single().value()
            self.assertIsInstance(value, DateTime)
            self.assertEqual(value, timezone("Europe/London").localize(DateTime(1976, 6, 13, 12, 34, 56.789012345)))

    def test_whole_second_localdatetime(self):
        self.assert_supports_temporal_types()
        with self.driver.session() as session:
            result = session.run("RETURN localdatetime('1976-06-13T12:34:56')")
            value = result.single().value()
            self.assertIsInstance(value, DateTime)
            self.assertEqual(value, DateTime(1976, 6, 13, 12, 34, 56))

    def test_nanosecond_resolution_localdatetime(self):
        self.assert_supports_temporal_types()
        with self.driver.session() as session:
            result = session.run("RETURN localdatetime('1976-06-13T12:34:56.789012345')")
            value = result.single().value()
            self.assertIsInstance(value, DateTime)
            self.assertEqual(value, DateTime(1976, 6, 13, 12, 34, 56.789012345))

    def test_duration(self):
        self.assert_supports_temporal_types()
        with self.driver.session() as session:
            result = session.run("RETURN duration('P1Y2M3DT4H5M6.789S')")
            value = result.single().value()
            self.assertIsInstance(value, Duration)
            self.assertEqual(value, Duration(years=1, months=2, days=3, hours=4, minutes=5, seconds=6.789))

    def test_nanosecond_resolution_duration(self):
        self.assert_supports_temporal_types()
        with self.driver.session() as session:
            result = session.run("RETURN duration('P1Y2M3DT4H5M6.789123456S')")
            value = result.single().value()
            self.assertIsInstance(value, Duration)
            self.assertEqual(value, Duration(years=1, months=2, days=3, hours=4, minutes=5, seconds=6.789123456))
