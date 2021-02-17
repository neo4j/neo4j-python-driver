#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) "Neo4j"
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


import pytest

from neo4j.exceptions import CypherError

from test.integration.tools import DirectIntegrationTestCase


class ResultConsumptionTestCase(DirectIntegrationTestCase):

    def test_can_consume_result_immediately(self):

        def _(tx):
            result = tx.run("UNWIND range(1, 3) AS n RETURN n")
            self.assertEqual([record[0] for record in result], [1, 2, 3])

        with self.driver.session() as session:
            session.read_transaction(_)

    def test_can_consume_result_from_buffer(self):

        def _(tx):
            result = tx.run("UNWIND range(1, 3) AS n RETURN n")
            result.detach()
            self.assertEqual([record[0] for record in result], [1, 2, 3])

        with self.driver.session() as session:
            session.read_transaction(_)

    def test_can_consume_result_after_commit(self):
        with self.driver.session() as session:
            tx = session.begin_transaction()
            result = tx.run("UNWIND range(1, 3) AS n RETURN n")
            tx.commit()
            self.assertEqual([record[0] for record in result], [1, 2, 3])

    def test_can_consume_result_after_rollback(self):
        with self.driver.session() as session:
            tx = session.begin_transaction()
            result = tx.run("UNWIND range(1, 3) AS n RETURN n")
            tx.rollback()
            self.assertEqual([record[0] for record in result], [1, 2, 3])

    def test_can_consume_result_after_session_close(self):
        with self.driver.session() as session:
            tx = session.begin_transaction()
            result = tx.run("UNWIND range(1, 3) AS n RETURN n")
            tx.commit()
        self.assertEqual([record[0] for record in result], [1, 2, 3])

    def test_can_consume_result_after_session_reuse(self):
        session = self.driver.session()
        tx = session.begin_transaction()
        result_a = tx.run("UNWIND range(1, 3) AS n RETURN n")
        tx.commit()
        session.close()
        session = self.driver.session()
        tx = session.begin_transaction()
        result_b = tx.run("UNWIND range(4, 6) AS n RETURN n")
        tx.commit()
        session.close()
        assert [record[0] for record in result_a] == [1, 2, 3]
        assert [record[0] for record in result_b] == [4, 5, 6]

    def test_can_consume_results_after_harsh_session_death(self):
        session = self.driver.session()
        result_a = session.run("UNWIND range(1, 3) AS n RETURN n")
        del session
        session = self.driver.session()
        result_b = session.run("UNWIND range(4, 6) AS n RETURN n")
        del session
        assert [record[0] for record in result_a] == [1, 2, 3]
        assert [record[0] for record in result_b] == [4, 5, 6]

    def test_can_consume_result_after_session_with_error(self):
        session = self.driver.session()
        with self.assertRaises(CypherError):
            session.run("X").consume()
        session.close()
        session = self.driver.session()
        tx = session.begin_transaction()
        result = tx.run("UNWIND range(1, 3) AS n RETURN n")
        tx.commit()
        session.close()
        assert [record[0] for record in result] == [1, 2, 3]

    def test_single_with_exactly_one_record(self):
        session = self.driver.session()
        result = session.run("UNWIND range(1, 1) AS n RETURN n")
        record = result.single()
        assert list(record.values()) == [1]

    def test_value_with_no_records(self):
        with self.driver.session() as session:
            result = session.run("CREATE ()")
            self.assertEqual(result.value(), [])

    def test_values_with_no_records(self):
        with self.driver.session() as session:
            result = session.run("CREATE ()")
            self.assertEqual(result.values(), [])

    def test_peek_can_look_one_ahead(self):
        session = self.driver.session()
        result = session.run("UNWIND range(1, 3) AS n RETURN n")
        record = result.peek()
        assert list(record.values()) == [1]

    def test_peek_fails_if_nothing_remains(self):
        session = self.driver.session()
        result = session.run("CREATE ()")
        upcoming = result.peek()
        assert upcoming is None

    def test_peek_does_not_advance_cursor(self):
        session = self.driver.session()
        result = session.run("UNWIND range(1, 3) AS n RETURN n")
        result.peek()
        assert [record[0] for record in result] == [1, 2, 3]

    def test_peek_at_different_stages(self):
        session = self.driver.session()
        result = session.run("UNWIND range(0, 9) AS n RETURN n")
        # Peek ahead to the first record
        expected_next = 0
        upcoming = result.peek()
        assert upcoming[0] == expected_next
        # Then look through all the other records
        for expected, record in enumerate(result):
            # Check this record is as expected
            assert record[0] == expected
            # Check the upcoming record is as expected...
            if expected < 9:
                # ...when one should follow
                expected_next = expected + 1
                upcoming = result.peek()
                assert upcoming[0] == expected_next
            else:
                # ...when none should follow
                upcoming = result.peek()
                assert upcoming is None

    def test_can_safely_exit_session_without_consuming_result(self):
        with self.driver.session() as session:
            session.run("RETURN 1")
        assert True

    def test_multiple_value(self):
        with self.driver.session() as session:
            result = session.run("UNWIND range(1, 3) AS n RETURN 1 * n AS x, 2 * n AS y, 3 * n AS z")
            self.assertEqual(result.value(), [1,
                                              2,
                                              3])

    def test_multiple_indexed_value(self):
        with self.driver.session() as session:
            result = session.run("UNWIND range(1, 3) AS n RETURN 1 * n AS x, 2 * n AS y, 3 * n AS z")
            self.assertEqual(result.value(2), [3,
                                               6,
                                               9])

    def test_multiple_keyed_value(self):
        with self.driver.session() as session:
            result = session.run("UNWIND range(1, 3) AS n RETURN 1 * n AS x, 2 * n AS y, 3 * n AS z")
            self.assertEqual(result.value("z"), [3,
                                                 6,
                                                 9])

    def test_multiple_values(self):
        with self.driver.session() as session:
            result = session.run("UNWIND range(1, 3) AS n RETURN 1 * n AS x, 2 * n AS y, 3 * n AS z")
            self.assertEqual(result.values(), [[1, 2, 3],
                                               [2, 4, 6],
                                               [3, 6, 9]])

    def test_multiple_indexed_values(self):
        with self.driver.session() as session:
            result = session.run("UNWIND range(1, 3) AS n RETURN 1 * n AS x, 2 * n AS y, 3 * n AS z")
            self.assertEqual(result.values(2, 0), [[3, 1],
                                                   [6, 2],
                                                   [9, 3]])

    def test_multiple_keyed_values(self):
        with self.driver.session() as session:
            result = session.run("UNWIND range(1, 3) AS n RETURN 1 * n AS x, 2 * n AS y, 3 * n AS z")
            self.assertEqual(result.values("z", "x"), [[3, 1],
                                                       [6, 2],
                                                       [9, 3]])

    def test_multiple_data(self):
        with self.driver.session() as session:
            result = session.run("UNWIND range(1, 3) AS n RETURN 1 * n AS x, 2 * n AS y, 3 * n AS z")
            self.assertEqual(result.data(), [{"x": 1, "y": 2, "z": 3},
                                             {"x": 2, "y": 4, "z": 6},
                                             {"x": 3, "y": 6, "z": 9}])

    def test_multiple_indexed_data(self):
        with self.driver.session() as session:
            result = session.run("UNWIND range(1, 3) AS n RETURN 1 * n AS x, 2 * n AS y, 3 * n AS z")
            self.assertEqual(result.data(2, 0), [{"x": 1, "z": 3},
                                                 {"x": 2, "z": 6},
                                                 {"x": 3, "z": 9}])

    def test_multiple_keyed_data(self):
        with self.driver.session() as session:
            result = session.run("UNWIND range(1, 3) AS n RETURN 1 * n AS x, 2 * n AS y, 3 * n AS z")
            self.assertEqual(result.data("z", "x"), [{"x": 1, "z": 3},
                                                     {"x": 2, "z": 6},
                                                     {"x": 3, "z": 9}])

    def test_value_with_no_keys_and_no_records(self):
        with self.driver.session() as session:
            result = session.run("CREATE ()")
            self.assertEqual(result.value(), [])

    def test_values_with_one_key_and_no_records(self):
        with self.driver.session() as session:
            result = session.run("UNWIND range(1, 0) AS n RETURN n")
            self.assertEqual(result.values(), [])

    def test_data_with_one_key_and_no_records(self):
        with self.driver.session() as session:
            result = session.run("UNWIND range(1, 0) AS n RETURN n")
            self.assertEqual(result.data(), [])


class SingleRecordTestCase(DirectIntegrationTestCase):

    def test_single_with_no_keys_and_no_records(self):
        with self.driver.session() as session:
            result = session.run("CREATE ()")
            record = result.single()
            self.assertIsNone(record)

    def test_single_with_one_key_and_no_records(self):
        with self.driver.session() as session:
            result = session.run("UNWIND range(1, 0) AS n RETURN n")
            record = result.single()
            self.assertIsNone(record)

    def test_single_with_multiple_records(self):
        import warnings
        session = self.driver.session()
        result = session.run("UNWIND range(1, 3) AS n RETURN n")
        with warnings.catch_warnings(record=True) as warning_list:
            warnings.simplefilter("always")
            record = result.single()
            assert len(warning_list) == 1
            assert record[0] == 1

    def test_single_consumes_entire_result_if_one_record(self):
        session = self.driver.session()
        result = session.run("UNWIND range(1, 1) AS n RETURN n")
        _ = result.single()
        assert not result.session

    def test_single_consumes_entire_result_if_multiple_records(self):
        session = self.driver.session()
        result = session.run("UNWIND range(1, 3) AS n RETURN n")
        with pytest.warns(UserWarning):
            _ = result.single()
        assert not result.session

    def test_single_value(self):
        with self.driver.session() as session:
            result = session.run("RETURN 1 AS x, 2 AS y, 3 AS z")
            self.assertEqual(result.single().value(), 1)

    def test_single_indexed_value(self):
        with self.driver.session() as session:
            result = session.run("RETURN 1 AS x, 2 AS y, 3 AS z")
            self.assertEqual(result.single().value(2), 3)

    def test_single_keyed_value(self):
        with self.driver.session() as session:
            result = session.run("RETURN 1 AS x, 2 AS y, 3 AS z")
            self.assertEqual(result.single().value("z"), 3)

    def test_single_values(self):
        with self.driver.session() as session:
            result = session.run("RETURN 1 AS x, 2 AS y, 3 AS z")
            self.assertEqual(result.single().values(), [1, 2, 3])

    def test_single_indexed_values(self):
        with self.driver.session() as session:
            result = session.run("RETURN 1 AS x, 2 AS y, 3 AS z")
            self.assertEqual(result.single().values(2, 0), [3, 1])

    def test_single_keyed_values(self):
        with self.driver.session() as session:
            result = session.run("RETURN 1 AS x, 2 AS y, 3 AS z")
            self.assertEqual(result.single().values("z", "x"), [3, 1])

    def test_single_data(self):
        with self.driver.session() as session:
            result = session.run("RETURN 1 AS x, 2 AS y, 3 AS z")
            self.assertEqual(result.single().data(), {"x": 1, "y": 2, "z": 3})

    def test_single_indexed_data(self):
        with self.driver.session() as session:
            result = session.run("RETURN 1 AS x, 2 AS y, 3 AS z")
            self.assertEqual(result.single().data(2, 0), {"x": 1, "z": 3})

    def test_single_keyed_data(self):
        with self.driver.session() as session:
            result = session.run("RETURN 1 AS x, 2 AS y, 3 AS z")
            self.assertEqual(result.single().data("z", "x"), {"x": 1, "z": 3})
