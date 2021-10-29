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


# TODO: this test will stay until a uniform behavior for `.single()` across the
#       drivers has been specified and tests are created in testkit
def test_result_single_with_no_records(session):
    result = session.run("CREATE ()")
    record = result.single()
    assert record is None


# TODO: this test will stay until a uniform behavior for `.single()` across the
#       drivers has been specified and tests are created in testkit
def test_result_single_with_one_record(session):
    result = session.run("UNWIND [1] AS n RETURN n")
    record = result.single()
    assert record["n"] == 1


# TODO: this test will stay until a uniform behavior for `.single()` across the
#       drivers has been specified and tests are created in testkit
def test_result_single_with_multiple_records(session):
    import warnings
    result = session.run("UNWIND [1, 2, 3] AS n RETURN n")
    with pytest.warns(UserWarning, match="Expected a result with a single record"):
        record = result.single()
        assert record[0] == 1


# TODO: this test will stay until a uniform behavior for `.single()` across the
#       drivers has been specified and tests are created in testkit
def test_result_single_consumes_the_result(session):
    result = session.run("UNWIND [1, 2, 3] AS n RETURN n")
    with pytest.warns(UserWarning, match="Expected a result with a single record"):
        _ = result.single()
        records = list(result)
        assert records == []
