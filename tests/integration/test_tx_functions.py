# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
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


from uuid import uuid4

import pytest

from neo4j import unit_of_work
from neo4j.exceptions import (
    ClientError,
    Neo4jError,
)


# python -m pytest tests/integration/test_tx_functions.py -s -v


@pytest.fixture(params=["read_transaction", "execute_read"])
def read_transaction(request):
    def executor(session, *args, **kwargs):
        if request.param == "read_transaction":
            with pytest.warns(
                DeprecationWarning,
                match="^read_transaction has been renamed to execute_read$"
            ):
                return session.read_transaction(*args, **kwargs)
        elif request.param == "execute_read":
            return session.execute_read(*args, **kwargs)
        raise ValueError(request.param)

    return executor


@pytest.fixture(params=["write_transaction", "execute_write"])
def write_transaction(request):
    def executor(session, *args, **kwargs):
        if request.param == "write_transaction":
            with pytest.warns(
                DeprecationWarning,
                match="^write_transaction has been renamed to execute_write$"
            ):
                return session.write_transaction(*args, **kwargs)
        elif request.param == "execute_write":
            return session.execute_write(*args, **kwargs)
        raise ValueError(request.param)

    return executor


def test_simple_read(session, read_transaction):

    def work(tx):
        return tx.run("RETURN 1").single().value()

    value = read_transaction(session, work)
    assert value == 1


def test_read_with_arg(session, read_transaction):

    def work(tx, x):
        return tx.run("RETURN $x", x=x).single().value()

    value = read_transaction(session, work, x=1)
    assert value == 1


def test_read_with_arg_and_metadata(session, read_transaction):

    @unit_of_work(timeout=25, metadata={"foo": "bar"})
    def work(tx):
        return tx.run("CALL dbms.getTXMetaData").single().value()

    try:
        value = read_transaction(session, work)
    except ClientError:
        pytest.skip("Transaction metadata and timeout only supported in Neo4j EE 3.5+")
    else:
        assert value == {"foo": "bar"}


def test_simple_write(session, write_transaction):

    def work(tx):
        return tx.run("CREATE (a {x: 1}) RETURN a.x").single().value()

    value = write_transaction(session, work)
    assert value == 1


def test_write_with_arg(session, write_transaction):

    def work(tx, x):
        return tx.run("CREATE (a {x: $x}) RETURN a.x", x=x).single().value()

    value = write_transaction(session, work, x=1)
    assert value == 1


def test_write_with_arg_and_metadata(session, write_transaction):

    @unit_of_work(timeout=25, metadata={"foo": "bar"})
    def work(tx, x):
        return tx.run("CREATE (a {x: $x}) RETURN a.x", x=x).single().value()

    try:
        value = write_transaction(session, work, x=1)
    except ClientError:
        pytest.skip("Transaction metadata and timeout only supported in Neo4j EE 3.5+")
    else:
        assert value == 1


def test_error_on_write_transaction(session, write_transaction):

    def f(tx, uuid):
        tx.run("CREATE (a:Thing {uuid:$uuid})", uuid=uuid), uuid4()

    with pytest.raises(TypeError):
        write_transaction(session, f)


def test_retry_logic(driver, read_transaction):
    # python -m pytest tests/integration/test_tx_functions.py -s -v -k test_retry_logic

    pytest.global_counter = 0

    def get_one(tx):
        result = tx.run("UNWIND [1,2,3,4] AS x RETURN x")
        records = list(result)
        pytest.global_counter += 1

        if pytest.global_counter < 3:
            database_unavailable = Neo4jError.hydrate(message="The database is not currently available to serve your request, refer to the database logs for more details. Retrying your request at a later time may succeed.", code="Neo.TransientError.Database.DatabaseUnavailable")
            raise database_unavailable

        return records

    with driver.session() as session:
        records = read_transaction(session, get_one)

        assert pytest.global_counter == 3

    del pytest.global_counter
