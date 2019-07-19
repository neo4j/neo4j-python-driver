#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2019 "Neo4j,"
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


from pytest import mark, raises

from neo4j.api import Security
from neo4j.bolt import Bolt
from neo4j.bolt.error import BoltConnectionError, BoltTransactionError, ClientError


@mark.asyncio
async def test_good_connectivity(address, auth):
    bolt = await Bolt.open(address, auth=auth)
    assert bolt.protocol_version
    await bolt.close()


@mark.asyncio
async def test_connectivity_over_ipv4(address, auth):
    bolt = await Bolt.open(("127.0.0.1", address[1]), auth=auth)
    assert bolt.protocol_version
    await bolt.close()


@mark.asyncio
async def test_connectivity_over_ipv6(address, auth):
    bolt = await Bolt.open(("::1", address[1], 0, 0), auth=auth)
    assert bolt.protocol_version
    await bolt.close()


@mark.asyncio
async def test_bad_connectivity(address, auth):
    with raises(BoltConnectionError) as e:
        _ = await Bolt.open(("localhost", 9999), auth=auth)
    assert e.value.address == ("localhost", 9999)


@mark.asyncio
async def test_security_none(address, auth):
    bolt = await Bolt.open(address, auth=auth, security=None)
    assert not bolt.security
    await bolt.close()


@mark.asyncio
async def test_security_false(address, auth):
    bolt = await Bolt.open(address, auth=auth, security=False)
    assert not bolt.security
    await bolt.close()


# TODO: re-enable when we have a way of testing against full certs
# @mark.asyncio
# async def test_security_true(address, auth):
#     bolt = await Bolt.open(address, auth=auth, security=True)
#     assert bolt.security
#     assert bolt.security == Security.default()
#     await bolt.close()


# TODO: re-enable when we have a way of testing against full certs
# @mark.asyncio
# async def test_security_custom(address, auth):
#     bolt = await Bolt.open(address, auth=auth, security=Security())
#     assert bolt.security
#     assert bolt.security == Security()
#     await bolt.close()


@mark.asyncio
async def test_unsupported_protocol_version(address, auth):
    with raises(ValueError):
        _ = await Bolt.open(address, auth=auth, protocol_version=(1, 0))


@mark.asyncio
async def test_bad_protocol_version_format(address, auth):
    with raises(TypeError):
        _ = await Bolt.open(address, auth=auth, protocol_version="4.0")


@mark.asyncio
async def test_bad_auth(address, auth):
    with raises(ClientError) as e:
        _ = await Bolt.open(address, auth=("sneaky", "hacker"))
    error = e.value
    assert error.category == "Security"
    assert error.title == "Unauthorized"
    assert error.result is None
    assert error.transaction is None


@mark.asyncio
async def test_autocommit_transaction(address, auth):
    bolt = await Bolt.open(address, auth=auth)
    values = []
    async for record in await bolt.run("UNWIND [2, 3, 5] AS n RETURN n"):
        values.append(record[0])
    await bolt.close()
    assert values == [2, 3, 5]


@mark.asyncio
async def test_explicit_transaction_with_commit(address, auth):
    bolt = await Bolt.open(address, auth=auth)
    tx = await bolt.begin()
    assert not tx.closed
    values = []
    async for record in await tx.run("UNWIND [2, 3, 5] AS n RETURN n"):
        values.append(record[0])
    bookmark = await tx.commit()
    assert bookmark  # We can't assert anything about the content
    assert tx.closed
    await bolt.close()
    assert values == [2, 3, 5]


@mark.asyncio
async def test_explicit_transaction_with_rollback(address, auth):
    bolt = await Bolt.open(address, auth=auth)
    tx = await bolt.begin()
    assert not tx.closed
    values = []
    async for record in await tx.run("UNWIND [2, 3, 5] AS n RETURN n"):
        values.append(record[0])
    await tx.rollback()
    assert tx.closed
    await bolt.close()
    assert values == [2, 3, 5]


@mark.asyncio
async def test_autocommit_in_autocommit(address, auth):
    bolt = await Bolt.open(address, auth=auth)
    values = []
    async for r1 in await bolt.run("UNWIND [2, 3, 5] AS n RETURN n"):
        async for r2 in await bolt.run("UNWIND [7, 11, 13] AS n RETURN n"):
            values.append(r1[0] * r2[0])
    await bolt.close()
    assert values == [14, 22, 26, 21, 33, 39, 35, 55, 65]


@mark.asyncio
async def test_explicit_in_autocommit(address, auth):
    bolt = await Bolt.open(address, auth=auth)
    tx = await bolt.begin()
    with raises(BoltTransactionError):
        _ = await bolt.run("UNWIND [2, 3, 5] AS n RETURN n")
    await tx.rollback()
    await bolt.close()


@mark.asyncio
async def test_autocommit_in_explicit(address, auth):
    bolt = await Bolt.open(address, auth=auth)
    tx = await bolt.begin()
    async for _ in await tx.run("UNWIND [2, 3, 5] AS n RETURN n"):
        with raises(BoltTransactionError):
            _ = await bolt.run("UNWIND [7, 11, 13] AS n RETURN n")
    await tx.commit()
    await bolt.close()


@mark.asyncio
async def test_explicit_in_explicit(address, auth):
    bolt = await Bolt.open(address, auth=auth)
    tx = await bolt.begin()
    with raises(BoltTransactionError):
        _ = await bolt.begin()
    await tx.rollback()
    await bolt.close()


@mark.asyncio
async def test_commit_is_non_idempotent(address, auth):
    bolt = await Bolt.open(address, auth=auth)
    tx = await bolt.begin()
    values = []
    async for record in await tx.run("UNWIND [2, 3, 5] AS n RETURN n"):
        values.append(record[0])
    await tx.commit()
    with raises(BoltTransactionError):
        await tx.commit()
    await bolt.close()
    assert values == [2, 3, 5]


@mark.asyncio
async def test_rollback_is_non_idempotent(address, auth):
    bolt = await Bolt.open(address, auth=auth)
    tx = await bolt.begin()
    values = []
    async for record in await tx.run("UNWIND [2, 3, 5] AS n RETURN n"):
        values.append(record[0])
    await tx.rollback()
    with raises(BoltTransactionError):
        await tx.rollback()
    await bolt.close()
    assert values == [2, 3, 5]


@mark.asyncio
async def test_cypher_error_in_autocommit_transaction(address, auth):
    bolt = await Bolt.open(address, auth=auth)
    with raises(ClientError) as e:
        async for _ in await bolt.run("X"):
            pass
    error = e.value
    assert isinstance(error, ClientError)
    assert error.category == "Statement"
    assert error.title == "SyntaxError"


@mark.asyncio
async def test_can_resume_after_error_in_autocommit_transaction(address, auth):
    bolt = await Bolt.open(address, auth=auth)
    with raises(ClientError):
        async for _ in await bolt.run("X"):
            pass
    values = []
    async for record in await bolt.run("RETURN 1"):
        values.append(record[0])
    await bolt.close()
    assert values == [1]


@mark.asyncio
async def test_cypher_error_in_explicit_transaction(address, auth):
    bolt = await Bolt.open(address, auth=auth)
    tx = await bolt.begin()
    result1 = await tx.run("X")
    result2 = await tx.run("RETURN 1")
    with raises(ClientError) as e:
        await tx.commit()
    error = e.value
    assert isinstance(error, ClientError)
    assert error.category == "Statement"
    assert error.title == "SyntaxError"
    assert error.result is result1
    assert error.transaction is tx
    ok = await result1.consume()
    assert not ok
    ok = await result2.consume()
    assert not ok
    await bolt.close()
