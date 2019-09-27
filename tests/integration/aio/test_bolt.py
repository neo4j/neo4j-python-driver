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


from asyncio import sleep, wait, wait_for, TimeoutError

from pytest import mark, raises

from neo4j.aio import Bolt, BoltPool
from neo4j.errors import BoltConnectionError, BoltTransactionError, ClientError


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
    bolt = await Bolt.open(address, auth=auth, secure=None)
    assert not bolt.secure
    await bolt.close()


@mark.asyncio
async def test_security_false(address, auth):
    bolt = await Bolt.open(address, auth=auth, secure=False)
    assert not bolt.secure
    await bolt.close()


# TODO: re-enable when we have a way of testing against full certs
# @mark.asyncio
# async def test_security_true(address, auth):
#     bolt = await Bolt.open(address, auth=auth, secure=True)
#     assert bolt.security
#     assert bolt.security == Security.default()
#     await bolt.close()


# TODO: re-enable when we have a way of testing against full certs
# @mark.asyncio
# async def test_security_custom(address, auth):
#     bolt = await Bolt.open(address, auth=auth, secure=...)
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
async def test_discarded_autocommit_transaction(address, auth):
    bolt = await Bolt.open(address, auth=auth)
    values = []
    async for record in await bolt.run("UNWIND [2, 3, 5] AS n RETURN n", discard=True):
        values.append(record[0])
    await bolt.close()
    assert values == []


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


@mark.asyncio
async def test_clean_transaction_function(bolt):

    async def work(tx):
        product = 1
        async for record in await tx.run("UNWIND [2, 3, 5] AS n RETURN n"):
            product *= record[0]
        return product

    value = await bolt.run_tx(work)
    assert value == 30


@mark.asyncio
async def test_dirty_transaction_function(bolt):
    await bolt.run("MATCH (_) DETACH DELETE _", discard=True)

    created = []

    async def create_nodes(tx):
        node_id = await tx.evaluate("CREATE (a) RETURN id(a)")
        created.append(node_id)
        raise RuntimeError("This should trigger a rollback")

    async def count_nodes(tx):
        return await tx.evaluate("MATCH (a) WHERE id(a) = $x "
                                 "RETURN count(a)", {"x": created[0]})

    with raises(RuntimeError):
        _ = await bolt.run_tx(create_nodes)

    assert len(created) == 1
    matched = await bolt.run_tx(count_nodes)
    assert matched == 0


@mark.asyncio
async def test_pool_exhaustion(opener, address):
    pool = BoltPool(opener, address, max_size=3)
    first = await pool.acquire()
    second = await pool.acquire()
    third = await pool.acquire()
    assert isinstance(first, Bolt)
    assert isinstance(second, Bolt)
    assert isinstance(third, Bolt)
    with raises(TimeoutError):
        _ = await wait_for(pool.acquire(), timeout=1)


@mark.asyncio
async def test_pool_reuse(opener, address):
    pool = BoltPool(opener, address, max_size=3)
    first = await pool.acquire()
    second = await pool.acquire()
    third = await pool.acquire()
    assert first is not second and second is not third and first is not third
    await pool.release(second)
    fourth = await pool.acquire()
    assert fourth is second


@mark.asyncio
async def test_pool_release_notifies_acquire(opener, address):
    pool = BoltPool(opener, address, max_size=1)
    first = await pool.acquire()

    async def delayed_release():
        await sleep(1)
        await pool.release(first)

    done, pending = await wait([
        delayed_release(),
        pool.acquire(),
    ])
    assert len(done) == 2
    assert len(pending) == 0
    assert pool.size == 1
    for future in done:
        result = future.result()
        assert result is None or result is first


@mark.asyncio
async def test_default_pool_open_and_close(bolt_pool, address):
    assert bolt_pool.address == address


@mark.asyncio
async def test_closing_pool_with_free_connections(opener, address):
    pool = BoltPool(opener, address, max_size=3)
    first = await pool.acquire()
    second = await pool.acquire()
    third = await pool.acquire()
    await pool.release(first)
    await pool.release(second)
    await pool.release(third)
    await pool.close()
    assert first.closed
    assert second.closed
    assert third.closed


@mark.asyncio
async def test_closing_pool_with_in_use_connections(opener, address):
    pool = BoltPool(opener, address, max_size=3)
    first = await pool.acquire()
    second = await pool.acquire()
    third = await pool.acquire()
    await pool.close()
    assert first.closed
    assert second.closed
    assert third.closed


@mark.asyncio
async def test_expired_connections_are_not_returned_to_pool(opener, address):
    pool = BoltPool(opener, address, max_size=1, max_age=0.25)
    assert pool.size == 0
    assert pool.in_use == 0
    cx = await pool.acquire()
    assert pool.size == 1
    assert pool.in_use == 1
    await sleep(0.5)
    await pool.release(cx)
    assert pool.size == 0
    assert pool.in_use == 0
    assert cx.closed


@mark.asyncio
async def test_closed_connections_are_not_returned_to_pool(opener, address):
    pool = BoltPool(opener, address, max_size=1)
    assert pool.size == 0
    assert pool.in_use == 0
    cx = await pool.acquire()
    assert pool.size == 1
    assert pool.in_use == 1
    await cx.close()
    await pool.release(cx)
    assert pool.size == 0
    assert pool.in_use == 0


@mark.asyncio
async def test_cannot_release_already_released_connection(bolt_pool):
    cx = await bolt_pool.acquire()
    await bolt_pool.release(cx)
    with raises(ValueError):
        await bolt_pool.release(cx)


@mark.asyncio
async def test_cannot_release_unowned_connection(bolt_pool, address, auth):
    cx = await Bolt.open(address, auth=auth)
    with raises(ValueError):
        await bolt_pool.release(cx)
