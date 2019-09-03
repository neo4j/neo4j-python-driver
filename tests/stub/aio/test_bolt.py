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


from asyncio import IncompleteReadError

from boltkit.server.stub import BoltStubService
from pytest import mark, raises

from neo4j.aio.bolt import Bolt
from neo4j.aio.bolt.error import BoltConnectionError, BoltConnectionBroken, BoltHandshakeError


@mark.asyncio
async def test_good_connectivity(script):
    async with BoltStubService.load(script("v3", "empty.script")) as stub:
        bolt = await Bolt.open(stub.addresses[0], auth=stub.auth)
        assert bolt.remote_address.host in ("localhost", "127.0.0.1", "::1")
        assert bolt.remote_address.port_number == stub.addresses[0].port_number
        assert bolt.local_address.host in ("localhost", "127.0.0.1", "::1")
        await bolt.close()


@mark.asyncio
async def test_bad_connectivity(script):
    with raises(BoltConnectionError):
        _ = await Bolt.open(("localhost", 9999), auth=())


@mark.asyncio
async def test_return_1(script):
    async with BoltStubService.load(script("v3", "return_1.script")) as stub:
        bolt = await Bolt.open(stub.addresses[0], auth=stub.auth)
        values = []
        async for record in await bolt.run("RETURN $x", {"x": 1}):
            values.append(record[0])
        await bolt.close()
        assert values == [1]


@mark.asyncio
async def test_explicit_protocol_version(script):
    async with BoltStubService.load(script("v3", "empty.script")) as stub:
        bolt = await Bolt.open(stub.addresses[0], auth=stub.auth, protocol_version=(3, 0))
        assert bolt.protocol_version == (3, 0)
        await bolt.close()


@mark.asyncio
async def test_explicit_unsupported_protocol_version(script):
    async with BoltStubService.load(script("v3", "empty.script")) as stub:
        with raises(ValueError):
            _ = await Bolt.open(stub.addresses[0], auth=stub.auth, protocol_version=(0, 1))


@mark.asyncio
async def test_illegal_protocol_version_type(script):
    async with BoltStubService.load(script("v3", "empty.script")) as stub:
        with raises(TypeError):
            _ = await Bolt.open(stub.addresses[0], auth=stub.auth, protocol_version=object())


@mark.asyncio
async def test_unusable_value_on_handshake(script):
    async with BoltStubService.load(script("v3", "unusable_value_on_handshake.script")) as stub:
        with raises(BoltHandshakeError) as e:
            await Bolt.open(stub.addresses[0], auth=stub.auth)
        assert isinstance(e.value.__cause__, ValueError)


@mark.asyncio
async def test_incomplete_read_on_handshake(script):
    async with BoltStubService.load(script("v3", "incomplete_read_on_handshake.script")) as stub:
        with raises(BoltConnectionBroken) as e:
            await Bolt.open(stub.addresses[0], auth=stub.auth)
        assert isinstance(e.value.__cause__, IncompleteReadError)


@mark.asyncio
async def test_unsupported_old_protocol_version(script):
    # TODO: fix task pending in boltkit that arises from this test
    async with BoltStubService.load(script("v3", "old_protocol.script")) as stub:
        with raises(BoltHandshakeError) as e:
            await Bolt.open(stub.addresses[0], auth=stub.auth, protocol_version=(3, 0))
        error = e.value
        assert isinstance(error, BoltHandshakeError)
        port = stub.primary_address.port_number
        assert error.address in {("localhost", port), ("127.0.0.1", port), ("::1", port)}
        assert error.request_data == (b"\x60\x60\xb0\x17"
                                      b"\x00\x00\x00\x03"
                                      b"\x00\x00\x00\x00"
                                      b"\x00\x00\x00\x00"
                                      b"\x00\x00\x00\x00")
        assert error.response_data == b"\x00\x00\x01\x00"


@mark.asyncio
async def test_incomplete_read_on_init(script):
    async with BoltStubService.load(script("v3", "incomplete_read_on_init.script")) as stub:
        with raises(BoltConnectionBroken) as e:
            await Bolt.open(stub.addresses[0], auth=stub.auth)
        assert isinstance(e.value.__cause__, IncompleteReadError)


@mark.asyncio
async def test_readonly_true(script):
    async with BoltStubService.load(script("v3", "readonly_true.script")) as stub:
        bolt = await Bolt.open(stub.addresses[0], auth=stub.auth)
        await bolt.run("RETURN 1", readonly=True)
        await bolt.close()


@mark.asyncio
async def test_readonly_false(script):
    async with BoltStubService.load(script("v3", "readonly_false.script")) as stub:
        bolt = await Bolt.open(stub.addresses[0], auth=stub.auth)
        await bolt.run("RETURN 1", readonly=False)
        await bolt.close()


@mark.asyncio
async def test_good_bookmarks_value(script):
    async with BoltStubService.load(script("v3", "good_bookmarks.script")) as stub:
        bolt = await Bolt.open(stub.addresses[0], auth=stub.auth)
        await bolt.run("RETURN 1", bookmarks=["bookmark1"])
        await bolt.close()


@mark.asyncio
async def test_bad_bookmarks_value(script):
    async with BoltStubService.load(script("v3", "empty.script")) as stub:
        bolt = await Bolt.open(stub.addresses[0], auth=stub.auth)
        with raises(TypeError):
            await bolt.run("RETURN 1", bookmarks=object())
        await bolt.close()


@mark.asyncio
async def test_good_metadata_value(script):
    async with BoltStubService.load(script("v3", "good_metadata.script")) as stub:
        bolt = await Bolt.open(stub.addresses[0], auth=stub.auth)
        await bolt.run("RETURN 1", metadata={"foo": "bar"})
        await bolt.close()


@mark.asyncio
async def test_bad_metadata_value(script):
    async with BoltStubService.load(script("v3", "empty.script")) as stub:
        bolt = await Bolt.open(stub.addresses[0], auth=stub.auth)
        with raises(TypeError):
            await bolt.run("RETURN 1", metadata=object())
        await bolt.close()


@mark.asyncio
async def test_good_timeout_value(script):
    async with BoltStubService.load(script("v3", "good_timeout.script")) as stub:
        bolt = await Bolt.open(stub.addresses[0], auth=stub.auth)
        await bolt.run("RETURN 1", timeout=15)
        await bolt.close()


@mark.asyncio
async def test_bad_timeout_value(script):
    async with BoltStubService.load(script("v3", "empty.script")) as stub:
        bolt = await Bolt.open(stub.addresses[0], auth=stub.auth)
        with raises(TypeError):
            await bolt.run("RETURN 1", timeout=object())
        await bolt.close()
