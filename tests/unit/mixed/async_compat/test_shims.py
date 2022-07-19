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


import asyncio
import sys

import pytest

from neo4j._async_compat import shims

from ...._async_compat import mark_async_test


@pytest.mark.skipif(
    sys.version_info < (3, 8),
    reason="wait_for is only broken in Python 3.8+"
)
@mark_async_test
async def test_wait_for_shim_is_necessary_starting_from_3x8():
    # when this tests fails, the shim became superfluous
    inner = asyncio.get_event_loop().create_future()
    outer = asyncio.wait_for(inner, 0.1)
    outer_future = asyncio.ensure_future(outer)
    await asyncio.sleep(0)
    inner.set_result(None)  # inner is done
    outer_future.cancel()  # AND outer got cancelled

    # this should propagate the cancellation, but it's broken :/
    await outer_future


@pytest.mark.skipif(
    sys.version_info >= (3, 8),
    reason="wait_for is only broken in Python 3.8+"
)
@mark_async_test
async def test_wait_for_shim_is_not_necessary_prior_to_3x8():
    inner = asyncio.get_event_loop().create_future()
    outer = asyncio.wait_for(inner, 0.1)
    outer_future = asyncio.ensure_future(outer)
    await asyncio.sleep(0)
    inner.set_result(None)  # inner is done
    outer_future.cancel()  # AND outer got cancelled

    with pytest.raises(asyncio.CancelledError):
        await outer_future


@mark_async_test
async def test_wait_for_shim_propagates_cancellation():
    inner = asyncio.get_event_loop().create_future()
    outer = shims.wait_for(inner, 0.1)
    outer_future = asyncio.ensure_future(outer)
    await asyncio.sleep(0)
    inner.set_result(None)  # inner is done
    outer_future.cancel()  # AND outer got cancelled

    with pytest.raises(asyncio.CancelledError):
        await outer_future
