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


from asyncio import sleep, wait
from random import random

from pytest import mark


async def _run_queries(bolt_pool, d, values):
    cx = await bolt_pool.acquire(force_reset=True)
    for x in values:
        await sleep(random())
        result = await cx.run("RETURN $x", {"x": x})
        record = await result.single()
        assert record[0] == x
        d.append(x)
    await bolt_pool.release(cx, force_reset=True)


async def _run_tasks(bolt_pool, n_tasks, n_queries):
    x_range = range(n_queries)
    y_range = range(n_tasks)
    data = [list() for _ in y_range]
    cos = {_run_queries(bolt_pool, d, x_range) for d in data}
    await wait(cos)
    for d in data:
        assert d == list(x_range)


@mark.asyncio
async def test_bolt_pool_should_allow_concurrent_async_usage(bolt_pool):
    await _run_tasks(bolt_pool, 10, 50)
