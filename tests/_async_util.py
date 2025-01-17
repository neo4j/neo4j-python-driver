# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
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


async def gather_cancel(*coros_or_futures):
    """
    Return a future aggregating results from the given coroutines/futures.

    A thin wrapper around asyncio.gather that cancels all coroutines/futures
    if any of them raises an exception.
    """
    futures = [asyncio.ensure_future(coro) for coro in coros_or_futures]
    try:
        await asyncio.gather(*futures)
    except:
        for future in futures:
            future.cancel()
        await asyncio.gather(*futures, return_exceptions=True)
        raise
