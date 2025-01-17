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


import random
from collections import defaultdict
from concurrent.futures import (
    as_completed,
    ThreadPoolExecutor,
)
from time import monotonic

from neo4j._sync.home_db_cache import HomeDbCache


# No async equivalent exists, because the async home db cache is not really
# async. As there's no IO involved, there's no need for locking in async world.
def test_concurrent_home_db_cache_access() -> None:
    workers = 25
    duration = 5
    value_pool_size = 50

    cache = HomeDbCache(ttl=0.001, max_size=value_pool_size - 2)
    keys = tuple(
        cache.compute_key(user, None)
        for user in map(str, range(1, value_pool_size + 1))
    )

    def worker(worked_id, end):
        non_checks = checks = 0

        value_counter = defaultdict(int)
        while monotonic() < end:
            for _ in range(20):  # to not check time too often
                i = random.randint(0, len(keys) - 1)
                value_count = value_counter[i]
                key = keys[i]
                rand = random.random()
                if rand < 0.1:
                    cache.set(key, None)
                    res = cache.get(key)
                    # Never want to read back this worker's own value
                    assert res is None or not res.startswith(f"{worked_id}-")
                elif rand < 0.55:
                    value_counter[i] += 1
                    value = f"{worked_id}-{value_count + 1}"
                    cache.set(key, value)
                    res = cache.get(key)
                    if res is not None and res.startswith(f"{worked_id}-"):
                        # never want to read back an old value of this worker
                        checks += 1
                        assert res == value
                    else:
                        non_checks += 1
                else:
                    res = cache.get(key)
                    if res is not None and res.startswith(f"{worked_id}-"):
                        # never want to read back an old value of this worker
                        checks += 1
                        assert res == f"{worked_id}-{value_count}"
                    else:
                        non_checks += 1

        # import json
        # print(
        #     f"{worked_id}:\n"
        #     f"{json.dumps(value_counter, indent=2)}\n"
        #     f"checks: {checks}, non_checks: {non_checks}\n",
        #     flush=True,
        # )

    with ThreadPoolExecutor(max_workers=workers) as executor:
        end = monotonic() + duration
        futures = (executor.submit(worker, i, end) for i in range(workers))
        for future in as_completed(futures):
            future.result()
