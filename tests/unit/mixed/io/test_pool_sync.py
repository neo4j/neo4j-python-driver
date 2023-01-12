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


from threading import (
    Condition,
    Thread,
)
from time import sleep

from ...sync.fixtures import *  # fixtures necessary for pytest
from ...sync.io.test_neo4j_pool import *
from ._common import MultiEvent


def test_force_new_auth_blocks(opener):
    count = 0
    done = False
    condition = Condition()
    event = MultiEvent()

    def auth_provider():
        nonlocal done, condition, count
        count += 1
        if count == 1:
            return "user1", "pass1"
        event.wait(1)
        with condition:
            event.increment()
            condition.wait()
        sleep(0.1)  # block
        done = True
        return "user", "password"

    config = PoolConfig()
    config.auth = auth_provider
    pool = Neo4jPool(
        opener, config, WorkspaceConfig(), ROUTER1_ADDRESS
    )

    assert count == 0
    cx = pool.acquire(READ_ACCESS, 30, "test_db", None, None, None)
    pool.release(cx)
    assert count == 1

    def task1():
        assert count == 1
        pool.force_new_auth()
        assert count == 2

    def task2():
        event.increment()
        event.wait(2)
        with condition:
            condition.notify()
        cx = pool.acquire(READ_ACCESS, 30, "test_db", None, None, None)
        assert done  # assert waited for blocking auth provider
        pool.release(cx)

    t1 = Thread(target=task1)
    t2 = Thread(target=task2)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
