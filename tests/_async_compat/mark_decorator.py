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


import pytest
import pytest_asyncio

from ..env import IS_WIN


# On Windows a per-test asyncio event loop exhausts the ephemeral
# port range. Heavily parametrized async tests churn thousands of short-lived
# sockets into TIME_WAIT (OSError WinError 10055 / WSAENOBUFS),
# which manifests as a hung or failing Windows CI job. Sharing one event loop
# per module on that platform removes the churn; every other platform keeps the
# default per-test loop.
if IS_WIN:
    mark_async_test = pytest.mark.asyncio(loop_scope="module")
else:
    mark_async_test = pytest.mark.asyncio

async_fixture = pytest_asyncio.fixture


def mark_sync_test(f):
    return f


fixture = pytest.fixture


class AsyncTestDecorators:
    mark_async_only_test = mark_async_test


class TestDecorators:
    @staticmethod
    def mark_async_only_test(f):
        skip_decorator = pytest.mark.skip("Async only test")
        return skip_decorator(f)
