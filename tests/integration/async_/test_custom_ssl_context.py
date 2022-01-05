# Copyright (c) "Neo4j"
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

from ssl import SSLContext

import pytest

from neo4j import AsyncGraphDatabase

from ..._async_compat import (
    mark_async_test,
    mock,
)


@mark_async_test
async def test_custom_ssl_context_is_wraps_connection(target, auth):
    class NoNeedToGoFurtherException(Exception):
        pass

    def wrap_fail(*_, **__):
        raise NoNeedToGoFurtherException()

    fake_ssl_context = mock.create_autospec(SSLContext)
    fake_ssl_context.wrap_socket.side_effect = wrap_fail
    fake_ssl_context.wrap_bio.side_effect = wrap_fail
    driver = AsyncGraphDatabase.neo4j_driver(
        target, auth=auth, ssl_context=fake_ssl_context
    )
    async with driver:
        async with driver.session() as session:
            with pytest.raises(NoNeedToGoFurtherException):
                await session.run("RETURN 1")
    assert (fake_ssl_context.wrap_socket.call_count
            + fake_ssl_context.wrap_bio.call_count) == 1
