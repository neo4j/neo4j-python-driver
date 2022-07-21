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


from ssl import SSLContext

import pytest

import neo4j
from neo4j._async_compat.util import Util

from ..._async_compat import mark_sync_test
from ...conftest import get_async_driver_no_warning


@mark_sync_test
def test_custom_ssl_context_wraps_connection(uri, auth, mocker):
    # Test that the driver calls either `.wrap_socket` or `.wrap_bio` on the
    # provided custom SSL context.

    class NoNeedToGoFurtherException(Exception):
        pass

    def wrap_fail(*_, **__):
        raise NoNeedToGoFurtherException()

    fake_ssl_context = mocker.create_autospec(SSLContext)
    fake_ssl_context.wrap_socket.side_effect = wrap_fail
    fake_ssl_context.wrap_bio.side_effect = wrap_fail

    if Util.is_async_code:
        driver = get_async_driver_no_warning(
            uri, auth=auth, ssl_context=fake_ssl_context
        )
    else:
        driver = neo4j.GraphDatabase.driver(
            uri, auth=auth, ssl_context=fake_ssl_context
        )
    with driver:
        with driver.session() as session:
            with pytest.raises(NoNeedToGoFurtherException):
                session.run("RETURN 1")

    assert (fake_ssl_context.wrap_socket.call_count
            + fake_ssl_context.wrap_bio.call_count) == 1
