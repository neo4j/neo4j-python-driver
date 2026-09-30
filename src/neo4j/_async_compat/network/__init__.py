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

from ... import _typing as t
from ._bolt_socket import (
    AsyncBoltSocketBase,
    BoltSocketBase,
)
from ._util import (
    AsyncNetworkUtil,
    NetworkUtil,
)


if t.TYPE_CHECKING:
    from ._http_query_api import (  # noqa: F401
        AsyncHTTPQueryAPI,
        AsyncHTTPQueryAPIFactory,
        HTTPQueryAPI,
        HTTPQueryAPIFactory,
        HTTPQueryAPIResponse,
        HTTPVerb,
        NO_DATA,
    )


__all__ = [
    "AsyncBoltSocketBase",
    "AsyncNetworkUtil",
    "BoltSocketBase",
    "NetworkUtil",
]

_http_query_api_exports = {  # noqa: RUF067
    "AsyncHTTPQueryAPI",
    "AsyncHTTPQueryAPIFactory",
    "HTTPQueryAPI",
    "HTTPQueryAPIFactory",
    "HTTPQueryAPIResponse",
    "HTTPVerb",
    "NO_DATA",
}
__all__.extend(_http_query_api_exports)  # noqa: RUF067


def __getattr__(name):
    if name in _http_query_api_exports:
        from . import _http_query_api

        for export in _http_query_api_exports:
            globals()[export] = getattr(_http_query_api, export)
        return globals()[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
