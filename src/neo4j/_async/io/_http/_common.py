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


from __future__ import annotations

import logging

from .... import _typing as t
from ...._async_compat.util import AsyncUtil
from ....exceptions import Neo4jError


if t.TYPE_CHECKING:
    from ...._async_compat.network import HTTPQueryAPIResponse


log = logging.getLogger("neo4j.io")


class ResponseHandler:
    def __init__(self, **handlers):
        self.handlers = handlers

    async def on_records(self, records):
        """Handle one or more RECORD messages been received."""
        handler = self.handlers.get("on_records")
        await AsyncUtil.callback(handler, records)

    async def on_success(self, metadata):
        """Handle a SUCCESS message been received."""
        handler = self.handlers.get("on_success")
        await AsyncUtil.callback(handler, metadata)

        if not metadata.get("has_more"):
            handler = self.handlers.get("on_summary")
            await AsyncUtil.callback(handler)

    async def on_failure(self, metadata):
        handler = self.handlers.get("on_failure")
        await AsyncUtil.callback(handler, metadata)
        handler = self.handlers.get("on_summary")
        await AsyncUtil.callback(handler)
        raise self._hydrate_error(metadata)

    async def on_ignored(self, metadata=None):
        """Handle an IGNORED message been received."""
        handler = self.handlers.get("on_ignored")
        await AsyncUtil.callback(handler, metadata)
        handler = self.handlers.get("on_summary")
        await AsyncUtil.callback(handler)

    def _hydrate_error(self, metadata):
        return Neo4jError._hydrate_neo4j(**metadata)


def generic_http_error(res: HTTPQueryAPIResponse) -> RuntimeError:
    msg = f"HTTP error {res.status}: {res.body}"
    if res.reason is not None:
        msg += f" - ({res.reason})"
    return RuntimeError(msg)
