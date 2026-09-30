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

import abc
import time
import traceback
from logging import getLogger

from .... import _typing as t
from ...._async_compat.concurrency import (
    AsyncCooperativeLock,
    AsyncLock,
)
from ...._async_compat.network import (
    AsyncHTTPQueryAPIFactory,
    HTTPVerb,
)
from ...._async_compat.util import AsyncUtil
from ...._io import HTTPServerInfo
from ...._logging import LazyStr
from ....exceptions import ServiceUnavailable
from ...config import AsyncPoolConfig
from .._connection import AsyncConnection
from ._common import generic_http_error


if t.TYPE_CHECKING:
    from ...._addressing import Address
    from ...._auth_management import (
        AsyncAuthManager,
        AuthManager,
    )
    from .._pool import AsyncHttpV2Pool
    from . import AsyncHTTPQueryAPI

log = getLogger("neo4j.io")


class IdGenerator:
    _next_id: int
    _lock: AsyncCooperativeLock

    _MAX = 2**16

    def __init__(self) -> None:
        self._next_id = 1
        self._lock = AsyncCooperativeLock()

    async def next_id(self) -> int:
        async with self._lock:
            current_id = self._next_id
            self._next_id = max((self._next_id + 1) % IdGenerator._MAX, 1)
        return current_id


class AsyncHttpConnectionFactory:
    _server_agent_cache: _ServerInfoCache
    _id_generator: t.ClassVar[IdGenerator] = IdGenerator()
    _address: Address
    _path: str

    def __init__(self, address: Address, path: str) -> None:
        self._address = address
        self._path = path
        if not self._path.startswith("/"):
            self._path = f"/{self._path}"
        self._server_agent_cache = AsyncHttpConnectionFactory._ServerInfoCache(
            address=address, path=self._path, log_id=0
        )

    async def open(
        self,
        factory: AsyncHTTPQueryAPIFactory,
        *,
        auth_manager: AsyncAuthManager | AuthManager,
        routing_context: dict[str, str] | None,
        pool_config: AsyncPoolConfig | None,
    ) -> AsyncHttpConnection:
        assert routing_context is None, "Found routing context over HTTP"
        if pool_config is None:
            pool_config = AsyncPoolConfig()
        auth = await AsyncUtil.callback(auth_manager.get_auth)
        query_api = await factory.new_http_query_api(
            self._address,
            self._path,
            pool_config=pool_config,
        )
        id_ = await self._id_generator.next_id()

        from ._http2 import AsyncHttpV2

        http_cls = AsyncHttpV2

        server_info = await self._server_agent_cache.get(factory, pool_config)

        return http_cls(
            self._address,
            query_api=query_api,
            auth=auth,
            auth_manager=auth_manager,
            id_=id_,
            http_server_info=server_info,
        )

    class _ServerInfoCache:
        _MAX_AGE: t.Final[float] = 60.0

        _value: HTTPServerInfo | None
        _last_fetch: float
        _lock: AsyncLock
        _log_id: int
        _address: Address
        _path: str

        def __init__(
            self, *, address: Address, path: str, log_id: int
        ) -> None:
            self._value = None
            self._last_fetch = float("-inf")
            self._lock = AsyncLock()
            self._address = address
            self._path = path
            self._log_id = log_id

        async def get(
            self,
            factory: AsyncHTTPQueryAPIFactory,
            pool_config: AsyncPoolConfig,
        ) -> HTTPServerInfo:
            age = time.monotonic() - self._last_fetch
            if self._value is not None and age <= self._MAX_AGE:
                return self._value
            async with self._lock:
                # check if value has been update while waiting for the lock
                age = time.monotonic() - self._last_fetch
                if self._value is not None and age <= self._MAX_AGE:
                    return self._value
                self._value = await self._fetch(factory, pool_config)
                return self._value

        async def _fetch(
            self,
            factory: AsyncHTTPQueryAPIFactory,
            pool_config: AsyncPoolConfig,
        ) -> HTTPServerInfo:
            query_api = await factory.new_http_query_api(
                self._address,
                self._path,
                pool_config=pool_config,
            )
            try:
                return await self._fetch_unguarded(query_api)
            except Exception as e:
                log.warning(
                    "[#%04X]  _: Failed to fetch server info: %r",
                    self._log_id,
                    e,
                )
                log.debug(
                    "[#%04X]  _: %s",
                    self._log_id,
                    LazyStr(
                        lambda exc: "".join(traceback.format_exception(exc)), e
                    ),
                )
                if isinstance(e, AsyncHTTPQueryAPIFactory.CONNECTION_ERRORS):
                    raise ServiceUnavailable(
                        f"Failed to fetch discovery endpoint: {e}"
                    ) from e
                raise
            finally:
                self._last_fetch = time.monotonic()
                await query_api.close()

        async def _fetch_unguarded(
            self,
            query_api: AsyncHTTPQueryAPI,
        ) -> HTTPServerInfo:
            res = await query_api.request(
                HTTPVerb.GET,
                "/",
                headers={"Accept": "application/json"},
                log_id=self._log_id,
            )
            if res.status >= 300:
                raise generic_http_error(res)
            neo4j_version = res.body["neo4j_version"]
            if not isinstance(neo4j_version, str):
                raise TypeError(
                    "Expected 'neo4j_version' to be str, "
                    f"got {type(neo4j_version)}"
                )
            bolt_version = None  # not yet exposed by the server
            return HTTPServerInfo(
                neo4j_version=neo4j_version, bolt_version=bolt_version
            )


class AsyncHttpConnection(AsyncConnection, abc.ABC):
    pool: AsyncHttpV2Pool | None = None

    @abc.abstractmethod
    async def close(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def kill(self) -> None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def connection_id(self) -> int:
        raise NotImplementedError
