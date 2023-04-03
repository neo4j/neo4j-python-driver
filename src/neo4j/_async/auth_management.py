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


# from __future__ import annotations
# work around for https://github.com/sphinx-doc/sphinx/pull/10880
# make sure TAuth is resolved in the docs, else they're pretty useless

import time
import typing as t
from logging import getLogger

from .._async_compat.concurrency import AsyncLock
from .._auth_management import (
    AsyncAuthManager,
    ExpiringAuth,
)

# work around for https://github.com/sphinx-doc/sphinx/pull/10880
# make sure TAuth is resolved in the docs, else they're pretty useless
# if t.TYPE_CHECKING:
from ..api import _TAuth


log = getLogger("neo4j")


class AsyncStaticAuthManager(AsyncAuthManager):
    _auth: _TAuth

    def __init__(self, auth: _TAuth) -> None:
        self._auth = auth

    async def get_auth(self) -> _TAuth:
        return self._auth

    async def on_auth_expired(self, auth: _TAuth) -> None:
        pass


class _ExpiringAuthHolder:
    def __init__(self, auth: ExpiringAuth) -> None:
        self._auth = auth
        self._expiry = None
        if auth.expires_in is not None:
            self._expiry = time.monotonic() + auth.expires_in

    @property
    def auth(self) -> _TAuth:
        return self._auth.auth

    def expired(self) -> bool:
        if self._expiry is None:
            return False
        return time.monotonic() > self._expiry

class AsyncExpirationBasedAuthManager(AsyncAuthManager):
    _current_auth: t.Optional[_ExpiringAuthHolder]
    _provider: t.Callable[[], t.Awaitable[ExpiringAuth]]
    _lock: AsyncLock


    def __init__(
        self,
        provider: t.Callable[[], t.Awaitable[ExpiringAuth]]
    ) -> None:
        self._provider = provider
        self._current_auth = None
        self._lock = AsyncLock()

    async def _refresh_auth(self):
        self._current_auth = _ExpiringAuthHolder(await self._provider())

    async def get_auth(self) -> _TAuth:
        async with self._lock:
            auth = self._current_auth
            if auth is not None and not auth.expired():
                return auth.auth
            log.debug("[     ]  _: <TEMPORAL AUTH> refreshing (time out)")
            await self._refresh_auth()
            assert self._current_auth is not None
            return self._current_auth.auth

    async def on_auth_expired(self, auth: _TAuth) -> None:
        async with self._lock:
            cur_auth = self._current_auth
            if cur_auth is not None and cur_auth.auth == auth:
                log.debug("[     ]  _: <TEMPORAL AUTH> refreshing (error)")
                await self._refresh_auth()


class AsyncAuthManagers:
    """A collection of :class:`.AsyncAuthManager` factories."""

    @staticmethod
    def static(auth: _TAuth) -> AsyncAuthManager:
        """Create a static auth manager.

        Example::

            # NOTE: this example is for illustration purposes only.
            #       The driver will automatically wrap static auth info in a
            #       static auth manager.

            import neo4j
            from neo4j.auth_management import AsyncAuthManagers


            auth = neo4j.basic_auth("neo4j", "password")

            with neo4j.GraphDatabase.driver(
                "neo4j://example.com:7687",
                auth=AsyncAuthManagers.static(auth)
                # auth=auth  # this is equivalent
            ) as driver:
                ...  # do stuff

        :param auth: The auth to return.

        :returns:
            An instance of an implementation of :class:`.AsyncAuthManager` that
            always returns the same auth.
        """
        return AsyncStaticAuthManager(auth)

    @staticmethod
    def expiration_based(
        provider: t.Callable[[], t.Awaitable[ExpiringAuth]]
    ) -> AsyncAuthManager:
        """Create an auth manager for potentially expiring auth info.

        .. warning::

            The provider function **must not** interact with the driver in any
            way as this can cause deadlocks and undefined behaviour.

            The provider function only ever return auth information belonging
            to the same identity.
            Switching identities is undefined behavior.

        Example::

            import neo4j
            from neo4j.auth_management import (
                AsyncAuthManagers,
                ExpiringAuth,
            )


            async def auth_provider():
                # some way to getting a token
                sso_token = await get_sso_token()
                # assume we know our tokens expire every 60 seconds
                expires_in = 60

                return ExpiringAuth(
                    auth=neo4j.bearer_auth(sso_token),
                    # Include a little buffer so that we fetch a new token
                    # *before* the old one expires
                    expires_in=expires_in - 10
                )


            with neo4j.GraphDatabase.driver(
                "neo4j://example.com:7687",
                auth=AsyncAuthManagers.temporal(auth_provider)
            ) as driver:
                ...  # do stuff

        :param provider:
            A callable that provides a :class:`.ExpiringAuth` instance.

        :returns:
            An instance of an implementation of :class:`.AsyncAuthManager` that
            returns auth info from the given provider and refreshes it, calling
            the provider again, when the auth info expires (either because it's
            reached its expiry time or because the server flagged it as
            expired).
        """
        return AsyncExpirationBasedAuthManager(provider)
