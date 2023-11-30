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

import typing as t
from logging import getLogger

from .._async_compat.concurrency import Lock
from .._auth_management import (
    AuthManager,
    expiring_auth_has_expired,
    ExpiringAuth,
)


if t.TYPE_CHECKING:
    from ..api import _TAuth
    from ..exceptions import Neo4jError


log = getLogger("neo4j")


class StaticAuthManager(AuthManager):
    _auth: _TAuth

    def __init__(self, auth: _TAuth) -> None:
        self._auth = auth

    def get_auth(self) -> _TAuth:
        return self._auth

    def handle_security_exception(
        self, auth: _TAuth, error: Neo4jError
    ) -> bool:
        return False


class Neo4jAuthTokenManager(AuthManager):
    _current_auth: t.Optional[ExpiringAuth]
    _provider: t.Callable[[], t.Union[ExpiringAuth]]
    _handled_codes: t.FrozenSet[str]
    _lock: Lock

    def __init__(
        self,
        provider: t.Callable[[], t.Union[ExpiringAuth]],
        handled_codes: t.FrozenSet[str]
    ) -> None:
        self._provider = provider
        self._handled_codes = handled_codes
        self._current_auth = None
        self._lock = Lock()

    def _refresh_auth(self):
        self._current_auth = self._provider()
        if self._current_auth is None:
            raise TypeError(
                "Auth provider function passed to expiration_based "
                "AuthManager returned None, expected ExpiringAuth"
            )

    def get_auth(self) -> _TAuth:
        with self._lock:
            auth = self._current_auth
            if auth is None or expiring_auth_has_expired(auth):
                log.debug("[     ]  _: <AUTH MANAGER> refreshing (%s)",
                          "init" if auth is None else "time out")
                self._refresh_auth()
                auth = self._current_auth
                assert auth is not None
            return auth.auth

    def handle_security_exception(
        self, auth: _TAuth, error: Neo4jError
    ) -> bool:
        if error.code not in self._handled_codes:
            return False
        with self._lock:
            cur_auth = self._current_auth
            if cur_auth is not None and cur_auth.auth == auth:
                log.debug("[     ]  _: <AUTH MANAGER> refreshing (error %s)",
                          error.code)
                self._refresh_auth()
            return True


class AuthManagers:
    """A collection of :class:`.AuthManager` factories.

    .. versionadded:: 5.8

    .. versionchanged:: 5.12

        * Method ``expiration_based()`` was renamed to :meth:`bearer`.
        * Added :meth:`basic`.

    .. versionchanged:: 5.14 Stabilized from preview.
    """

    @staticmethod
    def static(auth: _TAuth) -> AuthManager:
        """Create a static auth manager.

        Example::

            # NOTE: this example is for illustration purposes only.
            #       The driver will automatically wrap static auth info in a
            #       static auth manager.

            import neo4j
            from neo4j.auth_management import AuthManagers


            auth = neo4j.basic_auth("neo4j", "password")

            with neo4j.GraphDatabase.driver(
                "neo4j://example.com:7687",
                auth=AuthManagers.static(auth)
                # auth=auth  # this is equivalent
            ) as driver:
                ...  # do stuff

        :param auth: The auth to return.

        :returns:
            An instance of an implementation of :class:`.AuthManager` that
            always returns the same auth.

        .. versionadded:: 5.8

        .. versionchanged:: 5.14 Stabilized from preview.
        """
        return StaticAuthManager(auth)

    @staticmethod
    def basic(
        provider: t.Callable[[], t.Union[_TAuth]]
    ) -> AuthManager:
        """Create an auth manager handling basic auth password rotation.

        This factory wraps the provider function in an auth manager
        implementation that caches the provides auth info until either the
        server notifies the driver that the auth info is expired (by returning
        an error that indicates that basic auth has changed).

        .. warning::

            The provider function **must not** interact with the driver in any
            way as this can cause deadlocks and undefined behaviour.

            The provider function must only ever return auth information
            belonging to the same identity.
            Switching identities is undefined behavior.
            You may use session-level authentication for such use-cases
            :ref:`session-auth-ref`.

        Example::

            import neo4j
            from neo4j.auth_management import (
                AuthManagers,
                ExpiringAuth,
            )


            def auth_provider():
                # some way of getting a token
                user, password = get_current_auth()
                return (user, password)


            with neo4j.GraphDatabase.driver(
                "neo4j://example.com:7687",
                auth=AuthManagers.basic(auth_provider)
            ) as driver:
                ...  # do stuff

        :param provider:
            A callable that provides a :class:`.ExpiringAuth` instance.

        :returns:
            An instance of an implementation of :class:`.AuthManager` that
            returns auth info from the given provider and refreshes it, calling
            the provider again, when the auth info expires (because the server
            flagged it as expired).

        .. versionadded:: 5.12

        .. versionchanged:: 5.14 Stabilized from preview.
        """
        handled_codes = frozenset(("Neo.ClientError.Security.Unauthorized",))

        def wrapped_provider() -> ExpiringAuth:
            return ExpiringAuth(provider())

        return Neo4jAuthTokenManager(wrapped_provider, handled_codes)

    @staticmethod
    def bearer(
        provider: t.Callable[[], t.Union[ExpiringAuth]]
    ) -> AuthManager:
        """Create an auth manager for potentially expiring bearer auth tokens.

        This factory wraps the provider function in an auth manager
        implementation that caches the provides auth info until either the
        ``ExpiringAuth.expires_at`` is exceeded the server notifies the driver
        that the auth info is expired (by returning an error that indicates
        that the bearer auth token has expired).

        .. warning::

            The provider function **must not** interact with the driver in any
            way as this can cause deadlocks and undefined behaviour.

            The provider function must only ever return auth information
            belonging to the same identity.
            Switching identities is undefined behavior.
            You may use session-level authentication for such use-cases
            :ref:`session-auth-ref`.

        Example::

            import neo4j
            from neo4j.auth_management import (
                AuthManagers,
                ExpiringAuth,
            )


            def auth_provider():
                # some way of getting a token
                sso_token = get_sso_token()
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
                auth=AuthManagers.bearer(auth_provider)
            ) as driver:
                ...  # do stuff

        :param provider:
            A callable that provides a :class:`.ExpiringAuth` instance.

        :returns:
            An instance of an implementation of :class:`.AuthManager` that
            returns auth info from the given provider and refreshes it, calling
            the provider again, when the auth info expires (either because it's
            reached its expiry time or because the server flagged it as
            expired).

        .. versionadded:: 5.12

        .. versionchanged:: 5.14 Stabilized from preview.
        """
        handled_codes = frozenset((
            "Neo.ClientError.Security.TokenExpired",
            "Neo.ClientError.Security.Unauthorized",
        ))
        return Neo4jAuthTokenManager(provider, handled_codes)
