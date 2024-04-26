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

from .._async_compat.concurrency import (
    AsyncCooperativeLock,
    AsyncLock,
)
from .._auth_management import (
    AsyncAuthManager,
    AsyncClientCertificateProvider,
    ClientCertificate,
    expiring_auth_has_expired,
    ExpiringAuth,
)
from .._meta import preview


if t.TYPE_CHECKING:
    from ..api import _TAuth
    from ..exceptions import Neo4jError


log = getLogger("neo4j.auth_management")


class AsyncStaticAuthManager(AsyncAuthManager):
    _auth: _TAuth

    def __init__(self, auth: _TAuth) -> None:
        self._auth = auth

    async def get_auth(self) -> _TAuth:
        return self._auth

    async def handle_security_exception(
        self, auth: _TAuth, error: Neo4jError
    ) -> bool:
        return False


class AsyncNeo4jAuthTokenManager(AsyncAuthManager):
    _current_auth: t.Optional[ExpiringAuth]
    _provider: t.Callable[[], t.Awaitable[ExpiringAuth]]
    _handled_codes: t.FrozenSet[str]
    _lock: AsyncLock

    def __init__(
        self,
        provider: t.Callable[[], t.Awaitable[ExpiringAuth]],
        handled_codes: t.FrozenSet[str]
    ) -> None:
        self._provider = provider
        self._handled_codes = handled_codes
        self._current_auth = None
        self._lock = AsyncLock()

    async def _refresh_auth(self):
        try:
            self._current_auth = await self._provider()
        except BaseException as e:
            log.error("[     ]  _: <AUTH MANAGER> provider failed: %r", e)
            raise
        if self._current_auth is None:
            raise TypeError(
                "Auth provider function passed to expiration_based "
                "AuthManager returned None, expected ExpiringAuth"
            )

    async def get_auth(self) -> _TAuth:
        async with self._lock:
            auth = self._current_auth
            if auth is None or expiring_auth_has_expired(auth):
                log.debug("[     ]  _: <AUTH MANAGER> refreshing (%s)",
                          "init" if auth is None else "time out")
                await self._refresh_auth()
                auth = self._current_auth
                assert auth is not None
            return auth.auth

    async def handle_security_exception(
        self, auth: _TAuth, error: Neo4jError
    ) -> bool:
        if error.code not in self._handled_codes:
            return False
        async with self._lock:
            cur_auth = self._current_auth
            if cur_auth is not None and cur_auth.auth == auth:
                log.debug("[     ]  _: <AUTH MANAGER> refreshing (error %s)",
                          error.code)
                await self._refresh_auth()
            return True


class AsyncAuthManagers:
    """A collection of :class:`.AsyncAuthManager` factories.

    .. versionadded:: 5.8

    .. versionchanged:: 5.12

        * Method ``expiration_based()`` was renamed to :meth:`bearer`.
        * Added :meth:`basic`.

    .. versionchanged:: 5.14 Stabilized from preview.
    """

    @staticmethod
    def static(auth: _TAuth) -> AsyncAuthManager:
        """Create a static auth manager.

        The manager will always return the auth info provided at its creation.

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

        .. versionadded:: 5.8

        .. versionchanged:: 5.14 Stabilized from preview.
        """
        return AsyncStaticAuthManager(auth)

    @staticmethod
    def basic(
        provider: t.Callable[[], t.Awaitable[_TAuth]]
    ) -> AsyncAuthManager:
        """Create an auth manager handling basic auth password rotation.

        This factory wraps the provider function in an auth manager
        implementation that caches the provided auth info until the server
        notifies the driver that the auth info has expired (by returning
        an error that indicates that the password is invalid).

        Note that this implies that the provider function will be called again
        if it provides wrong auth info, potentially deferring failure due to a
        wrong password or username.

        .. warning::

            The provider function **must not** interact with the driver in any
            way as this can cause deadlocks and undefined behaviour.

            The provider function must only ever return auth information
            belonging to the same identity.
            Switching identities is undefined behavior.
            You may use :ref:`session-level authentication<session-auth-ref>`
            for such use-cases.

        Example::

            import neo4j
            from neo4j.auth_management import (
                AsyncAuthManagers,
                ExpiringAuth,
            )


            async def auth_provider():
                # some way of getting a token
                user, password = await get_current_auth()
                return (user, password)


            with neo4j.GraphDatabase.driver(
                "neo4j://example.com:7687",
                auth=AsyncAuthManagers.basic(auth_provider)
            ) as driver:
                ...  # do stuff

        :param provider:
            A callable that provides new auth info whenever the server notifies
            the driver that the previous auth info is invalid.

        :returns:
            An instance of an implementation of :class:`.AsyncAuthManager` that
            returns auth info from the given provider and refreshes it, calling
            the provider again, when the auth info was rejected by the server.

        .. versionadded:: 5.12

        .. versionchanged:: 5.14 Stabilized from preview.
        """
        handled_codes = frozenset(("Neo.ClientError.Security.Unauthorized",))

        async def wrapped_provider() -> ExpiringAuth:
            return ExpiringAuth(await provider())

        return AsyncNeo4jAuthTokenManager(wrapped_provider, handled_codes)

    @staticmethod
    def bearer(
        provider: t.Callable[[], t.Awaitable[ExpiringAuth]]
    ) -> AsyncAuthManager:
        """Create an auth manager for potentially expiring bearer auth tokens.

        This factory wraps the provider function in an auth manager
        implementation that caches the provided auth info until either the
        :attr:`.ExpiringAuth.expires_at` exceeded or the server notified the
        driver that the auth info has expired (by returning an error that
        indicates that the bearer auth token has expired).

        .. warning::

            The provider function **must not** interact with the driver in any
            way as this can cause deadlocks and undefined behaviour.

            The provider function must only ever return auth information
            belonging to the same identity.
            Switching identities is undefined behavior.
            You may use :ref:`session-level authentication<session-auth-ref>`
            for such use-cases.

        Example::

            import neo4j
            from neo4j.auth_management import (
                AsyncAuthManagers,
                ExpiringAuth,
            )


            async def auth_provider():
                # some way of getting a token
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
                auth=AsyncAuthManagers.bearer(auth_provider)
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

        .. versionadded:: 5.12

        .. versionchanged:: 5.14 Stabilized from preview.
        """
        handled_codes = frozenset((
            "Neo.ClientError.Security.TokenExpired",
            "Neo.ClientError.Security.Unauthorized",
        ))
        return AsyncNeo4jAuthTokenManager(provider, handled_codes)


class _AsyncStaticClientCertificateProvider(AsyncClientCertificateProvider):
    _cert: t.Optional[ClientCertificate]

    def __init__(self, cert: ClientCertificate) -> None:
        self._cert = cert

    async def get_certificate(self) -> t.Optional[ClientCertificate]:
        cert, self._cert = self._cert, None
        return cert


@preview("Mutual TLS is a preview feature.")
class AsyncRotatingClientCertificateProvider(AsyncClientCertificateProvider):
    """
    Implementation of a certificate provider that can rotate certificates.

    The provider will make the driver use the initial certificate for all
    connections until the certificate is updated using the
    :meth:`update_certificate` method.
    From that point on, the new certificate will be used for all new
    connections until :meth:`update_certificate` is called again and so on.

    **This is a preview** (see :ref:`filter-warnings-ref`).
    It might be changed without following the deprecation policy.
    See also
    https://github.com/neo4j/neo4j-python-driver/wiki/preview-features

    Example::

        from neo4j import AsyncGraphDatabase
        from neo4j.auth_management import (
            ClientCertificate,
            AsyncClientCertificateProviders,
        )


        provider = AsyncClientCertificateProviders.rotating(
            ClientCertificate(
                certfile="path/to/certfile.pem",
                keyfile="path/to/keyfile.pem",
                password=lambda: "super_secret_password"
            )
        )
        driver = AsyncGraphDatabase.driver(
           # secure driver must be configured for client certificate
           # to be used: (...+s[sc] scheme or encrypted=True)
           "neo4j+s://example.com:7687",
           # auth still required as before, unless server is configured to not
           # use authentication
           auth=("neo4j", "password"),
           client_certificate=provider
        )

        # do work with the driver, until the certificate needs to be rotated
        ...

        await provider.update_certificate(
            ClientCertificate(
                certfile="path/to/new/certfile.pem",
                keyfile="path/to/new/keyfile.pem",
                password=lambda: "new_super_secret_password"
            )
        )

        # do more work with the driver, until the certificate needs to be
        # rotated again
        ...

    :param initial_cert: The certificate to use initially.

    .. versionadded:: 5.19
    """
    def __init__(self, initial_cert: ClientCertificate) -> None:
        self._cert: t.Optional[ClientCertificate] = initial_cert
        self._lock = AsyncCooperativeLock()

    async def get_certificate(self) -> t.Optional[ClientCertificate]:
        async with self._lock:
            cert, self._cert = self._cert, None
            return cert

    async def update_certificate(self, cert: ClientCertificate) -> None:
        """
        Update the certificate to use for new connections.
        """
        async with self._lock:
            self._cert = cert


class AsyncClientCertificateProviders:
    """A collection of :class:`.AsyncClientCertificateProvider` factories.

    **This is a preview** (see :ref:`filter-warnings-ref`).
    It might be changed without following the deprecation policy.
    See also
    https://github.com/neo4j/neo4j-python-driver/wiki/preview-features

    .. versionadded:: 5.19
    """
    @staticmethod
    @preview("Mutual TLS is a preview feature.")
    def static(cert: ClientCertificate) -> AsyncClientCertificateProvider:
        """
        Create a static client certificate provider.

        The provider simply makes the driver use the given certificate for all
        connections.
        """
        return _AsyncStaticClientCertificateProvider(cert)

    @staticmethod
    @preview("Mutual TLS is a preview feature.")
    def rotating(
        initial_cert: ClientCertificate
    ) -> AsyncRotatingClientCertificateProvider:
        """
        Create certificate provider that allows for rotating certificates.

        .. seealso:: :class:`.AsyncRotatingClientCertificateProvider`
        """
        return AsyncRotatingClientCertificateProvider(initial_cert)
