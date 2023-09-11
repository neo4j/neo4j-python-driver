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


from __future__ import annotations

import abc
import time
import typing as t
from dataclasses import dataclass

from ._meta import preview
from .exceptions import Neo4jError


if t.TYPE_CHECKING:
    from .api import _TAuth


@preview("Auth managers are a preview feature.")
@dataclass
class ExpiringAuth:
    """Represents potentially expiring authentication information.

    This class is used with :meth:`.AuthManagers.expiration_based` and
    :meth:`.AsyncAuthManagers.expiration_based`.

    :param auth: The authentication information.
    :param expires_at:
        Unix timestamp (seconds since 1970-01-01 00:00:00 UTC)
        indicating when the authentication information expires.
        If :data:`None`, the authentication information is considered to not
        expire until the server explicitly indicates so.

    **This is a preview** (see :ref:`filter-warnings-ref`).
    It might be changed without following the deprecation policy.
    See also https://github.com/neo4j/neo4j-python-driver/wiki/preview-features

    .. seealso::
        :meth:`.AuthManagers.expiration_based`,
        :meth:`.AsyncAuthManagers.expiration_based`

    .. versionadded:: 5.8

    .. versionchanged:: 5.9
        Removed parameter and attribute ``expires_in`` (relative expiration
        time). Replaced with ``expires_at`` (absolute expiration time).
        :meth:`.expires_in` can be used to create an :class:`.ExpiringAuth`
        with a relative expiration time.
    """
    auth: _TAuth
    expires_at: t.Optional[float] = None

    def expires_in(self, seconds: float) -> ExpiringAuth:
        """Return a (flat) copy of this object with a new expiration time.

        This is a convenience method for creating an :class:`.ExpiringAuth`
        for a relative expiration time ("expires in" instead of "expires at").

            >>> import time, freezegun
            >>> with freezegun.freeze_time("1970-01-01 00:00:40"):
            ...     ExpiringAuth(("user", "pass")).expires_in(2)
            ExpiringAuth(auth=('user', 'pass'), expires_at=42.0)
            >>> with freezegun.freeze_time("1970-01-01 00:00:40"):
            ...     ExpiringAuth(("user", "pass"), time.time() + 2)
            ExpiringAuth(auth=('user', 'pass'), expires_at=42.0)

        :param seconds:
            The number of seconds from now until the authentication information
            expires.

        .. versionadded:: 5.9
        """
        return ExpiringAuth._without_warning(  # type: ignore
            self.auth, time.time() + seconds
        )


def expiring_auth_has_expired(auth: ExpiringAuth) -> bool:
    expires_at = auth.expires_at
    return expires_at is not None and expires_at < time.time()


class AuthManager(metaclass=abc.ABCMeta):
    """Baseclass for authentication information managers.

    The driver provides some default implementations of this class in
    :class:`.AuthManagers` for convenience.

    Custom implementations of this class can be used to provide more complex
    authentication refresh functionality.

    .. warning::

        The manager **must not** interact with the driver in any way as this
        can cause deadlocks and undefined behaviour.

        Furthermore, the manager is expected to be thread-safe.

        The token returned must always belong to the same identity.
        Switching identities using the `AuthManager` is undefined behavior.
        You may use session-level authentication for such use-cases
        :ref:`session-auth-ref`.

    **This is a preview** (see :ref:`filter-warnings-ref`).
    It might be changed without following the deprecation policy.
    See also https://github.com/neo4j/neo4j-python-driver/wiki/preview-features

    .. seealso:: :class:`.AuthManagers`

    .. versionadded:: 5.8

    .. versionchanged:: 5.12
        ``on_auth_expired`` was removed from the interface and replaced by
         :meth:`handle_security_exception`. The new method is called when the
        server returns any `Neo.ClientError.Security.*` error. It's signature
        differs in that it additionally received the error returned by the
        server and returns a boolean indicating whether the error was handled.
    """

    @abc.abstractmethod
    def get_auth(self) -> _TAuth:
        """Return the current authentication information.

        The driver will call this method very frequently. It is recommended
        to implement some form of caching to avoid unnecessary overhead.

        .. warning::

            The method must only ever return auth information belonging to the
            same identity.
            Switching identities using the `AuthManager` is undefined behavior.
            You may use session-level authentication for such use-cases
            :ref:`session-auth-ref`.
        """
        ...

    @abc.abstractmethod
    def handle_security_exception(
        self, auth: _TAuth, error: Neo4jError
    ) -> bool:
        """Handle the server indicating authentication failure.

        The driver will call this method when the server returns any
        `Neo.ClientError.Security.*` error. The error will then be processed
        further as usual.

        :param auth:
            The authentication information that was used when the server
            returned the error.
        :param error:
            The error returned by the server.

        :returns:
            Whether the error was handled (:const:`True`), in which case the
            driver will mark the error as retryable
            (see :meth:`.Neo4jError.is_retryable`).

        .. versionadded:: 5.12
        """
        ...


class AsyncAuthManager(metaclass=abc.ABCMeta):
    """Async version of :class:`.AuthManager`.

    **This is a preview** (see :ref:`filter-warnings-ref`).
    It might be changed without following the deprecation policy.
    See also https://github.com/neo4j/neo4j-python-driver/wiki/preview-features

    .. seealso:: :class:`.AuthManager`

    .. versionadded:: 5.8

    .. versionchanged:: 5.12
        ``on_auth_expired`` was removed from the interface and replaced by
         :meth:`handle_security_exception`. See :class:`.AuthManager`.
    """

    @abc.abstractmethod
    async def get_auth(self) -> _TAuth:
        """Async version of :meth:`.AuthManager.get_auth`.

        .. seealso:: :meth:`.AuthManager.get_auth`
        """
        ...

    @abc.abstractmethod
    async def handle_security_exception(
        self, auth: _TAuth, error: Neo4jError
    ) -> bool:
        """Async version of :meth:`.AuthManager.on_auth_expired`.

        .. seealso:: :meth:`.AuthManager.on_auth_expired`
        """
        ...
