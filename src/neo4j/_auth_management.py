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


import abc
import typing as t
from dataclasses import dataclass

from ._meta import preview
from .api import _TAuth


@preview("Auth managers are a preview feature.")
@dataclass
class ExpiringAuth:
    """Represents potentially expiring authentication information.

    This class is used with :meth:`.AuthManagers.temporal` and
    :meth:`.AsyncAuthManagers.temporal`.

    :param auth: The authentication information.
    :param expires_in: The number of seconds until the authentication
        information expires. If :data:`None`, the authentication information
        is considered to not expire until the server explicitly indicates so.

    **This is a preview** (see :ref:`filter-warnings-ref`).
    It might be changed without following the deprecation policy.
    See also https://github.com/neo4j/neo4j-python-driver/wiki/preview-features

    .. seealso::
        :meth:`.AuthManagers.temporal`, :meth:`.AsyncAuthManagers.temporal`

    .. versionadded:: 5.8
    """
    auth: _TAuth
    expires_in: t.Optional[float] = None


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

    **This is a preview** (see :ref:`filter-warnings-ref`).
    It might be changed without following the deprecation policy.
    See also https://github.com/neo4j/neo4j-python-driver/wiki/preview-features

    .. seealso:: :class:`.AuthManagers`

    .. versionadded:: 5.8
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
        """
        ...

    @abc.abstractmethod
    def on_auth_expired(self, auth: _TAuth) -> None:
        """Handle the server indicating expired authentication information.

        The driver will call this method when the server indicates that the
        provided authentication information is no longer valid.

        :param auth:
            The authentication information that the server flagged as no longer
            valid.
        """
        ...


class AsyncAuthManager(metaclass=abc.ABCMeta):
    """Async version of :class:`.AuthManager`.

    **This is a preview** (see :ref:`filter-warnings-ref`).
    It might be changed without following the deprecation policy.
    See also https://github.com/neo4j/neo4j-python-driver/wiki/preview-features

    .. seealso:: :class:`.AuthManager`

    .. versionadded:: 5.8
    """

    @abc.abstractmethod
    async def get_auth(self) -> _TAuth:
        """Async version of :meth:`.AuthManager.get_auth`.

        .. seealso:: :meth:`.AuthManager.get_auth`
        """
        ...

    @abc.abstractmethod
    async def on_auth_expired(self, auth: _TAuth) -> None:
        """Async version of :meth:`.AuthManager.on_auth_expired`.

        .. seealso:: :meth:`.AuthManager.on_auth_expired`
        """
        ...
