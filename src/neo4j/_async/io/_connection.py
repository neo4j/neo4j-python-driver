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

from ... import _typing as t


if t.TYPE_CHECKING:
    from ..._api import TelemetryAPI
    from ..._auth_management import (
        AsyncAuthManager,
        AuthManager,
    )
    from ..._codec.hydration import HydrationScope
    from ...api import _TAuth


class AsyncConnection(abc.ABC):
    @property
    @abc.abstractmethod
    def log_id(self) -> int:
        """
        Some id to prepend in log entries.

        The id should be unique to the connection (at least for a reasonable
        amount of time—might re-use numbers after a while).
        Further, should the id be in {0..0xFFFF} (i.e., 0 and 65535 inclusive).
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def auth_manager(self) -> AsyncAuthManager | AuthManager | None:
        """The current auth manager used for this connection, if any."""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def auth(self) -> _TAuth:
        """The current auth token in use."""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def ssr_enabled(self) -> bool:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def supports_multiple_results(self) -> bool:
        """
        Check if the connection version supports result multiplexing.

        Boolean flag to indicate if the connection version supports multiple
        queries to be buffered on the server side (True) or if all results need
        to be eagerly pulled before sending the next RUN (False).
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def supports_multiple_databases(self) -> bool:
        """
        Check if the connection version supports multiple databases.

        Boolean flag to indicate if the connection version supports multiple
        databases.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def supports_re_auth(self) -> bool:
        """Whether the connection version supports re-authentication."""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def supports_notification_filtering(self) -> bool:
        """Whether the connection version supports notification filtering."""
        raise NotImplementedError

    @abc.abstractmethod
    def new_hydration_scope(self) -> HydrationScope:
        raise NotImplementedError

    @abc.abstractmethod
    def telemetry(
        self,
        api: TelemetryAPI,
        dehydration_hooks=None,
        hydration_hooks=None,
        **handlers,
    ) -> None:
        """
        Send telemetry information about the API usage to the server.

        :param api: the API used.
        :param dehydration_hooks:
            Hooks to dehydrate types (dict from type (class) to dehydration
            function). Dehydration functions receive the value and returns an
            object of type understood by packstream.
        :param hydration_hooks:
            Hooks to hydrate types (mapping from type (class) to
            dehydration function). Dehydration functions receive the value of
            type understood by packstream and are free to return anything.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def run(
        self,
        query,
        parameters=None,
        mode=None,
        bookmarks=None,
        metadata=None,
        timeout=None,
        db=None,
        imp_user=None,
        notifications_min_severity=None,
        notifications_disabled_classifications=None,
        dehydration_hooks=None,
        hydration_hooks=None,
        **handlers,
    ) -> None:
        """
        Append a RUN message to the output queue.

        :param query: Cypher query string
        :param parameters: dictionary of Cypher parameters
        :param mode: access mode for routing - "READ" or "WRITE" (default)
        :param bookmarks: iterable of bookmark values after which this
            transaction should begin
        :param metadata: custom metadata dictionary to attach to the
            transaction
        :param timeout: timeout for transaction execution (seconds)
        :param db: name of the database against which to begin the transaction
            Requires Bolt 4.0+.
        :param imp_user: the user to impersonate
            Requires Bolt 4.4+.
        :param notifications_min_severity:
            minimum severity of notifications to be received.
            Requires Bolt 5.2+.
        :param notifications_disabled_classifications:
            list of notification classifications/categories to be disabled.
            Requires Bolt 5.2+.
        :param dehydration_hooks:
            Hooks to dehydrate types (dict from type (class) to dehydration
            function). Dehydration functions receive the value and returns an
            object of type understood by packstream.
        :param hydration_hooks:
            Hooks to hydrate types (mapping from type (class) to
            dehydration function). Dehydration functions receive the value of
            type understood by packstream and are free to return anything.
        :param handlers: handler functions passed into the returned Response
            object
        """
        raise NotImplementedError

    @abc.abstractmethod
    def discard(
        self,
        n=-1,
        qid=-1,
        dehydration_hooks=None,
        hydration_hooks=None,
        **handlers,
    ) -> None:
        """
        Append a DISCARD message to the output queue.

        :param n: number of records to discard, default = -1 (ALL)
        :param qid: query ID to discard for, default = -1 (last query)
        :param dehydration_hooks:
            Hooks to dehydrate types (dict from type (class) to dehydration
            function). Dehydration functions receive the value and returns an
            object of type understood by packstream.
        :param hydration_hooks:
            Hooks to hydrate types (mapping from type (class) to
            dehydration function). Dehydration functions receive the value of
            type understood by packstream and are free to return anything.
        :param handlers: handler functions passed into the returned Response
            object
        """
        raise NotImplementedError

    @abc.abstractmethod
    def pull(
        self,
        n=-1,
        qid=-1,
        dehydration_hooks=None,
        hydration_hooks=None,
        **handlers,
    ) -> None:
        """
        Append a PULL message to the output queue.

        :param n: number of records to pull, default = -1 (ALL)
        :param qid: query ID to pull for, default = -1 (last query)
        :param dehydration_hooks:
            Hooks to dehydrate types (dict from type (class) to dehydration
            function). Dehydration functions receive the value and returns an
            object of type understood by packstream.
        :param hydration_hooks:
            Hooks to hydrate types (mapping from type (class) to
            dehydration function). Dehydration functions receive the value of
            type understood by packstream and are free to return anything.
        :param handlers: handler functions passed into the returned Response
            object
        """
        raise NotImplementedError

    @abc.abstractmethod
    def begin(
        self,
        mode=None,
        bookmarks=None,
        metadata=None,
        timeout=None,
        db=None,
        imp_user=None,
        notifications_min_severity=None,
        notifications_disabled_classifications=None,
        dehydration_hooks=None,
        hydration_hooks=None,
        **handlers,
    ) -> None:
        """
        Append a BEGIN message to the output queue.

        :param mode: access mode for routing - "READ" or "WRITE" (default)
        :param bookmarks: iterable of bookmark values after which this
            transaction should begin
        :param metadata: custom metadata dictionary to attach to the
            transaction
        :param timeout: timeout for transaction execution (seconds)
        :param db: name of the database against which to begin the transaction
            Requires Bolt 4.0+.
        :param imp_user: the user to impersonate
            Requires Bolt 4.4+
        :param notifications_min_severity:
            minimum severity of notifications to be received.
            Requires Bolt 5.2+.
        :param notifications_disabled_classifications:
            list of notification classifications/categories to be disabled.
            Requires Bolt 5.2+.
        :param dehydration_hooks:
            Hooks to dehydrate types (dict from type (class) to dehydration
            function). Dehydration functions receive the value and returns an
            object of type understood by packstream.
        :param hydration_hooks:
            Hooks to hydrate types (mapping from type (class) to
            dehydration function). Dehydration functions receive the value of
            type understood by packstream and are free to return anything.
        :param handlers: handler functions passed into the returned Response
            object
        :returns: Response object
        """
        raise NotImplementedError

    @abc.abstractmethod
    def commit(
        self, dehydration_hooks=None, hydration_hooks=None, **handlers
    ) -> None:
        """
        Append a COMMIT message to the output queue.

        :param dehydration_hooks:
            Hooks to dehydrate types (dict from type (class) to dehydration
            function). Dehydration functions receive the value and returns an
            object of type understood by packstream.
        :param hydration_hooks:
            Hooks to hydrate types (mapping from type (class) to
            dehydration function). Dehydration functions receive the value of
            type understood by packstream and are free to return anything.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def rollback(
        self, dehydration_hooks=None, hydration_hooks=None, **handlers
    ) -> None:
        """
        Append a ROLLBACK message to the output queue.

        :param dehydration_hooks:
            Hooks to dehydrate types (dict from type (class) to dehydration
            function). Dehydration functions receive the value and returns an
            object of type understood by packstream.
        :param hydration_hooks:
            Hooks to hydrate types (mapping from type (class) to
            dehydration function). Dehydration functions receive the value of
        type understood by packstream and are free to return anything.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def reset(
        self, dehydration_hooks=None, hydration_hooks=None
    ) -> None:
        """
        Submit a RESET (send + consume all).

        Append a RESET message to the outgoing queue, sends it and consumes
        all remaining messages.

        :param dehydration_hooks:
            Hooks to dehydrate types (dict from type (class) to dehydration
            function). Dehydration functions receive the value and returns an
            object of type understood by packstream.
        :param hydration_hooks:
            Hooks to hydrate types (mapping from type (class) to
            dehydration function). Dehydration functions receive the value of
            type understood by packstream and are free to return anything.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def send_all(self) -> None:
        """Send all queued messages to the server."""
        raise NotImplementedError

    @abc.abstractmethod
    async def fetch_message(self):
        """
        Fetch the next outstanding messages.

        :returns: 2-tuple of number of detail messages and number of summary
            messages fetched
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def fetch_all(self) -> None:
        """
        Fetch all outstanding messages.

        :returns: 2-tuple of number of detail messages and number of summary
            messages fetched
        """
        raise NotImplementedError

    @abc.abstractmethod
    def defunct(self) -> bool:
        """TODO: docs."""
        raise NotImplementedError

    @abc.abstractmethod
    def closed(self) -> bool:
        """TODO: docs."""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def is_reset(self) -> bool:
        """TODO: docs."""
        raise NotImplementedError
