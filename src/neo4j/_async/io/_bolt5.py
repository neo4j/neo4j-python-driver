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


from logging import getLogger
from ssl import SSLSocket

from ..._codec.hydration import v2 as hydration_v2
from ..._exceptions import BoltProtocolError
from ...api import (
    READ_ACCESS,
    Version,
)
from ...exceptions import (
    ConfigurationError,
    DatabaseUnavailable,
    ForbiddenOnReadOnlyDatabase,
    Neo4jError,
    NotALeader,
    ServiceUnavailable,
)
from ._bolt import AsyncBolt
from ._bolt3 import (
    ServerStateManager,
    ServerStates,
)
from ._common import (
    check_supported_server_product,
    CommitResponse,
    InitResponse,
    Response,
)


log = getLogger("neo4j")


class AsyncBolt5x0(AsyncBolt):
    """Protocol handler for Bolt 5.0. """

    PROTOCOL_VERSION = Version(5, 0)

    HYDRATION_HANDLER_CLS = hydration_v2.HydrationHandler

    supports_multiple_results = True

    supports_multiple_databases = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._server_state_manager = ServerStateManager(
            ServerStates.CONNECTED, on_change=self._on_server_state_change
        )

    def _on_server_state_change(self, old_state, new_state):
        log.debug("[#%04X]  _: <CONNECTION> state: %s > %s", self.local_port,
                  old_state.name, new_state.name)

    @property
    def is_reset(self):
        # We can't be sure of the server's state if there are still pending
        # responses. Unless the last message we sent was RESET. In that case
        # the server state will always be READY when we're done.
        if (self.responses and self.responses[-1]
                and self.responses[-1].message == "reset"):
            return True
        return self._server_state_manager.state == ServerStates.READY

    @property
    def encrypted(self):
        return isinstance(self.socket, SSLSocket)

    @property
    def der_encoded_server_certificate(self):
        return self.socket.getpeercert(binary_form=True)

    def get_base_headers(self):
        headers = {"user_agent": self.user_agent}
        if self.routing_context is not None:
            headers["routing"] = self.routing_context
        return headers

    async def hello(self, dehydration_hooks=None, hydration_hooks=None):
        if (
            self.notifications_min_severity is not None
            or self.notifications_disabled_categories is not None
        ):
            self.assert_notification_filtering_support()

        def on_success(metadata):
            self.configuration_hints.update(metadata.pop("hints", {}))
            self.server_info.update(metadata)
            if "connection.recv_timeout_seconds" in self.configuration_hints:
                recv_timeout = self.configuration_hints[
                    "connection.recv_timeout_seconds"
                ]
                if isinstance(recv_timeout, int) and recv_timeout > 0:
                    self.socket.settimeout(recv_timeout)
                else:
                    log.info("[#%04X]  _: <CONNECTION> Server supplied an "
                             "invalid value for "
                             "connection.recv_timeout_seconds (%r). Make sure "
                             "the server and network is set up correctly.",
                             self.local_port, recv_timeout)

        headers = self.get_base_headers()
        headers.update(self.auth_dict)
        logged_headers = dict(headers)
        if "credentials" in logged_headers:
            logged_headers["credentials"] = "*******"
        log.debug("[#%04X]  C: HELLO %r", self.local_port, logged_headers)
        self._append(b"\x01", (headers,),
                     response=InitResponse(self, "hello", hydration_hooks,
                                           on_success=on_success),
                     dehydration_hooks=dehydration_hooks)
        await self.send_all()
        await self.fetch_all()
        check_supported_server_product(self.server_info.agent)

    async def route(self, database=None, imp_user=None, bookmarks=None,
                    dehydration_hooks=None, hydration_hooks=None):
        routing_context = self.routing_context or {}
        db_context = {}
        if database is not None:
            db_context.update(db=database)
        if imp_user is not None:
            db_context.update(imp_user=imp_user)
        log.debug("[#%04X]  C: ROUTE %r %r %r", self.local_port,
                  routing_context, bookmarks, db_context)
        metadata = {}
        if bookmarks is None:
            bookmarks = []
        else:
            bookmarks = list(bookmarks)
        self._append(b"\x66", (routing_context, bookmarks, db_context),
                     response=Response(self, "route", hydration_hooks,
                                       on_success=metadata.update),
                     dehydration_hooks=hydration_hooks)
        await self.send_all()
        await self.fetch_all()
        return [metadata.get("rt")]

    def run(self, query, parameters=None, mode=None, bookmarks=None,
            metadata=None, timeout=None, db=None, imp_user=None,
            notifications_min_severity=None,
            notifications_disabled_categories=None, dehydration_hooks=None,
            hydration_hooks=None, **handlers):
        if (
            notifications_min_severity is not None
            or notifications_disabled_categories is not None
        ):
            self.assert_notification_filtering_support()
        if not parameters:
            parameters = {}
        extra = {}
        if mode in (READ_ACCESS, "r"):
            # It will default to mode "w" if nothing is specified
            extra["mode"] = "r"
        if db:
            extra["db"] = db
        if imp_user:
            extra["imp_user"] = imp_user
        if bookmarks:
            try:
                extra["bookmarks"] = list(bookmarks)
            except TypeError:
                raise TypeError("Bookmarks must be provided as iterable")
        if metadata:
            try:
                extra["tx_metadata"] = dict(metadata)
            except TypeError:
                raise TypeError("Metadata must be coercible to a dict")
        if timeout is not None:
            try:
                extra["tx_timeout"] = int(1000 * float(timeout))
            except TypeError:
                raise TypeError("Timeout must be a number (in seconds)")
            if extra["tx_timeout"] < 0:
                raise ValueError("Timeout must be a number <= 0")
        fields = (query, parameters, extra)
        log.debug("[#%04X]  C: RUN %s", self.local_port,
                  " ".join(map(repr, fields)))
        self._append(b"\x10", fields,
                     Response(self, "run", hydration_hooks, **handlers),
                     dehydration_hooks=dehydration_hooks)

    def discard(self, n=-1, qid=-1, dehydration_hooks=None,
                hydration_hooks=None, **handlers):
        extra = {"n": n}
        if qid != -1:
            extra["qid"] = qid
        log.debug("[#%04X]  C: DISCARD %r", self.local_port, extra)
        self._append(b"\x2F", (extra,),
                     Response(self, "discard", hydration_hooks, **handlers),
                     dehydration_hooks=dehydration_hooks)

    def pull(self, n=-1, qid=-1, dehydration_hooks=None, hydration_hooks=None,
             **handlers):
        extra = {"n": n}
        if qid != -1:
            extra["qid"] = qid
        log.debug("[#%04X]  C: PULL %r", self.local_port, extra)
        self._append(b"\x3F", (extra,),
                     Response(self, "pull", hydration_hooks, **handlers),
                     dehydration_hooks=dehydration_hooks)

    def begin(self, mode=None, bookmarks=None, metadata=None, timeout=None,
              db=None, imp_user=None, notifications_min_severity=None,
              notifications_disabled_categories=None, dehydration_hooks=None,
              hydration_hooks=None, **handlers):
        if (
            notifications_min_severity is not None
            or notifications_disabled_categories is not None
        ):
            self.assert_notification_filtering_support()
        extra = {}
        if mode in (READ_ACCESS, "r"):
            # It will default to mode "w" if nothing is specified
            extra["mode"] = "r"
        if db:
            extra["db"] = db
        if imp_user:
            extra["imp_user"] = imp_user
        if bookmarks:
            try:
                extra["bookmarks"] = list(bookmarks)
            except TypeError:
                raise TypeError("Bookmarks must be provided as iterable")
        if metadata:
            try:
                extra["tx_metadata"] = dict(metadata)
            except TypeError:
                raise TypeError("Metadata must be coercible to a dict")
        if timeout is not None:
            try:
                extra["tx_timeout"] = int(1000 * float(timeout))
            except TypeError:
                raise TypeError("Timeout must be a number (in seconds)")
            if extra["tx_timeout"] < 0:
                raise ValueError("Timeout must be a number <= 0")
        log.debug("[#%04X]  C: BEGIN %r", self.local_port, extra)
        self._append(b"\x11", (extra,),
                     Response(self, "begin", hydration_hooks, **handlers),
                     dehydration_hooks=dehydration_hooks)

    def commit(self, dehydration_hooks=None, hydration_hooks=None, **handlers):
        log.debug("[#%04X]  C: COMMIT", self.local_port)
        self._append(b"\x12", (),
                     CommitResponse(self, "commit", hydration_hooks,
                                    **handlers),
                     dehydration_hooks=dehydration_hooks)

    def rollback(self, dehydration_hooks=None, hydration_hooks=None,
                 **handlers):
        log.debug("[#%04X]  C: ROLLBACK", self.local_port)
        self._append(b"\x13", (),
                     Response(self, "rollback", hydration_hooks, **handlers),
                     dehydration_hooks=dehydration_hooks)

    async def reset(self, dehydration_hooks=None, hydration_hooks=None):
        """Reset the connection.

        Add a RESET message to the outgoing queue, send it and consume all
        remaining messages.
        """

        def fail(metadata):
            raise BoltProtocolError("RESET failed %r" % metadata,
                                    self.unresolved_address)

        log.debug("[#%04X]  C: RESET", self.local_port)
        self._append(b"\x0F",
                     response=Response(self, "reset", hydration_hooks,
                                       on_failure=fail),
                     dehydration_hooks=dehydration_hooks)
        await self.send_all()
        await self.fetch_all()

    def goodbye(self, dehydration_hooks=None, hydration_hooks=None):
        log.debug("[#%04X]  C: GOODBYE", self.local_port)
        self._append(b"\x02", (), dehydration_hooks=dehydration_hooks)

    async def _process_message(self, tag, fields):
        """Process at most one message from the server, if available.

        :returns: 2-tuple of number of detail messages and number of summary
                 messages fetched
        """
        details = []
        summary_signature = summary_metadata = None
        if tag == b"\x71":  # RECORD
            details = fields
        elif fields:
            summary_signature = tag
            summary_metadata = fields[0]
        else:
            summary_signature = tag

        if details:
            # Do not log any data
            log.debug("[#%04X]  S: RECORD * %d", self.local_port, len(details))
            await self.responses[0].on_records(details)

        if summary_signature is None:
            return len(details), 0

        response = self.responses.popleft()
        response.complete = True
        if summary_signature == b"\x70":
            log.debug("[#%04X]  S: SUCCESS %r", self.local_port,
                      summary_metadata)
            self._server_state_manager.transition(response.message,
                                                  summary_metadata)
            await response.on_success(summary_metadata or {})
        elif summary_signature == b"\x7E":
            log.debug("[#%04X]  S: IGNORED", self.local_port)
            await response.on_ignored(summary_metadata or {})
        elif summary_signature == b"\x7F":
            log.debug("[#%04X]  S: FAILURE %r", self.local_port,
                      summary_metadata)
            self._server_state_manager.state = ServerStates.FAILED
            try:
                await response.on_failure(summary_metadata or {})
            except (ServiceUnavailable, DatabaseUnavailable):
                if self.pool:
                    await self.pool.deactivate(address=self.unresolved_address)
                raise
            except (NotALeader, ForbiddenOnReadOnlyDatabase):
                if self.pool:
                    self.pool.on_write_failure(address=self.unresolved_address)
                raise
            except Neo4jError as e:
                if self.pool and e._invalidates_all_connections():
                    await self.pool.mark_all_stale()
                raise
        else:
            raise BoltProtocolError(
                "Unexpected response message with signature %02X" % ord(
                    summary_signature
                ), self.unresolved_address
            )

        return len(details), 1


class AsyncBolt5x1(AsyncBolt5x0):

    PROTOCOL_VERSION = Version(5, 1)

    async def hello(self, dehydration_hooks=None, hydration_hooks=None):
        if (
            self.notifications_min_severity is not None
            or self.notifications_disabled_categories is not None
        ):
            self.assert_notification_filtering_support()

        def on_success(metadata):
            self.configuration_hints.update(metadata.pop("hints", {}))
            self.server_info.update(metadata)
            if "connection.recv_timeout_seconds" in self.configuration_hints:
                recv_timeout = self.configuration_hints[
                    "connection.recv_timeout_seconds"
                ]
                if isinstance(recv_timeout, int) and recv_timeout > 0:
                    self.socket.settimeout(recv_timeout)
                else:
                    log.info("[#%04X]  _: <CONNECTION> Server supplied an "
                             "invalid value for "
                             "connection.recv_timeout_seconds (%r). Make sure "
                             "the server and network is set up correctly.",
                             self.local_port, recv_timeout)

        extra = self.get_base_headers()
        log.debug("[#%04X]  C: HELLO %r", self.local_port, extra)
        self._append(b"\x01", (extra,),
                     response=InitResponse(self, "hello", hydration_hooks,
                                           on_success=on_success),
                     dehydration_hooks=dehydration_hooks)

        self.logon(dehydration_hooks, hydration_hooks)
        await self.send_all()
        await self.fetch_all()
        check_supported_server_product(self.server_info.agent)

    def logon(self, dehydration_hooks=None, hydration_hooks=None):
        logged_auth_dict = dict(self.auth_dict)
        if "credentials" in logged_auth_dict:
            logged_auth_dict["credentials"] = "*******"
        log.debug("[#%04X]  C: LOGON %r", self.local_port, logged_auth_dict)
        self._append(b"\x6A", (self.auth_dict,),
                     response=Response(self, "logon", hydration_hooks),
                     dehydration_hooks=dehydration_hooks)


class AsyncBolt5x2(AsyncBolt5x1):

    PROTOCOL_VERSION = Version(5, 2)

    def get_base_headers(self):
        headers = super().get_base_headers()
        if self.notifications_min_severity is not None:
            headers["notifications_minimum_severity"] =\
                self.notifications_min_severity
        if self.notifications_disabled_categories is not None:
            headers["notifications_disabled_categories"] = \
                self.notifications_disabled_categories
        return headers

    async def hello(self, dehydration_hooks=None, hydration_hooks=None):
        def on_success(metadata):
            self.configuration_hints.update(metadata.pop("hints", {}))
            self.server_info.update(metadata)
            if "connection.recv_timeout_seconds" in self.configuration_hints:
                recv_timeout = self.configuration_hints[
                    "connection.recv_timeout_seconds"
                ]
                if isinstance(recv_timeout, int) and recv_timeout > 0:
                    self.socket.settimeout(recv_timeout)
                else:
                    log.info("[#%04X]  _: <CONNECTION> Server supplied an "
                             "invalid value for "
                             "connection.recv_timeout_seconds (%r). Make sure "
                             "the server and network is set up correctly.",
                             self.local_port, recv_timeout)

        extra = self.get_base_headers()
        log.debug("[#%04X]  C: HELLO %r", self.local_port, extra)
        self._append(b"\x01", (extra,),
                     response=InitResponse(self, "hello", hydration_hooks,
                                           on_success=on_success),
                     dehydration_hooks=dehydration_hooks)

        self.logon(dehydration_hooks, hydration_hooks)
        await self.send_all()
        await self.fetch_all()
        check_supported_server_product(self.server_info.agent)

    def logon(self, dehydration_hooks=None, hydration_hooks=None):
        logged_auth_dict = dict(self.auth_dict)
        if "credentials" in logged_auth_dict:
            logged_auth_dict["credentials"] = "*******"
        log.debug("[#%04X]  C: LOGON %r", self.local_port, logged_auth_dict)
        self._append(b"\x6A", (self.auth_dict,),
                     response=Response(self, "logon", hydration_hooks),
                     dehydration_hooks=dehydration_hooks)

    def run(self, query, parameters=None, mode=None, bookmarks=None,
            metadata=None, timeout=None, db=None, imp_user=None,
            notifications_min_severity=None,
            notifications_disabled_categories=None, dehydration_hooks=None,
            hydration_hooks=None, **handlers):
        if not parameters:
            parameters = {}
        extra = {}
        if mode in (READ_ACCESS, "r"):
            # It will default to mode "w" if nothing is specified
            extra["mode"] = "r"
        if db:
            extra["db"] = db
        if imp_user:
            extra["imp_user"] = imp_user
        if notifications_min_severity is not None:
            extra["notifications_minimum_severity"] = \
                notifications_min_severity
        if notifications_disabled_categories is not None:
            extra["notifications_disabled_categories"] = \
                notifications_disabled_categories
        if bookmarks:
            try:
                extra["bookmarks"] = list(bookmarks)
            except TypeError:
                raise TypeError("Bookmarks must be provided as iterable")
        if metadata:
            try:
                extra["tx_metadata"] = dict(metadata)
            except TypeError:
                raise TypeError("Metadata must be coercible to a dict")
        if timeout is not None:
            try:
                extra["tx_timeout"] = int(1000 * float(timeout))
            except TypeError:
                raise TypeError("Timeout must be a number (in seconds)")
            if extra["tx_timeout"] < 0:
                raise ValueError("Timeout must be a number <= 0")
        fields = (query, parameters, extra)
        log.debug("[#%04X]  C: RUN %s", self.local_port,
                  " ".join(map(repr, fields)))
        self._append(b"\x10", fields,
                     Response(self, "run", hydration_hooks, **handlers),
                     dehydration_hooks=dehydration_hooks)

    def begin(self, mode=None, bookmarks=None, metadata=None, timeout=None,
              db=None, imp_user=None, notifications_min_severity=None,
              notifications_disabled_categories=None, dehydration_hooks=None,
              hydration_hooks=None, **handlers):
        extra = {}
        if mode in (READ_ACCESS, "r"):
            # It will default to mode "w" if nothing is specified
            extra["mode"] = "r"
        if db:
            extra["db"] = db
        if imp_user:
            extra["imp_user"] = imp_user
        if bookmarks:
            try:
                extra["bookmarks"] = list(bookmarks)
            except TypeError:
                raise TypeError("Bookmarks must be provided as iterable")
        if metadata:
            try:
                extra["tx_metadata"] = dict(metadata)
            except TypeError:
                raise TypeError("Metadata must be coercible to a dict")
        if timeout is not None:
            try:
                extra["tx_timeout"] = int(1000 * float(timeout))
            except TypeError:
                raise TypeError("Timeout must be a number (in seconds)")
            if extra["tx_timeout"] < 0:
                raise ValueError("Timeout must be a number <= 0")
        if notifications_min_severity is not None:
            extra["notifications_minimum_severity"] = \
                notifications_min_severity
        if notifications_disabled_categories is not None:
            extra["notifications_disabled_categories"] =\
                notifications_disabled_categories
        log.debug("[#%04X]  C: BEGIN %r", self.local_port, extra)
        self._append(b"\x11", (extra,),
                     Response(self, "begin", hydration_hooks, **handlers),
                     dehydration_hooks=dehydration_hooks)
