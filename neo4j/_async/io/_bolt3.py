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


from enum import Enum
from logging import getLogger
from ssl import SSLSocket

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
from ._common import (
    check_supported_server_product,
    CommitResponse,
    InitResponse,
    Response,
)


log = getLogger("neo4j")


class ServerStates(Enum):
    CONNECTED = "CONNECTED"
    READY = "READY"
    STREAMING = "STREAMING"
    TX_READY_OR_TX_STREAMING = "TX_READY||TX_STREAMING"
    FAILED = "FAILED"


class ServerStateManager:
    _STATE_TRANSITIONS = {
        ServerStates.CONNECTED: {
            "hello": ServerStates.READY,
        },
        ServerStates.READY: {
            "run": ServerStates.STREAMING,
            "begin": ServerStates.TX_READY_OR_TX_STREAMING,
        },
        ServerStates.STREAMING: {
            "pull": ServerStates.READY,
            "discard": ServerStates.READY,
            "reset": ServerStates.READY,
        },
        ServerStates.TX_READY_OR_TX_STREAMING: {
            "commit": ServerStates.READY,
            "rollback": ServerStates.READY,
            "reset": ServerStates.READY,
        },
        ServerStates.FAILED: {
            "reset": ServerStates.READY,
        }
    }

    def __init__(self, init_state, on_change=None):
        self.state = init_state
        self._on_change = on_change

    def transition(self, message, metadata):
        if metadata.get("has_more"):
            return
        state_before = self.state
        self.state = self._STATE_TRANSITIONS\
            .get(self.state, {})\
            .get(message, self.state)
        if state_before != self.state and callable(self._on_change):
            self._on_change(state_before, self.state)


class AsyncBolt3(AsyncBolt):
    """ Protocol handler for Bolt 3.

    This is supported by Neo4j versions 3.5, 4.0, 4.1, 4.2, 4.3, and 4.4.
    """

    PROTOCOL_VERSION = Version(3, 0)

    supports_multiple_results = False

    supports_multiple_databases = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._server_state_manager = ServerStateManager(
            ServerStates.CONNECTED, on_change=self._on_server_state_change
        )

    def _on_server_state_change(self, old_state, new_state):
        log.debug("[#%04X]  State: %s > %s", self.local_port,
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
        return {
            "user_agent": self.user_agent,
        }

    async def hello(self, dehydration_hooks=None, hydration_hooks=None):
        headers = self.get_base_headers()
        headers.update(self.auth_dict)
        logged_headers = dict(headers)
        if "credentials" in logged_headers:
            logged_headers["credentials"] = "*******"
        log.debug("[#%04X]  C: HELLO %r", self.local_port, logged_headers)
        self._append(b"\x01", (headers,),
                     response=InitResponse(self, "hello", hydration_hooks,
                                           on_success=self.server_info.update),
                     dehydration_hooks=dehydration_hooks)
        await self.send_all()
        await self.fetch_all()
        check_supported_server_product(self.server_info.agent)

    async def route(
        self, database=None, imp_user=None, bookmarks=None,
        dehydration_hooks=None, hydration_hooks=None
    ):
        if database is not None:
            raise ConfigurationError(
                "Database name parameter for selecting database is not "
                "supported in Bolt Protocol {!r}. Database name {!r}. "
                "Server Agent {!r}".format(
                    self.PROTOCOL_VERSION, database, self.server_info.agent
                )
            )
        if imp_user is not None:
            raise ConfigurationError(
                "Impersonation is not supported in Bolt Protocol {!r}. "
                "Trying to impersonate {!r}.".format(
                    self.PROTOCOL_VERSION, imp_user
                )
            )

        metadata = {}
        records = []

        # Ignoring database and bookmarks because there is no multi-db support.
        # The bookmarks are only relevant for making sure a previously created
        # db exists before querying a routing table for it.
        self.run(
            "CALL dbms.cluster.routing.getRoutingTable($context)",  # This is an internal procedure call. Only available if the Neo4j 3.5 is setup with clustering.
            {"context": self.routing_context},
            mode="r",                                               # Bolt Protocol Version(3, 0) supports mode="r"
            dehydration_hooks=dehydration_hooks,
            hydration_hooks=hydration_hooks,
            on_success=metadata.update
        )
        self.pull(dehydration_hooks = None, hydration_hooks = None,
                  on_success=metadata.update, on_records=records.extend)
        await self.send_all()
        await self.fetch_all()
        routing_info = [dict(zip(metadata.get("fields", ()), values)) for values in records]
        return routing_info

    def run(self, query, parameters=None, mode=None, bookmarks=None,
            metadata=None, timeout=None, db=None, imp_user=None,
            dehydration_hooks=None, hydration_hooks=None, **handlers):
        if db is not None:
            raise ConfigurationError(
                "Database name parameter for selecting database is not "
                "supported in Bolt Protocol {!r}. Database name {!r}.".format(
                    self.PROTOCOL_VERSION, db
                )
            )
        if imp_user is not None:
            raise ConfigurationError(
                "Impersonation is not supported in Bolt Protocol {!r}. "
                "Trying to impersonate {!r}.".format(
                    self.PROTOCOL_VERSION, imp_user
                )
            )
        if not parameters:
            parameters = {}
        extra = {}
        if mode in (READ_ACCESS, "r"):
            extra["mode"] = "r"  # It will default to mode "w" if nothing is specified
        if bookmarks:
            try:
                extra["bookmarks"] = list(bookmarks)
            except TypeError:
                raise TypeError("Bookmarks must be provided within an iterable")
        if metadata:
            try:
                extra["tx_metadata"] = dict(metadata)
            except TypeError:
                raise TypeError("Metadata must be coercible to a dict")
        if timeout is not None:
            try:
                extra["tx_timeout"] = int(1000 * float(timeout))
            except TypeError:
                raise TypeError("Timeout must be specified as a number of seconds")
            if extra["tx_timeout"] < 0:
                raise ValueError("Timeout must be a positive number or 0.")
        fields = (query, parameters, extra)
        log.debug("[#%04X]  C: RUN %s", self.local_port, " ".join(map(repr, fields)))
        self._append(b"\x10", fields,
                     Response(self, "run", hydration_hooks, **handlers),
                     dehydration_hooks=dehydration_hooks)

    def discard(self, n=-1, qid=-1, dehydration_hooks=None,
                hydration_hooks=None, **handlers):
        # Just ignore n and qid, it is not supported in the Bolt 3 Protocol.
        log.debug("[#%04X]  C: DISCARD_ALL", self.local_port)
        self._append(b"\x2F", (),
                     Response(self, "discard", hydration_hooks, **handlers),
                     dehydration_hooks=dehydration_hooks)

    def pull(self, n=-1, qid=-1, dehydration_hooks=None, hydration_hooks=None,
             **handlers):
        # Just ignore n and qid, it is not supported in the Bolt 3 Protocol.
        log.debug("[#%04X]  C: PULL_ALL", self.local_port)
        self._append(b"\x3F", (),
                     Response(self, "pull", hydration_hooks, **handlers),
                     dehydration_hooks=dehydration_hooks)

    def begin(self, mode=None, bookmarks=None, metadata=None, timeout=None,
              db=None, imp_user=None, dehydration_hooks=None,
              hydration_hooks=None, **handlers):
        if db is not None:
            raise ConfigurationError(
                "Database name parameter for selecting database is not "
                "supported in Bolt Protocol {!r}. Database name {!r}.".format(
                    self.PROTOCOL_VERSION, db
                )
            )
        if imp_user is not None:
            raise ConfigurationError(
                "Impersonation is not supported in Bolt Protocol {!r}. "
                "Trying to impersonate {!r}.".format(
                    self.PROTOCOL_VERSION, imp_user
                )
            )
        extra = {}
        if mode in (READ_ACCESS, "r"):
            extra["mode"] = "r"  # It will default to mode "w" if nothing is specified
        if bookmarks:
            try:
                extra["bookmarks"] = list(bookmarks)
            except TypeError:
                raise TypeError("Bookmarks must be provided within an iterable")
        if metadata:
            try:
                extra["tx_metadata"] = dict(metadata)
            except TypeError:
                raise TypeError("Metadata must be coercible to a dict")
        if timeout is not None:
            try:
                extra["tx_timeout"] = int(1000 * float(timeout))
            except TypeError:
                raise TypeError("Timeout must be specified as a number of seconds")
            if extra["tx_timeout"] < 0:
                raise ValueError("Timeout must be a positive number or 0.")
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
        """ Add a RESET message to the outgoing queue, send
        it and consume all remaining messages.
        """

        def fail(metadata):
            raise BoltProtocolError("RESET failed %r" % metadata, address=self.unresolved_address)

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
        """ Process at most one message from the server, if available.

        :return: 2-tuple of number of detail messages and number of summary
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
            log.debug("[#%04X]  S: RECORD * %d", self.local_port, len(details))  # Do not log any data
            await self.responses[0].on_records(details)

        if summary_signature is None:
            return len(details), 0

        response = self.responses.popleft()
        response.complete = True
        if summary_signature == b"\x70":
            log.debug("[#%04X]  S: SUCCESS %r", self.local_port, summary_metadata)
            self._server_state_manager.transition(response.message,
                                                  summary_metadata)
            await response.on_success(summary_metadata or {})
        elif summary_signature == b"\x7E":
            log.debug("[#%04X]  S: IGNORED", self.local_port)
            await response.on_ignored(summary_metadata or {})
        elif summary_signature == b"\x7F":
            log.debug("[#%04X]  S: FAILURE %r", self.local_port, summary_metadata)
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
                if self.pool and e.invalidates_all_connections():
                    await self.pool.mark_all_stale()
                raise
        else:
            raise BoltProtocolError("Unexpected response message with signature %02X" % summary_signature, address=self.unresolved_address)

        return len(details), 1
