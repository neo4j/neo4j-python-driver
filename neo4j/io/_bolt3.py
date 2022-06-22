#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) "Neo4j"
# Neo4j Sweden AB [http://neo4j.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from enum import Enum
from logging import getLogger
from ssl import SSLSocket

from neo4j._exceptions import (
    BoltError,
    BoltProtocolError,
)
from neo4j.api import (
    READ_ACCESS,
    Version,
)
from neo4j.exceptions import (
    ConfigurationError,
    DatabaseUnavailable,
    DriverError,
    ForbiddenOnReadOnlyDatabase,
    Neo4jError,
    NotALeader,
    ServiceUnavailable,
)
from neo4j.io import (
    Bolt,
    check_supported_server_product,
)
from neo4j.io._common import (
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


class Bolt3(Bolt):
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

    def hello(self):
        headers = self.get_base_headers()
        headers.update(self.auth_dict)
        logged_headers = dict(headers)
        if "credentials" in logged_headers:
            logged_headers["credentials"] = "*******"
        log.debug("[#%04X]  C: HELLO %r", self.local_port, logged_headers)
        self._append(b"\x01", (headers,),
                     response=InitResponse(self, "hello",
                                           on_success=self.server_info.update))
        self.send_all()
        self.fetch_all()
        check_supported_server_product(self.server_info.agent)

    def route(self, database=None, imp_user=None, bookmarks=None):
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
            on_success=metadata.update
        )
        self.pull(on_success=metadata.update, on_records=records.extend)
        self.send_all()
        self.fetch_all()
        routing_info = [dict(zip(metadata.get("fields", ()), values)) for values in records]
        return routing_info

    def run(self, query, parameters=None, mode=None, bookmarks=None,
            metadata=None, timeout=None, db=None, imp_user=None, **handlers):
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
        if timeout:
            try:
                extra["tx_timeout"] = int(1000 * timeout)
            except TypeError:
                raise TypeError("Timeout must be specified as a number of seconds")
        fields = (query, parameters, extra)
        log.debug("[#%04X]  C: RUN %s", self.local_port, " ".join(map(repr, fields)))
        if query.upper() == u"COMMIT":
            self._append(b"\x10", fields, CommitResponse(self, "run",
                                                         **handlers))
        else:
            self._append(b"\x10", fields, Response(self, "run", **handlers))

    def discard(self, n=-1, qid=-1, **handlers):
        # Just ignore n and qid, it is not supported in the Bolt 3 Protocol.
        log.debug("[#%04X]  C: DISCARD_ALL", self.local_port)
        self._append(b"\x2F", (), Response(self, "discard", **handlers))

    def pull(self, n=-1, qid=-1, **handlers):
        # Just ignore n and qid, it is not supported in the Bolt 3 Protocol.
        log.debug("[#%04X]  C: PULL_ALL", self.local_port)
        self._append(b"\x3F", (), Response(self, "pull", **handlers))

    def begin(self, mode=None, bookmarks=None, metadata=None, timeout=None,
              db=None, imp_user=None, **handlers):
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
        if timeout:
            try:
                extra["tx_timeout"] = int(1000 * timeout)
            except TypeError:
                raise TypeError("Timeout must be specified as a number of seconds")
        log.debug("[#%04X]  C: BEGIN %r", self.local_port, extra)
        self._append(b"\x11", (extra,), Response(self, "begin", **handlers))

    def commit(self, **handlers):
        log.debug("[#%04X]  C: COMMIT", self.local_port)
        self._append(b"\x12", (), CommitResponse(self, "commit", **handlers))

    def rollback(self, **handlers):
        log.debug("[#%04X]  C: ROLLBACK", self.local_port)
        self._append(b"\x13", (), Response(self, "rollback", **handlers))

    def reset(self):
        """ Add a RESET message to the outgoing queue, send
        it and consume all remaining messages.
        """

        def fail(metadata):
            raise BoltProtocolError("RESET failed %r" % metadata, address=self.unresolved_address)

        log.debug("[#%04X]  C: RESET", self.local_port)
        self._append(b"\x0F", response=Response(self, "reset", on_failure=fail))
        self.send_all()
        self.fetch_all()

    def goodbye(self):
        log.debug("[#%04X]  C: GOODBYE", self.local_port)
        self._append(b"\x02", ())

    def fetch_message(self):
        """ Receive at most one message from the server, if available.

        :return: 2-tuple of number of detail messages and number of summary
                 messages fetched
        """
        if self._closed:
            raise ServiceUnavailable("Failed to read from closed connection {!r} ({!r})".format(
                self.unresolved_address, self.server_info.address))

        if self._defunct:
            raise ServiceUnavailable("Failed to read from defunct connection {!r} ({!r})".format(
                self.unresolved_address, self.server_info.address))

        if not self.responses:
            return 0, 0

        # Receive exactly one message
        details, summary_signature, summary_metadata = next(self.inbox)

        if details:
            log.debug("[#%04X]  S: RECORD * %d", self.local_port, len(details))  # Do not log any data
            self.responses[0].on_records(details)

        if summary_signature is None:
            return len(details), 0

        response = self.responses.popleft()
        response.complete = True
        if summary_signature == b"\x70":
            log.debug("[#%04X]  S: SUCCESS %r", self.local_port, summary_metadata)
            self._server_state_manager.transition(response.message,
                                                  summary_metadata)
            response.on_success(summary_metadata or {})
        elif summary_signature == b"\x7E":
            log.debug("[#%04X]  S: IGNORED", self.local_port)
            response.on_ignored(summary_metadata or {})
        elif summary_signature == b"\x7F":
            log.debug("[#%04X]  S: FAILURE %r", self.local_port, summary_metadata)
            self._server_state_manager.state = ServerStates.FAILED
            try:
                response.on_failure(summary_metadata or {})
            except (ServiceUnavailable, DatabaseUnavailable):
                if self.pool:
                    self.pool.deactivate(address=self.unresolved_address),
                raise
            except (NotALeader, ForbiddenOnReadOnlyDatabase):
                if self.pool:
                    self.pool.on_write_failure(address=self.unresolved_address),
                raise
            except Neo4jError as e:
                if self.pool and e.invalidates_all_connections():
                    self.pool.mark_all_stale()
                raise
        else:
            raise BoltProtocolError("Unexpected response message with signature %02X" % summary_signature, address=self.unresolved_address)

        return len(details), 1

    def close(self):
        """ Close the connection.
        """
        if not self._closed:
            if not self._defunct:
                log.debug("[#%04X]  C: GOODBYE", self.local_port)
                self._append(b"\x02", ())
                try:
                    self._send_all()
                except (OSError, BoltError, DriverError):
                    pass
            log.debug("[#%04X]  C: <CLOSE>", self.local_port)
            try:
                self.socket.close()
            except OSError:
                pass
            finally:
                self._closed = True

    def closed(self):
        return self._closed

    def defunct(self):
        return self._defunct
