#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2020 "Neo4j,"
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

from collections import deque
from ssl import SSLSocket
from time import perf_counter
from neo4j.api import (
    Version,
    READ_ACCESS,
    DEFAULT_DATABASE,
)
from neo4j.io._common import (
    Inbox,
    Outbox,
    Response,
    InitResponse,
    CommitResponse,
)
from neo4j.meta import get_user_agent
from neo4j.exceptions import (
    AuthError,
    ServiceUnavailable,
    DatabaseUnavailable,
    NotALeader,
    ForbiddenOnReadOnlyDatabase,
    SessionExpired,
    ConfigurationError,
)
from neo4j._exceptions import (
    BoltIncompleteCommitError,
    BoltProtocolError,
)
from neo4j.packstream import (
    Unpacker,
    Packer,
)
from neo4j.io import (
    Bolt,
    BoltPool,
)
from neo4j.api import ServerInfo
from neo4j.addressing import Address

from logging import getLogger
log = getLogger("neo4j")


class Bolt3(Bolt):
    """ Protocol handler for Bolt 3.

    This is supported by Neo4j versions 3.5, 4.0, 4.1 and 4.2.
    """

    PROTOCOL_VERSION = Version(3, 0)

    # The socket
    in_use = False

    # The socket
    _closed = False

    # The socket
    _defunct = False

    #: The pool of which this connection is a member
    pool = None

    def __init__(self, unresolved_address, sock, max_connection_lifetime, *, auth=None, user_agent=None, routing_context=None):
        self.unresolved_address = unresolved_address
        self.socket = sock
        self.server_info = ServerInfo(Address(sock.getpeername()), self.PROTOCOL_VERSION)
        self.outbox = Outbox()
        self.inbox = Inbox(self.socket, on_error=self._set_defunct)
        self.packer = Packer(self.outbox)
        self.unpacker = Unpacker(self.inbox)
        self.responses = deque()
        self._max_connection_lifetime = max_connection_lifetime
        self._creation_timestamp = perf_counter()
        self.supports_multiple_results = False
        self.supports_multiple_databases = False
        self._is_reset = True
        self.routing_context = routing_context

        # Determine the user agent
        if user_agent:
            self.user_agent = user_agent
        else:
            self.user_agent = get_user_agent()

        # Determine auth details
        if not auth:
            self.auth_dict = {}
        elif isinstance(auth, tuple) and 2 <= len(auth) <= 3:
            from neo4j import Auth
            self.auth_dict = vars(Auth("basic", *auth))
        else:
            try:
                self.auth_dict = vars(auth)
            except (KeyError, TypeError):
                raise AuthError("Cannot determine auth details from %r" % auth)

        # Check for missing password
        try:
            credentials = self.auth_dict["credentials"]
        except KeyError:
            pass
        else:
            if credentials is None:
                raise AuthError("Password cannot be None")

    @property
    def encrypted(self):
        return isinstance(self.socket, SSLSocket)

    @property
    def der_encoded_server_certificate(self):
        return self.socket.getpeercert(binary_form=True)

    @property
    def local_port(self):
        try:
            return self.socket.getsockname()[1]
        except IOError:
            return 0

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
                     response=InitResponse(self, on_success=self.server_info._update_metadata))
        self.send_all()
        self.fetch_all()

    def run(self, query, parameters=None, mode=None, bookmarks=None, metadata=None, timeout=None, db=None, **handlers):
        if db is not None:
            raise ConfigurationError("Database name parameter for selecting database is not supported in Bolt Protocol {!r}. Database name {!r}.".format(Bolt3.PROTOCOL_VERSION, db))
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
            self._append(b"\x10", fields, CommitResponse(self, **handlers))
        else:
            self._append(b"\x10", fields, Response(self, **handlers))
        self._is_reset = False

    def run_get_routing_table(self, on_success, on_failure, database=DEFAULT_DATABASE):
        if database != DEFAULT_DATABASE:
            raise ConfigurationError("Database name parameter for selecting database is not supported in Bolt Protocol {!r}. Database name {!r}. Server Agent {!r}.".format(Bolt3.PROTOCOL_VERSION, database, self.server_info.agent))
        self.run(
            "CALL dbms.cluster.routing.getRoutingTable($context)",  # This is an internal procedure call. Only available if the Neo4j 3.5 is setup with clustering.
            {"context": self.routing_context},
            mode="r",                                               # Bolt Protocol Version(3, 0) supports mode="r"
            on_success=on_success,
            on_failure=on_failure,
        )

    def discard(self, n=-1, qid=-1, **handlers):
        # Just ignore n and qid, it is not supported in the Bolt 3 Protocol.
        log.debug("[#%04X]  C: DISCARD_ALL", self.local_port)
        self._append(b"\x2F", (), Response(self, **handlers))

    def pull(self, n=-1, qid=-1, **handlers):
        # Just ignore n and qid, it is not supported in the Bolt 3 Protocol.
        log.debug("[#%04X]  C: PULL_ALL", self.local_port)
        self._append(b"\x3F", (), Response(self, **handlers))
        self._is_reset = False

    def begin(self, mode=None, bookmarks=None, metadata=None, timeout=None, db=None, **handlers):
        if db is not None:
            raise ConfigurationError("Database name parameter for selecting database is not supported in Bolt Protocol {!r}. Database name {!r}.".format(Bolt3.PROTOCOL_VERSION, db))
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
        self._append(b"\x11", (extra,), Response(self, **handlers))
        self._is_reset = False

    def commit(self, **handlers):
        log.debug("[#%04X]  C: COMMIT", self.local_port)
        self._append(b"\x12", (), CommitResponse(self, **handlers))

    def rollback(self, **handlers):
        log.debug("[#%04X]  C: ROLLBACK", self.local_port)
        self._append(b"\x13", (), Response(self, **handlers))

    def _append(self, signature, fields=(), response=None):
        """ Add a message to the outgoing queue.

        :arg signature: the signature of the message
        :arg fields: the fields of the message as a tuple
        :arg response: a response object to handle callbacks
        """
        self.packer.pack_struct(signature, fields)
        self.outbox.chunk()
        self.outbox.chunk()
        self.responses.append(response)

    def reset(self):
        """ Add a RESET message to the outgoing queue, send
        it and consume all remaining messages.
        """

        def fail(metadata):
            raise BoltProtocolError("RESET failed %r" % metadata, address=self.unresolved_address)

        log.debug("[#%04X]  C: RESET", self.local_port)
        self._append(b"\x0F", response=Response(self, on_failure=fail))
        self.send_all()
        self.fetch_all()
        self._is_reset = True

    def _send_all(self):
        data = self.outbox.view()
        if data:
            self.socket.sendall(data)
            self.outbox.clear()

    def send_all(self):
        """ Send all queued messages to the server.
        """
        if self.closed():
            raise ServiceUnavailable("Failed to write to closed connection {!r} ({!r})".format(
                self.unresolved_address, self.server_info.address))

        if self.defunct():
            raise ServiceUnavailable("Failed to write to defunct connection {!r} ({!r})".format(
                self.unresolved_address, self.server_info.address))

        try:
            self._send_all()
        except (IOError, OSError) as error:
            log.error("Failed to write data to connection "
                      "{!r} ({!r}); ({!r})".
                      format(self.unresolved_address,
                             self.server_info.address,
                             "; ".join(map(repr, error.args))))
            if self.pool:
                self.pool.deactivate(address=self.unresolved_address)
            raise

    def fetch_message(self):
        """ Receive at least one message from the server, if available.

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
        try:
            details, summary_signature, summary_metadata = next(self.inbox)
        except (IOError, OSError) as error:
            log.error("Failed to read data from connection "
                      "{!r} ({!r}); ({!r})".
                      format(self.unresolved_address,
                             self.server_info.address,
                             "; ".join(map(repr, error.args))))
            if self.pool:
                self.pool.deactivate(address=self.unresolved_address)
            raise

        if details:
            log.debug("[#%04X]  S: RECORD * %d", self.local_port, len(details))  # Do not log any data
            self.responses[0].on_records(details)

        if summary_signature is None:
            return len(details), 0

        response = self.responses.popleft()
        response.complete = True
        if summary_signature == b"\x70":
            log.debug("[#%04X]  S: SUCCESS %r", self.local_port, summary_metadata)
            response.on_success(summary_metadata or {})
        elif summary_signature == b"\x7E":
            log.debug("[#%04X]  S: IGNORED", self.local_port)
            response.on_ignored(summary_metadata or {})
        elif summary_signature == b"\x7F":
            log.debug("[#%04X]  S: FAILURE %r", self.local_port, summary_metadata)
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
        else:
            raise BoltProtocolError("Unexpected response message with signature %02X" % summary_signature, address=self.unresolved_address)

        return len(details), 1

    def _set_defunct(self, error=None):
        direct_driver = isinstance(self.pool, BoltPool)

        message = ("Failed to read from defunct connection {!r} ({!r})".format(
            self.unresolved_address, self.server_info.address))

        if error:
            log.error(str(error))
        log.error(message)
        # We were attempting to receive data but the connection
        # has unexpectedly terminated. So, we need to close the
        # connection from the client side, and remove the address
        # from the connection pool.
        self._defunct = True
        self.close()
        if self.pool:
            self.pool.deactivate(address=self.unresolved_address)
        # Iterate through the outstanding responses, and if any correspond
        # to COMMIT requests then raise an error to signal that we are
        # unable to confirm that the COMMIT completed successfully.
        for response in self.responses:
            if isinstance(response, CommitResponse):
                if error:
                    raise BoltIncompleteCommitError(message, address=None) from error
                else:
                    raise BoltIncompleteCommitError(message, address=None)

        if direct_driver:
            if error:
                raise ServiceUnavailable(message) from error
            else:
                raise ServiceUnavailable(message)
        else:
            if error:
                raise SessionExpired(message) from error
            else:
                raise SessionExpired(message)

    def timedout(self):
        return 0 <= self._max_connection_lifetime <= perf_counter() - self._creation_timestamp

    def fetch_all(self):
        """ Fetch all outstanding messages.

        :return: 2-tuple of number of detail messages and number of summary
                 messages fetched
        """
        detail_count = summary_count = 0
        while self.responses:
            response = self.responses[0]
            while not response.complete:
                detail_delta, summary_delta = self.fetch_message()
                detail_count += detail_delta
                summary_count += summary_delta
        return detail_count, summary_count

    def close(self):
        """ Close the connection.
        """
        if not self._closed:
            if not self._defunct:
                log.debug("[#%04X]  C: GOODBYE", self.local_port)
                self._append(b"\x02", ())
                try:
                    self._send_all()
                except:
                    pass
            log.debug("[#%04X]  C: <CLOSE>", self.local_port)
            try:
                self.socket.close()
            except IOError:
                pass
            finally:
                self._closed = True

    def closed(self):
        return self._closed

    def defunct(self):
        return self._defunct
