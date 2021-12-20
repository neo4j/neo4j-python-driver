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


import abc
import asyncio
from collections import deque
from logging import getLogger
from time import perf_counter

from ..._async_compat.network import AsyncBoltSocket
from ..._exceptions import BoltHandshakeError
from ...addressing import Address
from ...api import (
    ServerInfo,
    Version,
)
from ...conf import PoolConfig
from ...exceptions import (
    AuthError,
    IncompleteCommit,
    ServiceUnavailable,
    SessionExpired,
)
from ...meta import get_user_agent
from ...packstream import (
    Packer,
    Unpacker,
)
from ._common import (
    AsyncInbox,
    CommitResponse,
    Outbox,
)


# Set up logger
log = getLogger("neo4j")


class AsyncBolt:
    """ Server connection for Bolt protocol.

    A :class:`.Bolt` should be constructed following a
    successful .open()

    Bolt handshake and takes the socket over which
    the handshake was carried out.
    """

    MAGIC_PREAMBLE = b"\x60\x60\xB0\x17"

    PROTOCOL_VERSION = None

    # flag if connection needs RESET to go back to READY state
    is_reset = False

    # The socket
    in_use = False

    # The socket
    _closed = False

    # The socket
    _defunct = False

    #: The pool of which this connection is a member
    pool = None

    # Store the id of the most recent ran query to be able to reduce sent bits by
    # using the default (-1) to refer to the most recent query when pulling
    # results for it.
    most_recent_qid = None

    def __init__(self, unresolved_address, sock, max_connection_lifetime, *,
                 auth=None, user_agent=None, routing_context=None):
        self.unresolved_address = unresolved_address
        self.socket = sock
        self.server_info = ServerInfo(Address(sock.getpeername()),
                                      self.PROTOCOL_VERSION)
        # so far `connection.recv_timeout_seconds` is the only available
        # configuration hint that exists. Therefore, all hints can be stored at
        # connection level. This might change in the future.
        self.configuration_hints = {}
        self.outbox = Outbox()
        self.inbox = AsyncInbox(self.socket, on_error=self._set_defunct_read)
        self.packer = Packer(self.outbox)
        self.unpacker = Unpacker(self.inbox)
        self.responses = deque()
        self._max_connection_lifetime = max_connection_lifetime
        self._creation_timestamp = perf_counter()
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

    def __del__(self):
        if not asyncio.iscoroutinefunction(self.close):
            self.close()

    @property
    @abc.abstractmethod
    def supports_multiple_results(self):
        """ Boolean flag to indicate if the connection version supports multiple
        queries to be buffered on the server side (True) or if all results need
        to be eagerly pulled before sending the next RUN (False).
        """
        pass

    @property
    @abc.abstractmethod
    def supports_multiple_databases(self):
        """ Boolean flag to indicate if the connection version supports multiple
        databases.
        """
        pass

    @classmethod
    def protocol_handlers(cls, protocol_version=None):
        """ Return a dictionary of available Bolt protocol handlers,
        keyed by version tuple. If an explicit protocol version is
        provided, the dictionary will contain either zero or one items,
        depending on whether that version is supported. If no protocol
        version is provided, all available versions will be returned.

        :param protocol_version: tuple identifying a specific protocol
            version (e.g. (3, 5)) or None
        :return: dictionary of version tuple to handler class for all
            relevant and supported protocol versions
        :raise TypeError: if protocol version is not passed in a tuple
        """

        # Carry out Bolt subclass imports locally to avoid circular dependency issues.
        from ._bolt3 import AsyncBolt3
        from ._bolt4 import (
            AsyncBolt4x0,
            AsyncBolt4x1,
            AsyncBolt4x2,
            AsyncBolt4x3,
            AsyncBolt4x4,
        )

        handlers = {
            AsyncBolt3.PROTOCOL_VERSION: AsyncBolt3,
            AsyncBolt4x0.PROTOCOL_VERSION: AsyncBolt4x0,
            AsyncBolt4x1.PROTOCOL_VERSION: AsyncBolt4x1,
            AsyncBolt4x2.PROTOCOL_VERSION: AsyncBolt4x2,
            AsyncBolt4x3.PROTOCOL_VERSION: AsyncBolt4x3,
            AsyncBolt4x4.PROTOCOL_VERSION: AsyncBolt4x4,
        }

        if protocol_version is None:
            return handlers

        if not isinstance(protocol_version, tuple):
            raise TypeError("Protocol version must be specified as a tuple")

        if protocol_version in handlers:
            return {protocol_version: handlers[protocol_version]}

        return {}

    @classmethod
    def version_list(cls, versions, limit=4):
        """ Return a list of supported protocol versions in order of
        preference. The number of protocol versions (or ranges)
        returned is limited to four.
        """
        # In fact, 4.3 is the fist version to support ranges. However, the range
        # support got backported to 4.2. But even if the server is too old to
        # have the backport, negotiating BOLT 4.1 is no problem as it's
        # equivalent to 4.2
        first_with_range_support = Version(4, 2)
        result = []
        for version in versions:
            if (result
                    and version >= first_with_range_support
                    and result[-1][0] == version[0]
                    and result[-1][1][1] == version[1] + 1):
                # can use range to encompass this version
                result[-1][1][1] = version[1]
                continue
            result.append(Version(version[0], [version[1], version[1]]))
            if len(result) == 4:
                break
        return result

    @classmethod
    def get_handshake(cls):
        """ Return the supported Bolt versions as bytes.
        The length is 16 bytes as specified in the Bolt version negotiation.
        :return: bytes
        """
        supported_versions = sorted(cls.protocol_handlers().keys(), reverse=True)
        offered_versions = cls.version_list(supported_versions)
        return b"".join(version.to_bytes() for version in offered_versions).ljust(16, b"\x00")

    @classmethod
    async def ping(cls, address, *, timeout=None, **config):
        """ Attempt to establish a Bolt connection, returning the
        agreed Bolt protocol version if successful.
        """
        config = PoolConfig.consume(config)
        try:
            s, protocol_version, handshake, data = \
                await AsyncBoltSocket.connect(
                    address,
                    timeout=timeout,
                    custom_resolver=config.resolver,
                    ssl_context=config.get_ssl_context(),
                    keep_alive=config.keep_alive,
                )
        except (ServiceUnavailable, SessionExpired, BoltHandshakeError):
            return None
        else:
            AsyncBoltSocket.close_socket(s)
            return protocol_version

    @classmethod
    async def open(
        cls, address, *, auth=None, timeout=None, routing_context=None, **pool_config
    ):
        """ Open a new Bolt connection to a given server address.

        :param address:
        :param auth:
        :param timeout: the connection timeout in seconds
        :param routing_context: dict containing routing context
        :param pool_config:
        :return:
        :raise BoltHandshakeError: raised if the Bolt Protocol can not negotiate a protocol version.
        :raise ServiceUnavailable: raised if there was a connection issue.
        """
        pool_config = PoolConfig.consume(pool_config)
        s, pool_config.protocol_version, handshake, data = \
            await AsyncBoltSocket.connect(
                address,
                timeout=timeout,
                custom_resolver=pool_config.resolver,
                ssl_context=pool_config.get_ssl_context(),
                keep_alive=pool_config.keep_alive,
            )

        # Carry out Bolt subclass imports locally to avoid circular dependency
        # issues.
        if pool_config.protocol_version == (3, 0):
            from ._bolt3 import AsyncBolt3
            bolt_cls = AsyncBolt3
        elif pool_config.protocol_version == (4, 0):
            from ._bolt4 import AsyncBolt4x0
            bolt_cls = AsyncBolt4x0
        elif pool_config.protocol_version == (4, 1):
            from ._bolt4 import AsyncBolt4x1
            bolt_cls = AsyncBolt4x1
        elif pool_config.protocol_version == (4, 2):
            from ._bolt4 import AsyncBolt4x2
            bolt_cls = AsyncBolt4x2
        elif pool_config.protocol_version == (4, 3):
            from ._bolt4 import AsyncBolt4x3
            bolt_cls = AsyncBolt4x3
        elif pool_config.protocol_version == (4, 4):
            from ._bolt4 import AsyncBolt4x4
            bolt_cls = AsyncBolt4x4
        else:
            log.debug("[#%04X]  S: <CLOSE>", s.getsockname()[1])
            AsyncBoltSocket.close_socket(s)

            supported_versions = cls.protocol_handlers().keys()
            raise BoltHandshakeError("The Neo4J server does not support communication with this driver. This driver have support for Bolt Protocols {}".format(supported_versions), address=address, request_data=handshake, response_data=data)

        connection = bolt_cls(
            address, s, pool_config.max_connection_lifetime, auth=auth,
            user_agent=pool_config.user_agent, routing_context=routing_context
        )

        try:
            await connection.hello()
        except Exception:
            await connection.close()
            raise

        return connection

    @property
    @abc.abstractmethod
    def encrypted(self):
        pass

    @property
    @abc.abstractmethod
    def der_encoded_server_certificate(self):
        pass

    @property
    @abc.abstractmethod
    def local_port(self):
        pass

    @abc.abstractmethod
    async def hello(self):
        """ Appends a HELLO message to the outgoing queue, sends it and consumes
         all remaining messages.
        """
        pass

    @abc.abstractmethod
    async def route(self, database=None, imp_user=None, bookmarks=None):
        """ Fetch a routing table from the server for the given
        `database`. For Bolt 4.3 and above, this appends a ROUTE
        message; for earlier versions, a procedure call is made via
        the regular Cypher execution mechanism. In all cases, this is
        sent to the network, and a response is fetched.

        :param database: database for which to fetch a routing table
            Requires Bolt 4.0+.
        :param imp_user: the user to impersonate
            Requires Bolt 4.4+.
        :param bookmarks: iterable of bookmark values after which this
                          transaction should begin
        :return: dictionary of raw routing data
        """
        pass

    @abc.abstractmethod
    def run(self, query, parameters=None, mode=None, bookmarks=None,
            metadata=None, timeout=None, db=None, imp_user=None, **handlers):
        """ Appends a RUN message to the output queue.

        :param query: Cypher query string
        :param parameters: dictionary of Cypher parameters
        :param mode: access mode for routing - "READ" or "WRITE" (default)
        :param bookmarks: iterable of bookmark values after which this transaction should begin
        :param metadata: custom metadata dictionary to attach to the transaction
        :param timeout: timeout for transaction execution (seconds)
        :param db: name of the database against which to begin the transaction
            Requires Bolt 4.0+.
        :param imp_user: the user to impersonate
            Requires Bolt 4.4+.
        :param handlers: handler functions passed into the returned Response object
        :return: Response object
        """
        pass

    @abc.abstractmethod
    def discard(self, n=-1, qid=-1, **handlers):
        """ Appends a DISCARD message to the output queue.

        :param n: number of records to discard, default = -1 (ALL)
        :param qid: query ID to discard for, default = -1 (last query)
        :param handlers: handler functions passed into the returned Response object
        :return: Response object
        """
        pass

    @abc.abstractmethod
    def pull(self, n=-1, qid=-1, **handlers):
        """ Appends a PULL message to the output queue.

        :param n: number of records to pull, default = -1 (ALL)
        :param qid: query ID to pull for, default = -1 (last query)
        :param handlers: handler functions passed into the returned Response object
        :return: Response object
        """
        pass

    @abc.abstractmethod
    def begin(self, mode=None, bookmarks=None, metadata=None, timeout=None,
              db=None, imp_user=None, **handlers):
        """ Appends a BEGIN message to the output queue.

        :param mode: access mode for routing - "READ" or "WRITE" (default)
        :param bookmarks: iterable of bookmark values after which this transaction should begin
        :param metadata: custom metadata dictionary to attach to the transaction
        :param timeout: timeout for transaction execution (seconds)
        :param db: name of the database against which to begin the transaction
            Requires Bolt 4.0+.
        :param imp_user: the user to impersonate
            Requires Bolt 4.4+
        :param handlers: handler functions passed into the returned Response object
        :return: Response object
        """
        pass

    @abc.abstractmethod
    def commit(self, **handlers):
        """ Appends a COMMIT message to the output queue."""
        pass

    @abc.abstractmethod
    def rollback(self, **handlers):
        """ Appends a ROLLBACK message to the output queue."""
        pass

    @abc.abstractmethod
    async def reset(self):
        """ Appends a RESET message to the outgoing queue, sends it and consumes
         all remaining messages.
        """
        pass

    def _append(self, signature, fields=(), response=None):
        """ Appends a message to the outgoing queue.

        :param signature: the signature of the message
        :param fields: the fields of the message as a tuple
        :param response: a response object to handle callbacks
        """
        self.packer.pack_struct(signature, fields)
        self.outbox.wrap_message()
        self.responses.append(response)

    async def _send_all(self):
        data = self.outbox.view()
        if data:
            try:
                await self.socket.sendall(data)
            except OSError as error:
                await self._set_defunct_write(error)
            self.outbox.clear()

    async def send_all(self):
        """ Send all queued messages to the server.
        """
        if self.closed():
            raise ServiceUnavailable("Failed to write to closed connection {!r} ({!r})".format(
                self.unresolved_address, self.server_info.address))

        if self.defunct():
            raise ServiceUnavailable("Failed to write to defunct connection {!r} ({!r})".format(
                self.unresolved_address, self.server_info.address))

        await self._send_all()

    @abc.abstractmethod
    async def fetch_message(self):
        """ Receive at most one message from the server, if available.

        :return: 2-tuple of number of detail messages and number of summary
                 messages fetched
        """
        pass

    async def fetch_all(self):
        """ Fetch all outstanding messages.

        :return: 2-tuple of number of detail messages and number of summary
                 messages fetched
        """
        detail_count = summary_count = 0
        while self.responses:
            response = self.responses[0]
            while not response.complete:
                detail_delta, summary_delta = await self.fetch_message()
                detail_count += detail_delta
                summary_count += summary_delta
        return detail_count, summary_count

    async def _set_defunct_read(self, error=None, silent=False):
        message = "Failed to read from defunct connection {!r} ({!r})".format(
            self.unresolved_address, self.server_info.address
        )
        await self._set_defunct(message, error=error, silent=silent)

    async def _set_defunct_write(self, error=None, silent=False):
        message = "Failed to write data to connection {!r} ({!r})".format(
            self.unresolved_address, self.server_info.address
        )
        await self._set_defunct(message, error=error, silent=silent)

    async def _set_defunct(self, message, error=None, silent=False):
        from ._pool import AsyncBoltPool
        direct_driver = isinstance(self.pool, AsyncBoltPool)

        if error:
            log.debug("[#%04X] %s", self.socket.getsockname()[1], error)
        log.error(message)
        # We were attempting to receive data but the connection
        # has unexpectedly terminated. So, we need to close the
        # connection from the client side, and remove the address
        # from the connection pool.
        self._defunct = True
        await self.close()
        if self.pool:
            await self.pool.deactivate(address=self.unresolved_address)
        # Iterate through the outstanding responses, and if any correspond
        # to COMMIT requests then raise an error to signal that we are
        # unable to confirm that the COMMIT completed successfully.
        if silent:
            return
        for response in self.responses:
            if isinstance(response, CommitResponse):
                if error:
                    raise IncompleteCommit(message) from error
                else:
                    raise IncompleteCommit(message)

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

    def stale(self):
        return (self._stale
                or (0 <= self._max_connection_lifetime
                    <= perf_counter() - self._creation_timestamp))

    _stale = False

    def set_stale(self):
        self._stale = True

    @abc.abstractmethod
    async def close(self):
        """ Close the connection.
        """
        pass

    @abc.abstractmethod
    def closed(self):
        pass

    @abc.abstractmethod
    def defunct(self):
        pass


AsyncBoltSocket.Bolt = AsyncBolt
