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
import asyncio
import typing as t
from collections import deque
from logging import getLogger
from time import monotonic

from ..._async_compat.network import BoltSocket
from ..._async_compat.util import Util
from ..._codec.hydration import (
    HydrationHandlerABC,
    v1 as hydration_v1,
)
from ..._codec.packstream import v1 as packstream_v1
from ..._deadline import Deadline
from ..._exceptions import (
    BoltError,
    BoltHandshakeError,
)
from ..._meta import USER_AGENT
from ..._sync.config import PoolConfig
from ...addressing import ResolvedAddress
from ...api import (
    Auth,
    ServerInfo,
    Version,
)
from ...exceptions import (
    AuthError,
    ConfigurationError,
    DriverError,
    IncompleteCommit,
    ServiceUnavailable,
    SessionExpired,
)
from ..config import PoolConfig
from ._common import (
    CommitResponse,
    Inbox,
    Outbox,
)


if t.TYPE_CHECKING:
    from ..._api import TelemetryAPI


# Set up logger
log = getLogger("neo4j.io")


class ServerStateManagerBase(abc.ABC):
    @abc.abstractmethod
    def __init__(self, init_state, on_change=None): ...

    @abc.abstractmethod
    def transition(self, message, metadata): ...

    @abc.abstractmethod
    def failed(self): ...


class ClientStateManagerBase(abc.ABC):
    @abc.abstractmethod
    def __init__(self, init_state, on_change=None): ...

    @abc.abstractmethod
    def transition(self, message): ...


class Bolt:
    """
    Server connection for Bolt protocol.

    A :class:`.Bolt` should be constructed following a
    successful .open()

    Bolt handshake and takes the socket over which
    the handshake was carried out.
    """

    # TODO: let packer/unpacker know of hydration (give them hooks?)
    # TODO: make sure query parameter dehydration gets clear error message.

    PACKER_CLS = packstream_v1.Packer
    UNPACKER_CLS = packstream_v1.Unpacker
    HYDRATION_HANDLER_CLS: type[HydrationHandlerABC] = (
        hydration_v1.HydrationHandler
    )

    MAGIC_PREAMBLE = b"\x60\x60\xb0\x17"

    PROTOCOL_VERSION: Version = None  # type: ignore[assignment]

    # flag if connection needs RESET to go back to READY state
    is_reset = False

    # The socket
    in_use = False

    # When the connection was last put back into the pool
    idle_since = float("-inf")
    # The database name the connection was last used with
    # (BEGIN for explicit transactions, RUN for auto-commit transactions)
    last_database: str | None = None

    # The socket
    _closing = False
    _closed = False
    _defunct = False

    #: The pool of which this connection is a member
    pool = None

    # Store the id of the most recent ran query to be able to reduce sent bits
    # by using the default (-1) to refer to the most recent query when pulling
    # results for it.
    most_recent_qid = None

    def __init__(
        self,
        unresolved_address,
        sock,
        max_connection_lifetime,
        *,
        auth=None,
        auth_manager=None,
        user_agent=None,
        routing_context=None,
        notifications_min_severity=None,
        notifications_disabled_classifications=None,
        telemetry_disabled=False,
    ):
        self.unresolved_address = unresolved_address
        self.socket = sock
        self.local_port = self.socket.getsockname()[1]
        self.server_info = ServerInfo(
            ResolvedAddress(
                sock.getpeername(), host_name=unresolved_address.host
            ),
            self.PROTOCOL_VERSION,
        )
        # so far `connection.recv_timeout_seconds` is the only available
        # configuration hint that exists. Therefore, all hints can be stored at
        # connection level. This might change in the future.
        self.configuration_hints = {}
        self.patch = {}
        self.outbox = Outbox(
            self.socket,
            on_error=self._set_defunct_write,
            packer_cls=self.PACKER_CLS,
        )
        self.inbox = Inbox(
            self.socket,
            on_error=self._set_defunct_read,
            unpacker_cls=self.UNPACKER_CLS,
        )
        self.hydration_handler = self.HYDRATION_HANDLER_CLS()
        self.responses = deque()
        self._max_connection_lifetime = max_connection_lifetime
        self._creation_timestamp = monotonic()
        self.routing_context = routing_context
        self.idle_since = monotonic()

        # Determine the user agent
        if user_agent:
            self.user_agent = user_agent
        else:
            self.user_agent = USER_AGENT

        self.auth = auth
        self.auth_dict = self._to_auth_dict(auth)
        self.auth_manager = auth_manager
        self.telemetry_disabled = telemetry_disabled

        self.notifications_min_severity = notifications_min_severity
        self.notifications_disabled_classifications = (
            notifications_disabled_classifications
        )

    def __del__(self):
        if not asyncio.iscoroutinefunction(self.close):
            self.close()

    @abc.abstractmethod
    def _get_server_state_manager(self) -> ServerStateManagerBase: ...

    @abc.abstractmethod
    def _get_client_state_manager(self) -> ClientStateManagerBase: ...

    @classmethod
    def _to_auth_dict(cls, auth):
        # Determine auth details
        if not auth:
            return {}
        elif isinstance(auth, tuple) and 2 <= len(auth) <= 3:
            return vars(Auth("basic", *auth))
        else:
            try:
                return vars(auth)
            except (KeyError, TypeError) as e:
                raise AuthError(
                    f"Cannot determine auth details from {auth!r}"
                ) from e

    @property
    def connection_id(self):
        return self.server_info._metadata.get("connection_id", "<unknown id>")

    @property
    @abc.abstractmethod
    def supports_multiple_results(self):
        """
        Check if the connection version supports result multiplexing.

        Boolean flag to indicate if the connection version supports multiple
        queries to be buffered on the server side (True) or if all results need
        to be eagerly pulled before sending the next RUN (False).
        """

    @property
    @abc.abstractmethod
    def supports_multiple_databases(self):
        """
        Check if the connection version supports multiple databases.

        Boolean flag to indicate if the connection version supports multiple
        databases.
        """

    @property
    @abc.abstractmethod
    def supports_re_auth(self):
        """Whether the connection version supports re-authentication."""

    def assert_re_auth_support(self):
        if not self.supports_re_auth:
            raise ConfigurationError(
                "User switching is not supported for Bolt "
                f"Protocol {self.PROTOCOL_VERSION!r}. Server Agent "
                f"{self.server_info.agent!r}"
            )

    @property
    @abc.abstractmethod
    def supports_notification_filtering(self):
        """Whether the connection version supports re-authentication."""

    def assert_notification_filtering_support(self):
        if not self.supports_notification_filtering:
            raise ConfigurationError(
                "Notification filtering is not supported for the Bolt "
                f"Protocol {self.PROTOCOL_VERSION!r}. Server Agent "
                f"{self.server_info.agent!r}"
            )

    # [bolt-version-bump] search tag when changing bolt version support
    @classmethod
    def protocol_handlers(cls, protocol_version=None):
        """
        Return a dictionary of available Bolt protocol handlers.

        The handlers are keyed by version tuple. If an explicit protocol
        version is provided, the dictionary will contain either zero or one
        items, depending on whether that version is supported. If no protocol
        version is provided, all available versions will be returned.

        :param protocol_version: tuple identifying a specific protocol
            version (e.g. (3, 5)) or None
        :returns: dictionary of version tuple to handler class for all
            relevant and supported protocol versions
        :raise TypeError: if protocol version is not passed in a tuple
        """
        # Carry out Bolt subclass imports locally to avoid circular dependency
        # issues.
        from ._bolt3 import Bolt3
        from ._bolt4 import (
            Bolt4x1,
            Bolt4x2,
            Bolt4x3,
            Bolt4x4,
        )
        from ._bolt5 import (
            Bolt5x0,
            Bolt5x1,
            Bolt5x2,
            Bolt5x3,
            Bolt5x4,
            Bolt5x5,
            Bolt5x6,
        )

        handlers = {
            Bolt3.PROTOCOL_VERSION: Bolt3,
            # 4.0 unsupported because no space left in the handshake
            Bolt4x1.PROTOCOL_VERSION: Bolt4x1,
            Bolt4x2.PROTOCOL_VERSION: Bolt4x2,
            Bolt4x3.PROTOCOL_VERSION: Bolt4x3,
            Bolt4x4.PROTOCOL_VERSION: Bolt4x4,
            Bolt5x0.PROTOCOL_VERSION: Bolt5x0,
            Bolt5x1.PROTOCOL_VERSION: Bolt5x1,
            Bolt5x2.PROTOCOL_VERSION: Bolt5x2,
            Bolt5x3.PROTOCOL_VERSION: Bolt5x3,
            Bolt5x4.PROTOCOL_VERSION: Bolt5x4,
            Bolt5x5.PROTOCOL_VERSION: Bolt5x5,
            Bolt5x6.PROTOCOL_VERSION: Bolt5x6,
        }

        if protocol_version is None:
            return handlers

        if not isinstance(protocol_version, tuple):
            raise TypeError("Protocol version must be specified as a tuple")

        if protocol_version in handlers:
            return {protocol_version: handlers[protocol_version]}

        return {}

    @classmethod
    def version_list(cls, versions):
        """
        Return a list of supported protocol versions in order of preference.

        The number of protocol versions (or ranges) returned is limited to 4.
        """
        # In fact, 4.3 is the fist version to support ranges. However, the
        # range support got backported to 4.2. But even if the server is too
        # old to have the backport, negotiating BOLT 4.1 is no problem as it's
        # equivalent to 4.2
        first_with_range_support = Version(4, 2)
        result = []
        for version in versions:
            if (
                result
                and version >= first_with_range_support
                and result[-1][0] == version[0]
                and result[-1][1][1] == version[1] + 1
            ):
                # can use range to encompass this version
                result[-1][1][1] = version[1]
                continue
            result.append(Version(version[0], [version[1], version[1]]))
            if len(result) == 4:
                break
        return result

    @classmethod
    def get_handshake(cls):
        """
        Return the supported Bolt versions as bytes.

        The length is 16 bytes as specified in the Bolt version negotiation.
        :returns: bytes
        """
        supported_versions = sorted(
            cls.protocol_handlers().keys(), reverse=True
        )
        offered_versions = cls.version_list(supported_versions)
        versions_bytes = (v.to_bytes() for v in offered_versions)
        return b"".join(versions_bytes).ljust(16, b"\x00")

    @classmethod
    def ping(cls, address, *, deadline=None, pool_config=None):
        """
        Attempt to establish a Bolt connection.

        A one-off connection is created and the agreed Bolt protocol version
        is returned on success.
        """
        if pool_config is None:
            pool_config = PoolConfig()
        if deadline is None:
            deadline = Deadline(None)

        try:
            (
                s,
                protocol_version,
                _handshake,
                _data,
            ) = BoltSocket.connect(
                address,
                tcp_timeout=pool_config.connection_timeout,
                deadline=deadline,
                custom_resolver=pool_config.resolver,
                ssl_context=pool_config.get_ssl_context(),
                keep_alive=pool_config.keep_alive,
            )
        except (ServiceUnavailable, SessionExpired, BoltHandshakeError):
            return None
        else:
            BoltSocket.close_socket(s)
            return protocol_version

    # [bolt-version-bump] search tag when changing bolt version support
    @classmethod
    def open(
        cls,
        address,
        *,
        auth_manager=None,
        deadline=None,
        routing_context=None,
        pool_config=None,
    ):
        """
        Open a new Bolt connection to a given server address.

        :param address:
        :param auth_manager:
        :param deadline: how long to wait for the connection to be established
        :param routing_context: dict containing routing context
        :param pool_config:

        :returns: connected Bolt instance

        :raise BoltHandshakeError:
            raised if the Bolt Protocol can not negotiate a protocol version.
        :raise ServiceUnavailable: raised if there was a connection issue.
        """
        if pool_config is None:
            pool_config = PoolConfig()
        if deadline is None:
            deadline = Deadline(None)

        s, protocol_version, handshake, data = BoltSocket.connect(
            address,
            tcp_timeout=pool_config.connection_timeout,
            deadline=deadline,
            custom_resolver=pool_config.resolver,
            ssl_context=pool_config.get_ssl_context(),
            keep_alive=pool_config.keep_alive,
        )

        pool_config.protocol_version = protocol_version

        # Carry out Bolt subclass imports locally to avoid circular dependency
        # issues.

        # avoid new lines after imports for better readability and conciseness
        # fmt: off
        if protocol_version == (5, 6):
            from ._bolt5 import Bolt5x6
            bolt_cls = Bolt5x6
        elif protocol_version == (5, 5):
            from ._bolt5 import Bolt5x5
            bolt_cls = Bolt5x5
        elif protocol_version == (5, 4):
            from ._bolt5 import Bolt5x4
            bolt_cls = Bolt5x4
        elif protocol_version == (5, 3):
            from ._bolt5 import Bolt5x3
            bolt_cls = Bolt5x3
        elif protocol_version == (5, 2):
            from ._bolt5 import Bolt5x2
            bolt_cls = Bolt5x2
        elif protocol_version == (5, 1):
            from ._bolt5 import Bolt5x1
            bolt_cls = Bolt5x1
        elif protocol_version == (5, 0):
            from ._bolt5 import Bolt5x0
            bolt_cls = Bolt5x0
        elif protocol_version == (4, 4):
            from ._bolt4 import Bolt4x4
            bolt_cls = Bolt4x4
        elif protocol_version == (4, 3):
            from ._bolt4 import Bolt4x3
            bolt_cls = Bolt4x3
        elif protocol_version == (4, 2):
            from ._bolt4 import Bolt4x2
            bolt_cls = Bolt4x2
        elif protocol_version == (4, 1):
            from ._bolt4 import Bolt4x1
            bolt_cls = Bolt4x1
        # Implementation for 4.0 exists, but there was no space left in the
        # handshake to offer this version to the server. Hence, the server
        # should never request us to speak bolt 4.0.
        # elif protocol_version == (4, 0):
        #     from ._bolt4 import AsyncBolt4x0
        #     bolt_cls = AsyncBolt4x0
        elif protocol_version == (3, 0):
            from ._bolt3 import Bolt3
            bolt_cls = Bolt3
        # fmt: on
        else:
            log.debug("[#%04X]  C: <CLOSE>", s.getsockname()[1])
            BoltSocket.close_socket(s)

            supported_versions = cls.protocol_handlers().keys()
            raise BoltHandshakeError(
                "The neo4j server does not support communication with this "
                "driver. This driver has support for Bolt protocols "
                f"{tuple(map(str, supported_versions))}.",
                address=address,
                request_data=handshake,
                response_data=data,
            )

        try:
            auth = Util.callback(auth_manager.get_auth)
        except asyncio.CancelledError as e:
            log.debug(
                "[#%04X]  C: <KILL> open auth manager failed: %r",
                s.getsockname()[1],
                e,
            )
            s.kill()
            raise
        except Exception as e:
            log.debug(
                "[#%04X]  C: <CLOSE> open auth manager failed: %r",
                s.getsockname()[1],
                e,
            )
            s.close()
            raise

        connection = bolt_cls(
            address,
            s,
            pool_config.max_connection_lifetime,
            auth=auth,
            auth_manager=auth_manager,
            user_agent=pool_config.user_agent,
            routing_context=routing_context,
            notifications_min_severity=pool_config.notifications_min_severity,
            notifications_disabled_classifications=pool_config.notifications_disabled_classifications,
            telemetry_disabled=pool_config.telemetry_disabled,
        )

        try:
            connection.socket.set_deadline(deadline)
            try:
                connection.hello()
            finally:
                connection.socket.set_deadline(None)
        except (
            Exception,
            # Python 3.8+: CancelledError is a subclass of BaseException
            asyncio.CancelledError,
        ) as e:
            log.debug("[#%04X]  C: <OPEN FAILED> %r", connection.local_port, e)
            connection.kill()
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

    @abc.abstractmethod
    def hello(self, dehydration_hooks=None, hydration_hooks=None):
        """
        Submit a HELLO (send + consume all).

        Append a HELLO message to the outgoing queue, sends it and consumes
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

    @abc.abstractmethod
    def logon(self, dehydration_hooks=None, hydration_hooks=None):
        """Append a LOGON message to the outgoing queue."""

    @abc.abstractmethod
    def logoff(self, dehydration_hooks=None, hydration_hooks=None):
        """Append a LOGOFF message to the outgoing queue."""

    def mark_unauthenticated(self):
        """Mark the connection as unauthenticated."""
        self.auth_dict = {}

    def re_auth(
        self,
        auth,
        auth_manager,
        force=False,
        dehydration_hooks=None,
        hydration_hooks=None,
    ):
        """
        Append LOGON, LOGOFF to the outgoing queue.

        If auth is the same as the current auth, this method does nothing.

        :returns: whether the auth was changed
        """
        new_auth_dict = self._to_auth_dict(auth)
        if not force and new_auth_dict == self.auth_dict:
            self.auth_manager = auth_manager
            self.auth = auth
            return False
        self.logoff(
            dehydration_hooks=dehydration_hooks,
            hydration_hooks=hydration_hooks,
        )
        self.auth_dict = new_auth_dict
        self.auth_manager = auth_manager
        self.auth = auth
        self.logon(
            dehydration_hooks=dehydration_hooks,
            hydration_hooks=hydration_hooks,
        )
        return True

    @abc.abstractmethod
    def route(
        self,
        database=None,
        imp_user=None,
        bookmarks=None,
        dehydration_hooks=None,
        hydration_hooks=None,
    ):
        """
        Fetch a routing table from the server for the given ``database``.

        For Bolt 4.3 and above, this appends a ROUTE
        message; for earlier versions, a procedure call is made via
        the regular Cypher execution mechanism. In all cases, this is
        sent to the network, and a response is fetched.

        :param database: database for which to fetch a routing table
            Requires Bolt 4.0+.
        :param imp_user: the user to impersonate
            Requires Bolt 4.4+.
        :param bookmarks: iterable of bookmark values after which this
                          transaction should begin
        :param dehydration_hooks:
            Hooks to dehydrate types (dict from type (class) to dehydration
            function). Dehydration functions receive the value and returns an
            object of type understood by packstream.
        :param hydration_hooks:
            Hooks to hydrate types (mapping from type (class) to
            dehydration function). Dehydration functions receive the value of
            type understood by packstream and are free to return anything.
        """

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
    ):
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

    @abc.abstractmethod
    def discard(
        self,
        n=-1,
        qid=-1,
        dehydration_hooks=None,
        hydration_hooks=None,
        **handlers,
    ):
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

    @abc.abstractmethod
    def pull(
        self,
        n=-1,
        qid=-1,
        dehydration_hooks=None,
        hydration_hooks=None,
        **handlers,
    ):
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
    ):
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

    @abc.abstractmethod
    def commit(self, dehydration_hooks=None, hydration_hooks=None, **handlers):
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

    @abc.abstractmethod
    def rollback(
        self, dehydration_hooks=None, hydration_hooks=None, **handlers
    ):
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

    @abc.abstractmethod
    def reset(self, dehydration_hooks=None, hydration_hooks=None):
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

    @abc.abstractmethod
    def goodbye(self, dehydration_hooks=None, hydration_hooks=None):
        """
        Append a GOODBYE message to the outgoing queue.

        :param dehydration_hooks:
            Hooks to dehydrate types (dict from type (class) to dehydration
            function). Dehydration functions receive the value and returns an
            object of type understood by packstream.
        :param hydration_hooks:
            Hooks to hydrate types (mapping from type (class) to
            dehydration function). Dehydration functions receive the value of
            type understood by packstream and are free to return anything.
        """

    def new_hydration_scope(self):
        return self.hydration_handler.new_hydration_scope()

    def _append(
        self, signature, fields=(), response=None, dehydration_hooks=None
    ):
        """
        Append a message to the outgoing queue.

        :param signature: the signature of the message
        :param fields: the fields of the message as a tuple
        :param response: a response object to handle callbacks
        :param dehydration_hooks:
            Hooks to dehydrate types (dict from type (class) to dehydration
            function). Dehydration functions receive the value and returns an
            object of type understood by packstream.
        """
        self.outbox.append_message(signature, fields, dehydration_hooks)
        self.responses.append(response)
        if response:
            self._get_client_state_manager().transition(response.message)

    def _send_all(self):
        if self.outbox.flush():
            self.idle_since = monotonic()

    def send_all(self):
        """Send all queued messages to the server."""
        if self.closed():
            raise ServiceUnavailable(
                "Failed to write to closed connection "
                f"{self.unresolved_address!r} ({self.server_info.address!r})"
            )
        if self.defunct():
            raise ServiceUnavailable(
                "Failed to write to defunct connection "
                f"{self.unresolved_address!r} ({self.server_info.address!r})"
            )

        self._send_all()

    @abc.abstractmethod
    def _process_message(self, tag, fields):
        """
        Receive at most one message from the server, if available.

        :returns: 2-tuple of number of detail messages and number of summary
                 messages fetched
        """

    def fetch_message(self):
        if self._closed:
            raise ServiceUnavailable(
                "Failed to read from closed connection "
                f"{self.unresolved_address!r} ({self.server_info.address!r})"
            )
        if self._defunct:
            raise ServiceUnavailable(
                "Failed to read from defunct connection "
                f"{self.unresolved_address!r} ({self.server_info.address!r})"
            )
        if not self.responses:
            return 0, 0

        # Receive exactly one message
        tag, fields = self.inbox.pop(
            hydration_hooks=self.responses[0].hydration_hooks
        )
        res = self._process_message(tag, fields)
        self.idle_since = monotonic()
        return res

    def fetch_all(self):
        """
        Fetch all outstanding messages.

        :returns: 2-tuple of number of detail messages and number of summary
                 messages fetched
        """
        detail_count = summary_count = 0
        while not self._closed and self.responses:
            response = self.responses[0]
            while not response.complete:
                detail_delta, summary_delta = self.fetch_message()
                detail_count += detail_delta
                summary_count += summary_delta
        return detail_count, summary_count

    def _set_defunct_read(self, error=None, silent=False):
        message = (
            "Failed to read from defunct connection "
            f"{self.unresolved_address!r} ({self.server_info.address!r})"
        )
        self._set_defunct(message, error=error, silent=silent)

    def _set_defunct_write(self, error=None, silent=False):
        message = (
            "Failed to write data to connection "
            f"{self.unresolved_address!r} ({self.server_info.address!r})"
        )
        self._set_defunct(message, error=error, silent=silent)

    def _set_defunct(self, message, error=None, silent=False):
        direct_driver = getattr(self.pool, "is_direct_pool", False)
        user_cancelled = isinstance(error, asyncio.CancelledError)

        if error:
            log.debug(
                "[#%04X]  _: <CONNECTION> error: %r", self.local_port, error
            )
        if not user_cancelled:
            log.error(message)
        # We were attempting to receive data but the connection
        # has unexpectedly terminated. So, we need to close the
        # connection from the client side, and remove the address
        # from the connection pool.
        self._defunct = True
        if user_cancelled:
            self.kill()
            raise error  # cancellation error should not be re-written
        if not self._closing:
            # If we fail while closing the connection, there is no need to
            # remove the connection from the pool, nor to try to close the
            # connection again.
            self.close()
            if self.pool and not self._get_server_state_manager().failed():
                self.pool.deactivate(address=self.unresolved_address)

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
        elif error:
            raise SessionExpired(message) from error
        else:
            raise SessionExpired(message)

    def stale(self):
        return self._stale or (
            0
            <= self._max_connection_lifetime
            <= monotonic() - self._creation_timestamp
        )

    _stale = False

    def set_stale(self):
        self._stale = True

    def close(self):
        """Close the connection."""
        if self._closed or self._closing:
            return
        self._closing = True
        if not self._defunct:
            self.goodbye()
            try:
                self._send_all()
            except (OSError, BoltError, DriverError) as exc:
                log.debug(
                    "[#%04X]  _: <CONNECTION> ignoring failed close %r",
                    self.local_port,
                    exc,
                )
        log.debug("[#%04X]  C: <CLOSE>", self.local_port)
        try:
            self.socket.close()
        except OSError:
            pass
        finally:
            self._closed = True

    def kill(self):
        """Close the socket most violently. No flush, no goodbye, no mercy."""
        if self._closed:
            return
        log.debug("[#%04X]  C: <KILL>", self.local_port)
        self._closing = True
        try:
            self.socket.kill()
        except OSError as exc:
            log.debug(
                "[#%04X]  _: <CONNECTION> ignoring failed kill %r",
                self.local_port,
                exc,
            )
        finally:
            self._closed = True

    def closed(self):
        return self._closed

    def defunct(self):
        return self._defunct

    def is_idle_for(self, timeout):
        """
        Check if connection has been idle for at least the given timeout.

        :param timeout: timeout in seconds
        :type timeout: float

        :rtype: bool
        """
        return monotonic() - self.idle_since > timeout


BoltSocket.Bolt = Bolt  # type: ignore


def tx_timeout_as_ms(timeout: float) -> int:
    """
    Round transaction timeout to milliseconds.

    Values in (0, 1], else values are rounded using the built-in round()
    function (round n.5 values to nearest even).

    :param timeout: timeout in seconds (must be >= 0)

    :returns: timeout in milliseconds (rounded)

    :raise ValueError: if timeout is negative
    """
    try:
        timeout = float(timeout)
    except (TypeError, ValueError) as e:
        err_type = type(e)
        msg = "Timeout must be specified as a number of seconds"
        raise err_type(msg) from None
    if timeout < 0:
        raise ValueError("Timeout must be a positive number or 0.")
    ms = int(round(1000 * timeout))
    if ms == 0 and timeout > 0:
        # Special case for 0 < timeout < 0.5 ms.
        # This would be rounded to 0 ms, but the server interprets this as
        # infinite timeout. So we round to the smallest possible timeout: 1 ms.
        ms = 1
    return ms
