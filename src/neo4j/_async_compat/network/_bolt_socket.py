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
import errno
import logging
import typing as t
from contextlib import suppress


# fmt: off
# isort: off
# isort seems buggy with the noqa comment
from socket import (
    AF_INET,
    AF_INET6,
    IPPROTO_TCP,
    SHUT_RDWR,
    SO_KEEPALIVE,
    socket,
    SOL_SOCKET,
    TCP_NODELAY,
    timeout as SocketTimeout,  # noqa: N812 (it is a class)
)
# isort: on
# fmt: on
from ssl import (
    CertificateError,
    HAS_SNI,
    SSLError,
    SSLSocket,
)

from ..._deadline import Deadline
from ..._exceptions import (
    BoltProtocolError,
    BoltSecurityError,
    SocketDeadlineExceededError,
)
from ...exceptions import ServiceUnavailable
from ..shims import wait_for


if t.TYPE_CHECKING:
    import typing_extensions as te

    from ..._async.io import AsyncBolt
    from ..._sync.io import Bolt


log = logging.getLogger("neo4j.io")


def _sanitize_deadline(deadline):
    if deadline is None:
        return None
    deadline = Deadline.from_timeout_or_deadline(deadline)
    if deadline.to_timeout() is None:
        return None
    return deadline


class AsyncBoltSocketBase(abc.ABC):
    Bolt: te.Final[type[AsyncBolt]] = None  # type: ignore[assignment]

    def __init__(self, reader, protocol, writer) -> None:
        self._reader = reader  # type: asyncio.StreamReader
        self._protocol = protocol  # type: asyncio.StreamReaderProtocol
        self._writer = writer  # type: asyncio.StreamWriter
        # 0 - non-blocking
        # None infinitely blocking
        # int - seconds to wait for data
        self._timeout = None
        self._deadline = None

    async def _wait_for_io(self, io_async_fn, *args, **kwargs):
        timeout = self._timeout
        to_raise = SocketTimeout
        if self._deadline is not None:
            deadline_timeout = self._deadline.to_timeout()
            if deadline_timeout <= 0:
                raise SocketDeadlineExceededError("timed out")
            if timeout is None or deadline_timeout <= timeout:
                timeout = deadline_timeout
                to_raise = SocketDeadlineExceededError

        io_fut = io_async_fn(*args, **kwargs)
        if timeout is not None and timeout <= 0:
            # give the io-operation time for one loop cycle to do its thing
            io_fut = asyncio.create_task(io_fut)
            try:
                await asyncio.sleep(0)
            except asyncio.CancelledError:
                # This is emulating non-blocking. There is no cancelling this!
                # Still, we don't want to silently swallow the cancellation.
                # Hence, we flag this task as cancelled again, so that the next
                # `await` will raise the CancelledError.
                asyncio.current_task().cancel()
        try:
            return await wait_for(io_fut, timeout)
        except asyncio.TimeoutError as e:
            raise to_raise("timed out") from e

    def get_deadline(self):
        return self._deadline

    def set_deadline(self, deadline):
        self._deadline = _sanitize_deadline(deadline)

    @property
    def _socket(self) -> socket:
        return self._writer.transport.get_extra_info("socket")

    def getsockname(self):
        return self._writer.transport.get_extra_info("sockname")

    def getpeername(self):
        return self._writer.transport.get_extra_info("peername")

    def getpeercert(self, *args, **kwargs):
        return self._writer.transport.get_extra_info("ssl_object").getpeercert(
            *args, **kwargs
        )

    def gettimeout(self):
        return self._timeout

    def settimeout(self, timeout):
        if timeout is None:
            self._timeout = timeout
        else:
            assert timeout >= 0
            self._timeout = timeout

    async def recv(self, n):
        return await self._wait_for_io(self._reader.read, n)

    async def recv_into(self, buffer, nbytes):
        # FIXME: not particularly memory or time efficient
        res = await self._wait_for_io(self._reader.read, nbytes)
        buffer[: len(res)] = res
        return len(res)

    async def sendall(self, data):
        self._writer.write(data)
        return await self._wait_for_io(self._writer.drain)

    async def close(self):
        self._writer.close()
        await self._writer.wait_closed()

    def kill(self):
        self._writer.close()

    @classmethod
    async def _connect_secure(
        cls, resolved_address, timeout, keep_alive, ssl_context
    ) -> te.Self:
        """
        Connect to the address and return the socket.

        :param resolved_address:
        :param timeout: seconds
        :param keep_alive: True or False
        :param ssl_context: SSLContext or None

        :returns: AsyncBoltSocket object
        """
        loop = asyncio.get_event_loop()
        s = None
        local_port = 0

        # TODO: tomorrow me: fix this mess
        try:
            if len(resolved_address) == 2:
                s = socket(AF_INET)
            elif len(resolved_address) == 4:
                s = socket(AF_INET6)
            else:
                raise ValueError(f"Unsupported address {resolved_address!r}")
            s.setblocking(False)  # asyncio + blocking = no-no!
            log.debug("[#0000]  C: <OPEN> %s", resolved_address)
            await wait_for(loop.sock_connect(s, resolved_address), timeout)
            local_port = s.getsockname()[1]

            keep_alive = 1 if keep_alive else 0
            s.setsockopt(SOL_SOCKET, SO_KEEPALIVE, keep_alive)

            ssl_kwargs: dict[str, t.Any] = {}

            if ssl_context is not None:
                hostname = resolved_address._host_name or None
                sni_host = hostname if HAS_SNI and hostname else None
                ssl_kwargs.update(ssl=ssl_context, server_hostname=sni_host)
                log.debug("[#%04X]  C: <SECURE> %s", local_port, hostname)

            reader = asyncio.StreamReader(
                limit=2**16,  # 64 KiB,
                loop=loop,
            )
            protocol = asyncio.StreamReaderProtocol(reader, loop=loop)
            transport, _ = await loop.create_connection(
                lambda: protocol, sock=s, **ssl_kwargs
            )
            writer = asyncio.StreamWriter(transport, protocol, reader, loop)

            if ssl_context is not None:
                # Check that the server provides a certificate
                der_encoded_server_certificate = transport.get_extra_info(
                    "ssl_object"
                ).getpeercert(binary_form=True)
                if der_encoded_server_certificate is None:
                    local_port = s.getsockname()[1]
                    raise BoltProtocolError(
                        "When using an encrypted socket, the server should "
                        "always provide a certificate",
                        address=(resolved_address._host_name, local_port),
                    )

            return cls(reader, protocol, writer)

        except asyncio.TimeoutError:
            log.debug("[#0000]  S: <TIMEOUT> %s", resolved_address)
            log.debug("[#0000]  C: <CLOSE> %s", resolved_address)
            if s:
                cls._kill_raw_socket(s)
            raise ServiceUnavailable(
                "Timed out trying to establish connection to "
                f"{resolved_address!r}"
            ) from None
        except asyncio.CancelledError:
            log.debug("[#0000]  S: <CANCELLED> %s", resolved_address)
            log.debug("[#0000]  C: <CLOSE> %s", resolved_address)
            if s:
                cls._kill_raw_socket(s)
            raise
        except (SSLError, CertificateError) as error:
            if s:
                cls._kill_raw_socket(s)
            raise BoltSecurityError(
                message="Failed to establish encrypted connection.",
                address=(resolved_address._host_name, local_port),
            ) from error
        except Exception as error:
            log.debug(
                "[#0000]  S: <ERROR> %s %s",
                type(error).__name__,
                " ".join(map(repr, error.args)),
            )
            log.debug("[#0000]  C: <CLOSE> %s", resolved_address)
            if s:
                cls._kill_raw_socket(s)
            if isinstance(error, OSError):
                raise ServiceUnavailable(
                    f"Failed to establish connection to {resolved_address!r} "
                    f"(reason {error})"
                ) from error
            raise

    @abc.abstractmethod
    async def _handshake(self, resolved_address, deadline): ...

    @classmethod
    @abc.abstractmethod
    async def connect(
        cls,
        address,
        *,
        tcp_timeout,
        deadline,
        custom_resolver,
        ssl_context,
        keep_alive,
    ): ...

    @classmethod
    async def close_socket(cls, socket_):
        if isinstance(socket_, AsyncBoltSocketBase):
            with suppress(OSError):
                await socket_.close()
        else:
            cls._kill_raw_socket(socket_)

    @classmethod
    def _kill_raw_socket(cls, socket_):
        with suppress(OSError):
            socket_.shutdown(SHUT_RDWR)
        with suppress(OSError):
            socket_.close()


class BoltSocketBase:
    Bolt: te.Final[type[Bolt]] = None  # type: ignore[assignment]

    def __init__(self, socket_: socket):
        self._socket = socket_
        self._deadline = None

    @property
    def _socket(self):
        return self.__socket

    @_socket.setter
    def _socket(self, socket_: socket | SSLSocket):
        self.__socket = socket_
        self.getsockname = socket_.getsockname
        self.getpeername = socket_.getpeername
        if hasattr(socket, "getpeercert"):
            self.getpeercert = t.cast(SSLSocket, socket_).getpeercert
        elif "getpeercert" in self.__dict__:
            del self.__dict__["getpeercert"]
        self.gettimeout = socket_.gettimeout
        self.settimeout = socket_.settimeout

    getsockname: t.Callable = None  # type: ignore
    getpeername: t.Callable = None  # type: ignore
    getpeercert: t.Callable = None  # type: ignore
    gettimeout: t.Callable = None  # type: ignore
    settimeout: t.Callable = None  # type: ignore

    def _wait_for_io(self, func, *args, **kwargs):
        if self._deadline is None:
            return func(*args, **kwargs)
        timeout = self._socket.gettimeout()
        deadline_timeout = self._deadline.to_timeout()
        if deadline_timeout <= 0:
            raise SocketDeadlineExceededError("timed out")
        if timeout is None or deadline_timeout <= timeout:
            self._socket.settimeout(deadline_timeout)
            try:
                return func(*args, **kwargs)
            except SocketTimeout as e:
                raise SocketDeadlineExceededError("timed out") from e
            finally:
                self._socket.settimeout(timeout)
        return func(*args, **kwargs)

    def get_deadline(self):
        return self._deadline

    def set_deadline(self, deadline):
        self._deadline = _sanitize_deadline(deadline)

    def recv(self, n):
        return self._wait_for_io(self._socket.recv, n)

    def recv_into(self, buffer, nbytes):
        return self._wait_for_io(self._socket.recv_into, buffer, nbytes)

    def sendall(self, data):
        return self._wait_for_io(self._socket.sendall, data)

    def close(self):
        self.close_socket(self._socket)

    def kill(self):
        self._socket.close()

    @classmethod
    def _connect_secure(
        cls, resolved_address, timeout, keep_alive, ssl_context
    ):
        """
        Connect to the address and return the socket.

        :param resolved_address:
        :param timeout: seconds
        :param keep_alive: True or False
        :returns: socket object
        """
        s = None  # The socket

        try:
            try:
                if len(resolved_address) == 2:
                    s = socket(AF_INET)
                elif len(resolved_address) == 4:
                    s = socket(AF_INET6)
                else:
                    raise ValueError(
                        f"Unsupported address {resolved_address!r}"
                    )
                try:
                    s.setsockopt(IPPROTO_TCP, TCP_NODELAY, 1)
                except OSError as e:
                    # option might not be supported on all platforms
                    if e.errno != errno.ENOPROTOOPT:
                        raise
                t = s.gettimeout()
                if timeout:
                    s.settimeout(timeout)
                log.debug("[#0000]  C: <OPEN> %s", resolved_address)
                s.connect(resolved_address)
                s.settimeout(t)
                keep_alive = 1 if keep_alive else 0
                s.setsockopt(SOL_SOCKET, SO_KEEPALIVE, keep_alive)
            except SocketTimeout:
                log.debug("[#0000]  S: <TIMEOUT> %s", resolved_address)
                raise ServiceUnavailable(
                    "Timed out trying to establish connection to "
                    f"{resolved_address!r}"
                ) from None
            except Exception as error:
                log.debug(
                    "[#0000]  S: <ERROR> %s %s",
                    type(error).__name__,
                    " ".join(map(repr, error.args)),
                )
                if isinstance(error, OSError):
                    raise ServiceUnavailable(
                        "Failed to establish connection to "
                        f"{resolved_address!r} (reason {error})"
                    ) from error
                raise

            local_port = s.getsockname()[1]
            # Secure the connection if an SSL context has been provided
            if ssl_context:
                hostname = resolved_address._host_name or None
                sni_host = hostname if HAS_SNI and hostname else None
                log.debug("[#%04X]  C: <SECURE> %s", local_port, hostname)
                try:
                    s = ssl_context.wrap_socket(s, server_hostname=sni_host)
                except (OSError, SSLError, CertificateError) as cause:
                    raise BoltSecurityError(
                        message="Failed to establish encrypted connection.",
                        address=(hostname, local_port),
                    ) from cause
                # Check that the server provides a certificate
                der_encoded_server_certificate = s.getpeercert(
                    binary_form=True
                )
                if der_encoded_server_certificate is None:
                    raise BoltProtocolError(
                        "When using an encrypted socket, the server should"
                        "always provide a certificate",
                        address=(hostname, local_port),
                    )
        except Exception:
            if s is not None:
                log.debug("[#0000]  C: <CLOSE> %s", resolved_address)
                cls._kill_raw_socket(s)
            raise

        return cls(s)

    @abc.abstractmethod
    def _handshake(self, resolved_address, deadline): ...

    @classmethod
    @abc.abstractmethod
    def connect(
        cls,
        address,
        *,
        tcp_timeout,
        deadline,
        custom_resolver,
        ssl_context,
        keep_alive,
    ): ...

    @classmethod
    def close_socket(cls, socket_):
        if isinstance(socket_, BoltSocketBase):
            socket_ = socket_._socket
        cls._kill_raw_socket(socket_)

    @classmethod
    def _kill_raw_socket(cls, socket_):
        with suppress(OSError):
            socket_.shutdown(SHUT_RDWR)
        with suppress(OSError):
            socket_.close()
