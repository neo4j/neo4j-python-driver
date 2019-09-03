#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2019 "Neo4j,"
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


from asyncio import (
    IncompleteReadError,
    Semaphore,
    StreamReader,
    StreamReaderProtocol,
    StreamWriter,
    get_event_loop,
    wait,
)
from collections import deque
from logging import getLogger
from os import strerror
from ssl import SSLError
from sys import platform, version_info
from time import perf_counter

from neo4j.addressing import Address
from neo4j.aio.bolt._mixins import Addressable, Breakable
from neo4j.errors import (
    BoltError,
    BoltConnectionError,
    BoltSecurityError,
    BoltConnectionBroken,
    BoltHandshakeError,
)
from neo4j.api import Security, Version
from neo4j.meta import version as neo4j_version


log = getLogger("neo4j")


MAGIC = b"\x60\x60\xB0\x17"


class Bolt(Addressable, object):

    #: Security configuration for this connection.
    security = None

    #: As a class attribute, this denotes the version of Bolt handled
    #: by that subclass. As an instance attribute, this represents the
    #: version of the protocol in use.
    protocol_version = ()

    # Record of the time at which this connection was opened.
    __t_opened = None

    # Handle to the StreamReader object.
    __reader = None

    # Handle to the StreamWriter object, which can be used on close.
    __writer = None

    # Flag to indicate that the connection is closed
    __closed = False

    @classmethod
    def default_user_agent(cls):
        """ Return the default user agent string for a connection.
        """
        template = "neo4j-python/{} Python/{}.{}.{}-{}-{} ({})"
        fields = (neo4j_version,) + tuple(version_info) + (platform,)
        return template.format(*fields)

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

        # Carry out subclass imports locally to avoid circular
        # dependency issues.
        from neo4j.aio.bolt.v3 import Bolt3

        handlers = {bolt.protocol_version: bolt for bolt in [
            # This list can be updated as protocol
            # versions are added and removed.
            Bolt3,
        ]}

        if protocol_version is None:
            return handlers
        if not isinstance(protocol_version, tuple):
            raise TypeError("Protocol version must be specified as a tuple")
        return {version: handler
                for version, handler in handlers.items()
                if version == protocol_version}

    @classmethod
    def opener(cls, auth=None, security=False, protocol_version=None, loop=None):
        """ Create and return an opener function for a given set of
        configuration parameters. This is useful when multiple servers share
        the same configuration details, such as within a connection pool.
        """

        async def f(address):
            return await Bolt.open(address, auth=auth, security=security,
                                   protocol_version=protocol_version, loop=loop)

        return f

    @classmethod
    async def open(cls, address, *, auth=None, security=False, protocol_version=None, loop=None):
        """ Open a socket connection and perform protocol version
        negotiation, in order to construct and return a Bolt client
        instance for a supported Bolt protocol version.

        :param address: tuples of host and port, such as
                        ("127.0.0.1", 7687)
        :param auth:
        :param security:
        :param protocol_version:
        :param loop:
        :return: instance of a Bolt subclass
        :raise BoltConnectionError: if a connection could not be
            established
        :raise BoltConnectionLost: if an I/O error occurs on the
            underlying socket connection
        :raise BoltHandshakeError: if handshake completes without a
            successful negotiation
        :raise TypeError: if any of the arguments provided are passed
            as incompatible types
        :raise ValueError: if any of the arguments provided are passed
            with unsupported values
        """

        # Connect
        address = Address(address)
        reader, writer, security = await cls._connect(address, security, loop)

        try:

            # Handshake
            subclass = await cls._handshake(reader, writer, protocol_version)

            # Instantiation
            inst = subclass(reader, writer)
            inst.security = security
            assert hasattr(inst, "__ainit__")
            await inst.__ainit__(auth)
            return inst

        except BoltError:
            writer.write_eof()
            writer.close()
            raise

    @classmethod
    async def _connect(cls, address, security, loop):
        """ Attempt to establish a TCP connection to the address
        provided.

        :param address:
        :param security:
        :param loop:
        :return: a 3-tuple of reader, writer and security settings for
            the new connection
        :raise BoltConnectionError: if a connection could not be
            established
        """
        assert isinstance(address, Address)
        if loop is None:
            loop = get_event_loop()
        connection_args = {
            "host": address.host,
            "port": address.port,
            "family": address.family,
            # TODO: other args
        }
        if security is True:
            security = Security.default()
        if isinstance(security, Security):
            ssl_context = security.to_ssl_context()
            connection_args["ssl"] = ssl_context
            connection_args["server_hostname"] = address.host
        elif security:
            raise TypeError("Unsupported security configuration {!r}".format(security))
        else:
            security = None
        log.debug("[#0000] C: <DIAL> %s", address)
        try:
            reader = BoltStreamReader(loop=loop)
            protocol = StreamReaderProtocol(reader, loop=loop)
            transport, _ = await loop.create_connection(lambda: protocol, **connection_args)
            writer = BoltStreamWriter(transport, protocol, reader, loop)
        except SSLError as err:
            log.debug("[#%04X] S: <REJECT> %s (%d %s)", 0, address,
                      err.errno, strerror(err.errno))
            raise BoltSecurityError("Failed to establish a secure connection", address) from err
        except OSError as err:
            log.debug("[#%04X] S: <REJECT> %s (%d %s)", 0, address,
                      err.errno, strerror(err.errno))
            raise BoltConnectionError("Failed to establish a connection", address) from err
        else:
            local_address = Address(transport.get_extra_info("sockname"))
            remote_address = Address(transport.get_extra_info("peername"))
            log.debug("[#%04X] S: <ACCEPT> %s -> %s",
                      local_address.port_number, local_address, remote_address)
            return reader, writer, security

    @classmethod
    async def _handshake(cls, reader, writer, protocol_version):
        """ Carry out a Bolt handshake, optionally requesting a
        specific protocol version.

        :param reader:
        :param writer:
        :param protocol_version:
        :return:
        :raise BoltConnectionLost: if an I/O error occurs on the
            underlying socket connection
        :raise BoltHandshakeError: if handshake completes without a
            successful negotiation
        """
        local_address = Address(writer.transport.get_extra_info("sockname"))
        remote_address = Address(writer.transport.get_extra_info("peername"))

        handlers = cls.protocol_handlers(protocol_version)
        if not handlers:
            raise ValueError("No protocol handlers available (requested Bolt %r)", protocol_version)
        offered_versions = sorted(handlers.keys(), reverse=True)[:4]

        request_data = MAGIC + b"".join(
            v.to_bytes() for v in offered_versions).ljust(16, b"\x00")
        log.debug("[#%04X] C: <HANDSHAKE> %r", local_address.port_number, request_data)
        writer.write(request_data)
        await writer.drain()
        response_data = await reader.readexactly(4)
        log.debug("[#%04X] S: <HANDSHAKE> %r", local_address.port_number, response_data)
        try:
            agreed_version = Version.from_bytes(response_data)
        except ValueError as err:
            writer.close()
            raise BoltHandshakeError("Unexpected handshake response %r" % response_data,
                                     remote_address, request_data, response_data) from err
        try:
            subclass = handlers[agreed_version]
        except KeyError:
            log.debug("Unsupported Bolt protocol version %s", agreed_version)
            raise BoltHandshakeError("Unsupported Bolt protocol version",
                                     remote_address, request_data, response_data)
        else:
            return subclass

    def __new__(cls, reader, writer):
        obj = super().__new__(cls)
        obj.__t_opened = perf_counter()
        obj.__reader = reader
        obj.__writer = writer
        Addressable.set_transport(obj, writer.transport)
        return obj

    def __repr__(self):
        return "<Bolt address=%r protocol_version=%r>" % (self.remote_address,
                                                          self.protocol_version)

    async def __ainit__(self, auth):
        """ Asynchronous initializer for implementation by subclasses.

        :param auth:
        """

    @property
    def age(self):
        """ The age of this connection in seconds.
        """
        return perf_counter() - self.__t_opened

    @property
    def broken(self):
        """ Flag to indicate whether this connection has been broken
        by the network or remote peer.
        """
        return self.__reader.broken or self.__writer.broken

    @property
    def closed(self):
        """ Flag to indicate whether this connection has been closed
        locally."""
        return self.__closed

    async def close(self):
        """ Close the connection.
        """
        if self.closed:
            return
        if not self.broken:
            log.debug("[#%04X] S: <HANGUP>", self.local_address.port_number)
            self.__writer.write_eof()
            self.__writer.close()
            try:
                await self.__writer.wait_closed()
            except BoltConnectionBroken:
                pass
        self.__closed = True

    async def reset(self, force=False):
        """ Reset the connection to a clean state.

        By default, a RESET message will only be sent if required, i.e.
        if the connection is not already in a clean state. If forced,
        this check will be overridden and a RESET will be sent
        regardless.
        """

    async def run(self, cypher, parameters=None, discard=False, readonly=False,
                  bookmarks=None, timeout=None, metadata=None):
        """ Run an auto-commit transaction.

        :param cypher:
        :param parameters:
        :param discard:
        :param readonly:
        :param bookmarks:
        :param timeout:
        :param metadata:
        :raise BoltTransactionError: if a transaction cannot be carried
            out at this time
        """

    async def begin(self, readonly=False, bookmarks=None,
                    timeout=None, metadata=None):
        """ Begin an explicit transaction.

        :param readonly:
        :param bookmarks:
        :param timeout:
        :param metadata:
        :return:
        """

    async def run_tx(self, f, args=None, kwargs=None, readonly=False,
                     bookmarks=None, timeout=None, metadata=None):
        """ Run a transaction function and return the return value from
        that function.
        """


class BoltStreamReader(Addressable, Breakable, StreamReader):
    """ Wrapper for asyncio.streams.StreamReader
    """

    def set_transport(self, transport):
        Addressable.set_transport(self, transport)
        StreamReader.set_transport(self, transport)

    async def readuntil(self, separator=b'\n'):  # pragma: no cover
        assert False  # not used by current implementation

    async def read(self, n=-1):  # pragma: no cover
        assert False  # not used by current implementation

    async def readexactly(self, n):
        try:
            return await super().readexactly(n)
        except IncompleteReadError as err:
            message = ("Network read incomplete (received {} of {} "
                       "bytes)".format(len(err.partial), err.expected))
            log.debug("[#%04X] S: <CLOSE>", self.local_address.port_number)
            Breakable.set_broken(self)
            raise BoltConnectionBroken(message, self.remote_address) from err
        except OSError as err:
            log.debug("[#%04X] S: <CLOSE> %d %s", err.errno, strerror(err.errno))
            Breakable.set_broken(self)
            raise BoltConnectionBroken("Network read failed", self.remote_address) from err


class BoltStreamWriter(Addressable, Breakable, StreamWriter):
    """ Wrapper for asyncio.streams.StreamWriter
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        Addressable.set_transport(self, self.transport)

    async def drain(self):
        try:
            await super().drain()
        except OSError as err:
            log.debug("[#%04X] S: <CLOSE> (%s)", self.local_address.port_number, err)
            Breakable.set_broken(self)
            raise BoltConnectionBroken("Network write failed", self.remote_address) from err

    async def wait_closed(self):
        try:
            await super().wait_closed()
        except AttributeError:  # pragma: no cover
            # This is a dirty hack for Python 3.6, which didn't include
            # 'wait_closed'. The code polls waiting for the stream
            # reader inside the protocol to go away which, by the
            # implementation of 3.6, occurs on 'connection_lost'. This
            # hack is likely safe unless the implementation of 3.6
            # changes in a subsequent patch, and can be removed when
            # Python 3.6 support is no longer required.
            #
            from asyncio import sleep
            try:
                while self._protocol._stream_reader is not None:
                    await sleep(0.1)
            except AttributeError:
                pass


class BoltPool:
    """ A pool of connections to a single address.

    :param opener: a function to which an address can be passed that
        returns an open and ready Bolt connection
    :param address: the remote address for which this pool operates
    :param max_size: the maximum permitted number of simultaneous
        connections that may be owned by this pool, both in-use and
        free
    :param max_age: the maximum permitted age, in seconds, for
        connections to be retained in this pool
    """

    def __init__(self, opener, address, max_size=1, max_age=None):
        self._opener = opener
        self._address = address
        self._max_size = max_size
        self._max_age = max_age
        self._in_use_list = deque()
        self._free_list = deque()
        self._slots = Semaphore(self._max_size)

    def __contains__(self, cx):
        return cx in self._in_use_list or cx in self._free_list

    def __len__(self):
        return self.size

    @property
    def address(self):
        """ The remote address for which this pool operates.
        """
        return self._address

    @property
    def max_size(self):
        """ The maximum permitted number of simultaneous connections
        that may be owned by this pool, both in-use and free.
        """
        return self._max_size

    @property
    def max_age(self):
        """ The maximum permitted age, in seconds, for connections to
        be retained in this pool.
        """
        return self._max_age

    @property
    def in_use(self):
        """ The number of connections in this pool that are currently
        in use.
        """
        return len(self._in_use_list)

    @property
    def free(self):
        """ The number of free connections available in this connection
        pool.
        """
        return len(self._free_list)

    @property
    def size(self):
        """ The number of connections (both in-use and free) currently
        owned by this connection pool.
        """
        return self.in_use + self.free

    async def acquire(self):
        """ Acquire a connection from the pool.

        In the simplest case, this will return an existing open
        connection, if one is free. If not, and the pool is not full,
        a new connection will be created. If the pool is full and no
        free connections are available, this will block until a
        connection is released, or until the acquire call is cancelled.
        """
        cx = None
        while cx is None or cx.broken or cx.closed:
            try:
                # Plan A: select a free connection from the pool
                cx = self._free_list.popleft()
            except IndexError:
                if self.size < self.max_size:
                    # Plan B: if the pool isn't full, open a new connection
                    cx = await self._opener(self.address)
                else:
                    # Plan C: wait for an in-use connection to become available
                    await self._slots.acquire()
            else:
                expired = self.max_age is not None and cx.age > self.max_age
                if expired:
                    await cx.close()
                else:
                    await cx.reset(force=True)
        self._in_use_list.append(cx)
        return cx

    async def release(self, cx):
        """ Release a Bolt connection back into the pool.

        :param cx: the connection to release
        :raise ValueError: if the connection is not currently in use,
            or if it does not belong to this pool
        """
        if cx in self._in_use_list:
            self._in_use_list.remove(cx)
            await cx.reset()
            self._free_list.append(cx)
            self._slots.release()
        elif cx in self._free_list:
            raise ValueError("Connection is not in use")
        else:
            raise ValueError("Connection does not belong to this pool")

    async def prune(self):
        """ Close all free connections.
        """
        await self.__close(self._free_list)

    async def close(self):
        """ Close all connections.

        This does not permanently disable the connection pool, merely
        ensures all open connections are shut down, including those in
        use. It is perfectly acceptable to re-acquire connections after
        pool closure, which will have the implicit affect of reopening
        the pool.
        """
        await self.prune()
        await self.__close(self._in_use_list)

    @classmethod
    async def __close(cls, connections):
        """ Close all connections in the given list.
        """
        closers = deque()
        while True:
            try:
                cx = connections.popleft()
            except IndexError:
                break
            else:
                closers.append(cx.close())
        if closers:
            await wait(closers)
