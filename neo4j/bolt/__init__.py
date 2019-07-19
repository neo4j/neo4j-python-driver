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
    get_event_loop,
    StreamReader,
    StreamReaderProtocol,
    StreamWriter,
    IncompleteReadError,
)
from logging import getLogger
from os import strerror
from ssl import SSLError
from sys import platform, version_info


from neo4j.addressing import Address
from neo4j.api import Security, Version
from neo4j.bolt.error import (
    BoltError,
    BoltConnectionError,
    BoltSecurityError,
    BoltConnectionLost,
    BoltHandshakeError,
)
from neo4j.meta import version as neo4j_version


log = getLogger("neo4j")


MAGIC = b"\x60\x60\xB0\x17"


class Addressable:
    """ Mixin for providing access to local and remote address
    properties via an asyncio.Transport object.
    """

    __transport = None

    def _set_transport(self, transport):
        self.__transport = transport

    @property
    def local_address(self):
        return Address(self.__transport.get_extra_info("sockname"))

    @property
    def remote_address(self):
        return Address(self.__transport.get_extra_info("peername"))


class Bolt(Addressable, object):

    #: Security configuration for this connection.
    security = None

    #: As a class attribute, this denotes the version of Bolt handled
    #: by that subclass. As an instance attribute, this represents the
    #: version of the protocol in use.
    protocol_version = ()

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
        from neo4j.bolt.v3 import Bolt3

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
    async def open(cls, address, auth=None, security=False, protocol_version=None, loop=None):
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
        reader, writer, security = await cls._connect(Address(address), security, loop)

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
        except SSLError as error:
            log.debug("[#%04X] S: <REJECT> %s (%d %s)", 0, address,
                      error.errno, strerror(error.errno))
            raise BoltSecurityError("Failed to establish a secure connection", address) from error
        except OSError as error:
            log.debug("[#%04X] S: <REJECT> %s (%d %s)", 0, address,
                      error.errno, strerror(error.errno))
            raise BoltConnectionError("Failed to establish a connection", address) from error
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
        except ValueError as error:
            writer.close()
            raise BoltHandshakeError("Unexpected handshake response %r" % response_data,
                                     remote_address, request_data, response_data) from error
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
        obj.__writer = writer
        Addressable._set_transport(obj, writer.transport)
        return obj

    def __repr__(self):
        return "<Bolt address=%r protocol_version=%r>" % (self.remote_address,
                                                          self.protocol_version)

    async def __ainit__(self, auth):
        """ Asynchronous initializer for implementation by subclasses.

        :param auth:
        """

    @property
    def closed(self):
        return self.__closed

    async def close(self):
        """ Close the connection.
        """
        if self.closed:
            return
        log.debug("[#%04X] S: <HANGUP>", self.local_address.port_number)
        self.__writer.write_eof()
        self.__writer.close()
        try:
            await self.__writer.wait_closed()
        except BoltConnectionLost:
            pass
        finally:
            self.__closed = True

    async def run(self, cypher, parameters=None, readonly=False, bookmarks=None,
                  metadata=None, timeout=None):
        """ Run an auto-commit transaction.

        :param cypher:
        :param parameters:
        :param readonly:
        :param bookmarks:
        :param metadata:
        :param timeout:
        :raise BoltTransactionError: if a transaction cannot be carried
            out at this time
        """

    async def begin(self, readonly=False, bookmarks=None,
                    metadata=None, timeout=None):
        """ Begin an explicit transaction.

        :param readonly:
        :param bookmarks:
        :param metadata:
        :param timeout:
        :return:
        """

    async def run_tx(self, work, readonly=False, bookmarks=None,
                     metadata=None, timeout=None):
        """ Run a transaction function and return the return value from
        that function.
        """


class BoltStreamReader(Addressable, StreamReader):
    """ Wrapper for asyncio.streams.StreamReader
    """

    defunct = False

    def set_transport(self, transport):
        Addressable._set_transport(self, transport)
        StreamReader.set_transport(self, transport)

    async def readuntil(self, separator=b'\n'):  # pragma: no cover
        assert False  # not used by current implementation

    async def read(self, n=-1):  # pragma: no cover
        assert False  # not used by current implementation

    async def readexactly(self, n):
        try:
            return await super().readexactly(n)
        except IncompleteReadError as error:
            message = ("Network read incomplete (received {} of {} "
                       "bytes)".format(len(error.partial), error.expected))
            log.debug("[#%04X] S: <CLOSE>", self.local_address.port_number, message)
            raise BoltConnectionLost(message, self.remote_address) from error
        except OSError as error:
            log.debug("[#%04X] S: <CLOSE> %d %s", error.errno, strerror(error.errno))
            raise BoltConnectionLost("Network read failed", self.remote_address) from error


class BoltStreamWriter(Addressable, StreamWriter):
    """ Wrapper for asyncio.streams.StreamWriter
    """

    defunct = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        Addressable._set_transport(self, self.transport)

    async def drain(self):
        try:
            await super().drain()
        except OSError as error:
            log.debug("[#%04X] S: <CLOSE> (%s)", self.local_address.port_number, error)
            raise BoltConnectionLost("Network write failed", self.remote_address) from error

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
