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
    Lock,
    StreamReader,
    StreamReaderProtocol,
    StreamWriter,
    get_event_loop,
    wait,
)
from collections import deque
from logging import getLogger
from os import strerror
from random import choice
from ssl import SSLError
from sys import platform, version_info
from time import perf_counter

from neo4j.addressing import Address
from neo4j.aio._collections import WaitingList
from neo4j.aio._mixins import Addressable, Breakable
from neo4j.errors import (
    BoltError,
    BoltConnectionError,
    BoltSecurityError,
    BoltConnectionBroken,
    BoltHandshakeError,
    Neo4jAvailabilityError,
)
from neo4j.api import Version
from neo4j import PoolConfig
from neo4j.conf import Config
from neo4j.meta import version as neo4j_version
from neo4j.routing import RoutingTable


log = getLogger(__name__)


MAGIC = b"\x60\x60\xB0\x17"


class Bolt(Addressable, object):

    #: True if this instance uses secure communication, false
    #: otherwise.
    secure = None

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
        from neo4j.aio.bolt3 import Bolt3

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
    def opener(cls, auth=None, **config):
        """ Create and return an opener function for a given set of
        configuration parameters. This is useful when multiple servers share
        the same configuration details, such as within a connection pool.
        """

        async def f(address, *, loop=None):
            return await Bolt.open(address, auth=auth, loop=loop, **config)

        return f

    @classmethod
    async def open(cls, address, *, auth=None, loop=None, **config):
        """ Open a socket connection and perform protocol version
        negotiation, in order to construct and return a Bolt client
        instance for a supported Bolt protocol version.

        :param address: tuples of host and port, such as
                        ("127.0.0.1", 7687)
        :param auth:
        :param loop:
        :param config:
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

        # Args
        address = Address(address)
        if loop is None:
            loop = get_event_loop()
        config = PoolConfig._consume(config)

        # Connect
        reader, writer = await cls._connect(address, loop, config)

        try:

            # Handshake
            subclass = await cls._handshake(reader, writer, config.protocol_version)

            # Instantiation
            obj = subclass(reader, writer)
            obj.secure = bool(config.secure)
            assert hasattr(obj, "__ainit__")
            await obj.__ainit__(auth)
            return obj

        except BoltError:
            writer.write_eof()
            writer.close()
            raise

    @classmethod
    async def _connect(cls, address, loop, config):
        """ Attempt to establish a TCP connection to the address
        provided.

        :param address:
        :param loop:
        :param config:
        :return: a 3-tuple of reader, writer and security settings for
            the new connection
        :raise BoltConnectionError: if a connection could not be
            established
        """
        assert isinstance(address, Address)
        assert loop is not None
        assert isinstance(config, Config)
        connection_args = {
            "host": address.host,
            "port": address.port,
            "family": address.family,
            # TODO: other args
        }
        ssl_context = config.get_ssl_context()
        if ssl_context:
            connection_args["ssl"] = ssl_context
            connection_args["server_hostname"] = address.host
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
            return reader, writer

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

    async def get_routing_table(self, context=None):
        """ Fetch a new routing table.

        :param context: the routing context to use for this call
        :return: a new RoutingTable instance or None if the given router is
                 currently unable to provide routing information
        :raise ServiceUnavailable: if no writers are available
        :raise ProtocolError: if the routing information received is unusable
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

    @classmethod
    async def open(cls, opener, address, *, size=1, max_size=1, max_age=None, loop=None):
        """ Create a new connection pool, with an option to seed one
        or more initial connections.
        """
        pool = cls(opener, address, max_size=max_size, max_age=max_age, loop=loop)
        seeds = [await pool.acquire() for _ in range(size)]
        for seed in seeds:
            await pool.release(seed)
        return pool

    def __init__(self, opener, address, *, max_size=1, max_age=None, loop=None):
        self._opener = opener
        self._address = Address(address)
        self._max_size = max_size
        self._max_age = max_age
        self._loop = loop
        self._in_use_list = deque()
        self._free_list = deque()
        self._waiting_list = WaitingList(loop=self._loop)

    def __repr__(self):
        return "<{} addr'{}' [{}{}{}]>".format(
            self.__class__.__name__,
            self.address,
            "|" * len(self._in_use_list),
            "." * len(self._free_list),
            " " * (self.max_size - self.size),
        )

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

    @max_size.setter
    def max_size(self, value):
        old_value = self._max_size
        self._max_size = value
        if value > old_value:
            # The maximum size has grown, so new slots have become
            # available. Notify any waiting acquirers of this extra
            # capacity.
            self._waiting_list.notify()

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
    def size(self):
        """ The total number of connections (both in-use and free)
        currently owned by this connection pool.
        """
        return len(self._in_use_list) + len(self._free_list)

    async def _sanitize(self, cx, *, force_reset):
        """ Attempt to clean up a connection, such that it can be
        reused.

        If the connection is broken or closed, it can be discarded.
        Otherwise, the age of the connection is checked against the
        maximum age permitted by this pool, consequently closing it
        on expiry.

        Should the connection be neither broken, closed nor expired,
        it will be reset (optionally forcibly so) and the connection
        object will be returned, indicating success.
        """
        if cx.broken or cx.closed:
            return None
        expired = self.max_age is not None and cx.age > self.max_age
        if expired:
            await cx.close()
            return None
        await cx.reset(force=force_reset)
        return cx

    async def acquire(self, *, force_reset=False):
        """ Acquire a connection from the pool.

        In the simplest case, this will return an existing open
        connection, if one is free. If not, and the pool is not full,
        a new connection will be created. If the pool is full and no
        free connections are available, this will block until a
        connection is released, or until the acquire call is cancelled.

        :param force_reset: if true, the connection will be forcibly
            reset before being returned; if false, this will only occur
            if the connection is not already in a clean state
        :return: a Bolt connection object
        """
        log.debug("Acquiring connection from pool %r", self)
        cx = None
        while cx is None or cx.broken or cx.closed:
            try:
                # Plan A: select a free connection from the pool
                cx = self._free_list.popleft()
            except IndexError:
                if self.size < self.max_size:
                    # Plan B: if the pool isn't full, open
                    # a new connection
                    cx = await self._opener(self.address, loop=self._loop)
                else:
                    # Plan C: wait for more capacity to become
                    # available, then try again
                    log.debug("Joining waiting list")
                    await self._waiting_list.join()
            else:
                cx = await self._sanitize(cx, force_reset=force_reset)
        self._in_use_list.append(cx)
        return cx

    async def release(self, cx, *, force_reset=False):
        """ Release a Bolt connection, putting it back into the pool
        if the connection is healthy and the pool is not already at
        capacity.

        :param cx: the connection to release
        :param force_reset: if true, the connection will be forcibly
            reset before being released back into the pool; if false,
            this will only occur if the connection is not already in a
            clean state
        :raise ValueError: if the connection is not currently in use,
            or if it does not belong to this pool
        """
        log.debug("Releasing connection %r", cx)
        if cx in self._in_use_list:
            self._in_use_list.remove(cx)
            if self.size < self.max_size:
                # If there is spare capacity in the pool, attempt to
                # sanitize the connection and return it to the pool.
                cx = await self._sanitize(cx, force_reset=force_reset)
                if cx:
                    # Carry on only if sanitation succeeded.
                    if self.size < self.max_size:
                        # Check again if there is still capacity.
                        self._free_list.append(cx)
                        self._waiting_list.notify()
                    else:
                        # Otherwise, close the connection.
                        await cx.close()
            else:
                # If the pool is full, simply close the connection.
                await cx.close()
        elif cx in self._free_list:
            raise ValueError("Connection is not in use")
        else:
            raise ValueError("Connection does not belong to this pool")

    async def prune(self):
        """ Close all free connections.
        """
        await self.__close(self._free_list)

    async def close(self):
        """ Close all connections immediately.

        This does not permanently disable the connection pool, it
        merely shuts down all open connections, including those in
        use. Depending on the applications, it may be perfectly
        acceptable to re-acquire connections after pool closure,
        which will have the implicit affect of reopening the pool.

        To close gracefully, allowing work in progress to continue
        until connections are released, use the following sequence
        instead:

            pool.max_size = 0
            pool.prune()

        This will force all future connection acquisitions onto the
        waiting list, and released connections will be closed instead
        of being returned to the pool.
        """
        await self.prune()
        await self.__close(self._in_use_list)

    async def __close(self, connections):
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
            await wait(closers, loop=self._loop)


class Neo4jPool:
    """ Connection pool with routing table.
    """

    @classmethod
    async def open(cls, opener, *addresses, routing_context=None, max_size_per_host=100, loop=None):
        # TODO: get initial routing table and construct
        obj = cls(opener, *addresses, routing_context=routing_context,
                  max_size_per_host=max_size_per_host, loop=loop)
        await obj._ensure_routing_table_is_fresh()
        return obj

    def __init__(self, opener, *addresses, routing_context=None, max_size_per_host=100, loop=None):
        if loop is None:
            self._loop = get_event_loop()
        else:
            self._loop = loop
        self._pools = {}
        self._missing_writer = False
        self._refresh_lock = Lock(loop=self._loop)
        self._opener = opener
        self._routing_context = routing_context
        self._max_size_per_host = max_size_per_host
        self._initial_routers = addresses
        self._routing_table = RoutingTable(addresses)
        self._activate_new_pools_in(self._routing_table)

    def _activate_new_pools_in(self, routing_table):
        """ Add pools for addresses that exist in the given routing
        table but which don't already have pools.
        """
        for address in routing_table.servers():
            if address not in self._pools:
                self._pools[address] = BoltPool(self._opener, address,
                                                max_size=self._max_size_per_host,
                                                loop=self._loop)

    async def _deactivate_pools_not_in(self, routing_table):
        """ Deactivate any pools that aren't represented in the given
        routing table.
        """
        for address in self._pools:
            if address not in routing_table:
                await self._deactivate(address)

    async def _get_routing_table_from(self, *routers):
        """ Try to update routing tables with the given routers.

        :return: True if the routing table is successfully updated,
        otherwise False
        """
        log.debug("Attempting to update routing table from "
                  "{}".format(", ".join(map(repr, routers))))
        for router in routers:
            pool = self._pools[router]
            cx = await pool.acquire()
            try:
                new_routing_table = await cx.get_routing_table(self._routing_context)
            except BoltError:
                await self._deactivate(router)
            else:
                num_routers = len(new_routing_table.routers)
                num_readers = len(new_routing_table.readers)
                num_writers = len(new_routing_table.writers)

                # No writers are available. This likely indicates a temporary state,
                # such as leader switching, so we should not signal an error.
                # When no writers available, then we flag we are reading in absence of writer
                self._missing_writer = (num_writers == 0)

                # No routers
                if num_routers == 0:
                    continue

                # No readers
                if num_readers == 0:
                    continue

                log.debug("Successfully updated routing table from "
                          "{!r} ({!r})".format(router, self._routing_table))
                return new_routing_table
            finally:
                await pool.release(cx)
        return None

    async def _get_routing_table(self):
        """ Update the routing table from the first router able to provide
        valid routing information.
        """
        # copied because it can be modified
        existing_routers = list(self._routing_table.routers)

        has_tried_initial_routers = False
        if self._missing_writer:
            has_tried_initial_routers = True
            rt = await self._get_routing_table_from(self._initial_routers)
            if rt:
                return rt

        rt = await self._get_routing_table_from(*existing_routers)
        if rt:
            return rt

        if not has_tried_initial_routers and self._initial_routers not in existing_routers:
            rt = await self._get_routing_table_from(self._initial_routers)
            if rt:
                return rt

        # None of the routers have been successful, so just fail
        log.error("Unable to retrieve routing information")
        raise Neo4jAvailabilityError("Unable to retrieve routing information")

    async def _ensure_routing_table_is_fresh(self, readonly=False):
        """ Update the routing table if stale.

        This method performs two freshness checks, before and after acquiring
        the refresh lock. If the routing table is already fresh on entry, the
        method exits immediately; otherwise, the refresh lock is acquired and
        the second freshness check that follows determines whether an update
        is still required.
        """
        if self._routing_table.is_fresh(readonly=readonly):
            return
        async with self._refresh_lock:
            if self._routing_table.is_fresh(readonly=readonly):
                if readonly:
                    # if reader is fresh but writers are not, then
                    # we are reading in absence of writer
                    self._missing_writer = not self._routing_table.is_fresh(readonly=False)
            else:
                rt = await self._get_routing_table()
                self._activate_new_pools_in(rt)
                self._routing_table.update(rt)
                await self._deactivate_pools_not_in(rt)

    async def _select_pool(self, readonly=False):
        """ Selects the pool with the fewest in-use connections.
        """
        await self._ensure_routing_table_is_fresh(readonly=readonly)
        if readonly:
            addresses = self._routing_table.readers
        else:
            addresses = self._routing_table.writers
        pools = [pool for address, pool in self._pools.items() if address in addresses]
        pools_by_usage = {}
        for pool in pools:
            pools_by_usage.setdefault(pool.in_use, []).append(pool)
        if not pools_by_usage:
            raise Neo4jAvailabilityError("No {} service currently "
                                         "available".format("read" if readonly else "write"))
        return choice(pools_by_usage[min(pools_by_usage)])

    async def acquire(self, *, readonly=False, force_reset=False):
        """ Acquire a connection to a server that can satisfy a set of parameters.

        :param readonly: true if a readonly connection is required,
            otherwise false
        :param force_reset:
        """
        while True:
            pool = await self._select_pool(readonly=readonly)
            try:
                cx = await pool.acquire(force_reset=force_reset)
            except BoltError:
                await self._deactivate(pool.address)
            else:
                if not readonly:
                    # If we're not acquiring a connection as
                    # readonly, then intercept NotALeader and
                    # ForbiddenOnReadOnlyDatabase errors to
                    # invalidate the routing table.
                    from neo4j.errors import (
                        NotALeader,
                        ForbiddenOnReadOnlyDatabase,
                    )

                    def handler(failure):
                        """ Invalidate the routing table before raising the failure.
                        """
                        log.debug("[#0000]  C: <ROUTING> Invalidating routing table")
                        self._routing_table.ttl = 0
                        raise failure

                    cx.set_failure_handler(NotALeader, handler)
                    cx.set_failure_handler(ForbiddenOnReadOnlyDatabase, handler)
                return cx

    async def release(self, connection, *, force_reset=False):
        """ Release a connection back into the pool.
        This method is thread safe.
        """
        for pool in self._pools.values():
            try:
                await pool.release(connection, force_reset=force_reset)
            except ValueError:
                pass
            else:
                # Unhook any custom error handling and exit.
                from neo4j.errors import (
                    NotALeader,
                    ForbiddenOnReadOnlyDatabase,
                )
                connection.del_failure_handler(NotALeader)
                connection.del_failure_handler(ForbiddenOnReadOnlyDatabase)
                break
        else:
            raise ValueError("Connection does not belong to this pool")

    async def _deactivate(self, address):
        """ Deactivate an address from the connection pool,
        if present, remove from the routing table and also closing
        all idle connections to that address.
        """
        log.debug("[#0000]  C: <ROUTING> Deactivating address %r", address)
        # We use `discard` instead of `remove` here since the former
        # will not fail if the address has already been removed.
        self._routing_table.routers.discard(address)
        self._routing_table.readers.discard(address)
        self._routing_table.writers.discard(address)
        log.debug("[#0000]  C: <ROUTING> table=%r", self._routing_table)
        try:
            pool = self._pools.pop(address)
        except KeyError:
            pass  # assume the address has already been removed
        else:
            pool.max_size = 0
            await pool.prune()

    async def close(self, force=False):
        """ Close all connections and empty the pool. If forced, in-use
        connections will be closed immediately; if not, they will
        remain open until released.
        """
        pools = dict(self._pools)
        self._pools.clear()
        for address, pool in pools.items():
            if force:
                await pool.close()
            else:
                pool.max_size = 0
                await pool.prune()


class Neo4j:

    # The default router address list to use if no addresses are specified.
    default_router_addresses = Address.parse_list(":7687 :17601 :17687")

    # TODO
    # @classmethod
    # async def open(cls, *addresses, auth=None, security=False, protocol_version=None, loop=None):
    #     opener = Bolt.opener(auth=auth, security=security, protocol_version=protocol_version)
    #     router_addresses = Address.parse_list(" ".join(addresses), default_port=7687)
    #     return cls(opener, router_addresses, loop=loop)
    #
    # def __init__(self, opener, router_addresses, loop=None):
    #     self._routers = Neo4jPool(opener, router_addresses or self.default_router_addresses)
    #     self._writers = Neo4jPool(opener)
    #     self._readers = Neo4jPool(opener)
    #     self._routing_table = None
    #
    # @property
    # def routing_table(self):
    #     return self._routing_table
    #
    # async def update_routing_table(self):
    #     cx = await self._routers.acquire()
    #     try:
    #         result = await cx.run("CALL dbms.cluster.routing.getRoutingTable({context})", {"context": {}})
    #         record = await result.single()
    #         self._routing_table = RoutingTable.parse_routing_info([record])  # TODO: handle ValueError?
    #         return self._routing_table
    #     finally:
    #         self._routers.release(cx)


# async def main():
#     from neo4j.debug import watch; watch("neo4j")
#     neo4j = await Neo4j.open(":17601 :17602 :17603", auth=("neo4j", "password"))
#     await neo4j.update_routing_table()
#     print(neo4j.routing_table)
#
#
# if __name__ == "__main__":
#     run(main())
