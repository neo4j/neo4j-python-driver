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


from collections import deque
from inspect import iscoroutinefunction
from logging import getLogger
from time import perf_counter
from warnings import warn

from neo4j import DEFAULT_PORT
from neo4j.addressing import Address
from neo4j.aio import Bolt
from neo4j._collections import OrderedSet
from neo4j.aio._mixins import Addressable
from neo4j.api import Bookmark, Version
from neo4j.data import Record
from neo4j.errors import (
    BoltError,
    BoltFailure,
    BoltConnectionBroken,
    BoltConnectionClosed,
    BoltTransactionError,
    BoltRoutingError,
)
from neo4j.packstream import PackStream, Structure


log = getLogger("neo4j")


class IgnoredType:

    def __new__(cls):
        return Ignored

    def __bool__(self):
        return False

    def __repr__(self):
        return "Ignored"


Ignored = object.__new__(IgnoredType)


class Summary:

    def __init__(self, metadata, success):
        self._metadata = metadata
        self._success = bool(success)

    def __bool__(self):
        return self._success

    def __repr__(self):
        return "<{} {}>".format(
            self.__class__.__name__,
            " ".join("{}={!r}".format(k, v) for k, v in sorted(self._metadata.items())))

    @property
    def metadata(self):
        return self._metadata

    @property
    def success(self):
        return self._success


class Response:
    """ Collector for response data, consisting of an optional
    sequence of records and a mandatory summary.
    """

    result = None

    def __init__(self, courier):
        self._courier = courier
        self._records = deque()
        self._summary = None

    def put_record(self, record):
        """ Append a record to the end of the record deque.

        :param record:
        """
        self._records.append(record)

    async def get_record(self):
        """ Fetch and return the next record from the top of the
        record deque.

        :return:
        """

        # R = has records
        # S = has summary
        #
        # R=0, S=0 - fetch, check again
        # R=1, S=0 - pop
        # R=0, S=1 - raise stop
        # R=1, S=1 - pop
        while True:
            try:
                return self._records.popleft()
            except IndexError:
                if self._summary is None:
                    await self._courier.fetch(stop=lambda: bool(self._records))
                else:
                    return None

    def put_summary(self, summary):
        """ Update the stored summary value.

        :param summary:
        """
        self._summary = summary

    async def get_summary(self):
        """ Fetch and return the summary value.

        :return:
        """
        await self._courier.fetch(stop=lambda: self._summary is not None)
        return self._summary


class Result:
    """ The result of a Cypher execution.
    """

    def __init__(self, tx, head, body):
        self._tx = tx
        self._head = head
        self._body = body
        self._head.result = self
        self._body.result = self

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            values = await self._body.get_record()
        except BoltFailure as failure:
            # FAILURE
            await self._tx.fail(failure)
        else:
            # RECORD or end of records
            if values is None:
                raise StopAsyncIteration
            else:
                return Record(zip(await self.fields(), values))

    @property
    def transaction(self):
        return self._tx

    async def get_header(self):
        try:
            header = await self._head.get_summary()
        except BoltFailure as failure:
            # FAILURE
            await self._tx.fail(failure)
        else:
            # SUCCESS or IGNORED
            return header

    async def consume(self):
        try:
            footer = await self._body.get_summary()
        except BoltFailure as failure:
            # FAILURE
            await self._tx.fail(failure)
        else:
            # SUCCESS or IGNORED
            # The return value of this function can be used as a
            # predicate, since SUCCESS will return a Summary that
            # coerces to True, and IGNORED will return Ignored, which
            # coerces to False.
            return footer

    async def fields(self):
        header = await self.get_header()
        return header.metadata.get("fields", ())

    async def single(self):
        """ Obtain the next and only remaining record from this result.

        A warning is generated if more than one record is available but
        the first of these is still returned.

        :return: the next :class:`.Record` or :const:`None` if no
            records remain
        :warn: if more than one record is available
        """
        records = [record async for record in self]
        size = len(records)
        if size == 0:
            return None
        if size != 1:
            warn("Expected a result with a single record, but this result contains %d" % size)
        return records[0]


class Transaction:

    @classmethod
    async def begin(cls, courier, readonly=False, bookmarks=None,
                    timeout=None, metadata=None):
        """ Begin an explicit transaction.
        """
        tx = cls(courier, readonly=readonly, bookmarks=bookmarks, timeout=timeout,
                 metadata=metadata)
        tx._autocommit = False
        courier.write_begin(tx._extras)
        if bookmarks:
            # If bookmarks are passed, BEGIN should sync to the
            # network. This ensures that any failures that occur are
            # raised at an appropriate time, rather than later in the
            # transaction. Conversely, if no bookmarks are passed, it
            # should be fine to sync lazily.
            await courier.send()
            await courier.fetch()
        return tx

    def _add_extra(self, key, coercion=lambda x: x, **values):
        for name, value in values.items():
            if value:
                try:
                    self._extras[key] = coercion(value)
                except TypeError:
                    raise TypeError("Unsupported type for {} {!r}".format(name, value))

    def __init__(self, courier, readonly=False, bookmarks=None, timeout=None, metadata=None):
        """

        :param courier:
        :param readonly: if true, the transaction should be readonly,
            otherwise it should have full read/write access
        :param bookmarks: iterable of bookmarks which must all have
            been seen by the server before this transaction begins
        :param timeout: a transaction execution timeout, passed to the
            database kernel on execution
        :param metadata: application metadata tied to this transaction;
            generally used for audit purposes
        """
        self._courier = courier
        self._autocommit = True
        self._closed = False
        self._failure = None
        self._extras = {}
        self._add_extra("mode", lambda x: "R" if x else None, readonly=readonly)
        self._add_extra("bookmarks", list, bookmarks=bookmarks)
        self._add_extra("tx_timeout", lambda x: int(1000 * x), timeout=timeout)
        self._add_extra("tx_metadata", dict, metadata=metadata)

    @property
    def autocommit(self):
        return self._autocommit

    @property
    def closed(self):
        return self._closed

    @property
    def failure(self):
        return self._failure

    async def run(self, cypher, parameters=None, discard=False):
        self._assert_open()
        head = self._courier.write_run(cypher, dict(parameters or {}),
                                       self._extras if self._autocommit else {})
        if discard:
            body = self._courier.write_discard_all()
        else:
            body = self._courier.write_pull_all()
        if self._autocommit:
            try:
                await self._courier.send()
            finally:
                self._closed = True
        return Result(self, head, body)

    async def evaluate(self, cypher, parameters=None, key=0, default=None):
        """ Run Cypher and return a single value (by default the first
        value) from the first and only record.
        """
        result = await self.run(cypher, parameters)
        record = await result.single()
        return record.value(key, default)

    async def commit(self):
        self._assert_open()
        if self._autocommit:
            raise BoltTransactionError("Cannot explicitly commit an auto-commit "
                                       "transaction", self._courier.remote_address)
        try:
            commit = self._courier.write_commit()
            await self._courier.send()
            await self._courier.fetch()
            summary = await commit.get_summary()
            return Bookmark(summary.metadata.get("bookmark"))
        finally:
            self._closed = True

    async def rollback(self):
        self._assert_open()
        if self._autocommit:
            raise BoltTransactionError("Cannot explicitly rollback an auto-commit "
                                       "transaction", self._courier.remote_address)
        try:
            self._courier.write_rollback()
            await self._courier.send()
            await self._courier.fetch()
        finally:
            self._closed = True

    async def fail(self, failure):
        """ Called internally with a BoltFailure object when a FAILURE
        message is received. This will reset the connection, close the
        transaction and raise the failure exception.

        :param failure:
        :return:
        """
        if not self._failure:
            self._courier.write_reset()
            await self._courier.send()
            await self._courier.fetch()
            self._closed = True
            self._failure = failure
            raise self._failure

    def _assert_open(self):
        if self.closed:
            raise BoltTransactionError("Transaction is already "
                                       "closed", self._courier.remote_address)


class RoutingTable:

    @classmethod
    def parse_routing_info(cls, servers, ttl):
        """ Parse the records returned from the procedure call and
        return a new RoutingTable instance.
        """
        routers = []
        readers = []
        writers = []
        try:
            for server in servers:
                role = server["role"]
                addresses = []
                for address in server["addresses"]:
                    addresses.append(Address.parse(address, default_port=DEFAULT_PORT))
                if role == "ROUTE":
                    routers.extend(addresses)
                elif role == "READ":
                    readers.extend(addresses)
                elif role == "WRITE":
                    writers.extend(addresses)
        except (KeyError, TypeError):
            raise ValueError("Cannot parse routing info")
        else:
            return cls(routers, readers, writers, ttl)

    def __init__(self, routers=(), readers=(), writers=(), ttl=0):
        self.routers = OrderedSet(routers)
        self.readers = OrderedSet(readers)
        self.writers = OrderedSet(writers)
        self.last_updated_time = perf_counter()
        self.ttl = ttl

    def __repr__(self):
        return "RoutingTable(routers=%r, readers=%r, writers=%r, last_updated_time=%r, ttl=%r)" % (
            self.routers,
            self.readers,
            self.writers,
            self.last_updated_time,
            self.ttl,
        )

    def __contains__(self, address):
        return address in self.routers or address in self.readers or address in self.writers

    def is_fresh(self, readonly=False):
        """ Indicator for whether routing information is still usable.
        """
        assert isinstance(readonly, bool)
        log.debug("[#0000]  C: <ROUTING> Checking table freshness (readonly=%r)", readonly)
        expired = self.last_updated_time + self.ttl <= perf_counter()
        if readonly:
            has_server_for_mode = bool(self.readers)
        else:
            has_server_for_mode = bool(self.writers)
        log.debug("[#0000]  C: <ROUTING> Table expired=%r", expired)
        log.debug("[#0000]  C: <ROUTING> Table routers=%r", self.routers)
        log.debug("[#0000]  C: <ROUTING> Table has_server_for_mode=%r", has_server_for_mode)
        return not expired and self.routers and has_server_for_mode

    def update(self, new_routing_table):
        """ Update the current routing table with new routing information
        from a replacement table.
        """
        self.routers.replace(new_routing_table.routers)
        self.readers.replace(new_routing_table.readers)
        self.writers.replace(new_routing_table.writers)
        self.last_updated_time = perf_counter()
        self.ttl = new_routing_table.ttl
        log.debug("[#0000]  S: <ROUTING> table=%r", self)

    def servers(self):
        return set(self.routers) | set(self.writers) | set(self.readers)


class Courier(Addressable, object):

    def __init__(self, reader, writer, on_failure):
        self._stream = PackStream(reader, writer)
        self._fail = on_failure
        self._requests_to_send = 0
        self._responses = deque()
        Addressable.set_transport(self, writer.transport)

    @property
    def requests_to_send(self):
        return self._requests_to_send

    @property
    def responses_to_fetch(self):
        return len(self._responses)

    @property
    def connection_id(self):
        return self.local_address.port_number

    def write_hello(self, extras):
        logged_extras = dict(extras)
        if "credentials" in logged_extras:
            logged_extras["credentials"] = "*******"
        log.debug("[#%04X] C: HELLO %r", self.connection_id, logged_extras)
        return self._write(Structure(b"\x01", extras))

    def write_goodbye(self):
        log.debug("[#%04X] C: GOODBYE", self.connection_id)
        return self._write(Structure(b"\x02"))

    def write_reset(self):
        log.debug("[#%04X] C: RESET", self.connection_id)
        return self._write(Structure(b"\x0F"))

    def write_run(self, cypher, parameters, extras):
        parameters = dict(parameters or {})
        extras = dict(extras or {})
        log.debug("[#%04X] C: RUN %r %r %r", self.connection_id, cypher, parameters, extras)
        return self._write(Structure(b"\x10", cypher, parameters, extras))

    def write_begin(self, extras):
        log.debug("[#%04X] C: BEGIN %r", self.connection_id, extras)
        return self._write(Structure(b"\x11", extras))

    def write_commit(self):
        log.debug("[#%04X] C: COMMIT", self.connection_id)
        return self._write(Structure(b"\x12"))

    def write_rollback(self):
        log.debug("[#%04X] C: ROLLBACK", self.connection_id)
        return self._write(Structure(b"\x13"))

    def write_discard_all(self):
        log.debug("[#%04X] C: DISCARD_ALL", self.connection_id)
        return self._write(Structure(b"\x2F"))

    def write_pull_all(self):
        log.debug("[#%04X] C: PULL_ALL", self.connection_id)
        return self._write(Structure(b"\x3F"))

    def _write(self, message):
        self._stream.write_message(message)
        self._requests_to_send += 1
        response = Response(self)
        self._responses.append(response)
        return response

    async def send(self):
        log.debug("[#%04X] C: <SEND>", self.connection_id)
        await self._stream.drain()
        self._requests_to_send = 0

    async def fetch(self, stop=lambda: None):
        """ Fetch zero or more messages, stopping when no more pending
        responses need to be populated, when the stop condition
        is fulfilled, or when a failure is encountered (for which an
        exception will be raised).

        :param stop:
        """
        while self.responses_to_fetch and not stop():
            fetched = await self._read()
            if isinstance(fetched, list):
                self._responses[0].put_record(fetched)
            else:
                response = self._responses.popleft()
                response.put_summary(fetched)
                if isinstance(fetched, Summary) and not fetched.success:
                    code = fetched.metadata.get("code")
                    message = fetched.metadata.get("message")
                    failure = BoltFailure(message, self.remote_address, code, response)
                    self._fail(failure)

    async def _read(self):
        message = await self._stream.read_message()
        if not isinstance(message, Structure):
            # TODO: log, signal defunct and close
            raise BoltError("Received illegal message "
                            "type {}".format(type(message)), self.remote_address)
        if message.tag == b"\x70":
            metadata = message.fields[0]
            log.debug("[#%04X] S: SUCCESS %r", self.connection_id, metadata)
            return Summary(metadata, success=True)
        elif message.tag == b"\x71":
            data = message.fields[0]
            log.debug("[#%04X] S: RECORD %r", self.connection_id, data)
            return data
        elif message.tag == b"\x7E":
            log.debug("[#%04X] S: IGNORED", self.connection_id)
            return Ignored
        elif message.tag == b"\x7F":
            metadata = message.fields[0]
            log.debug("[#%04X] S: FAILURE %r", self.connection_id, metadata)
            return Summary(metadata, success=False)
        else:
            # TODO: log, signal defunct and close
            raise BoltError("Received illegal message structure "
                            "tag {}".format(message.tag), self.remote_address)


class Bolt3(Bolt):

    protocol_version = Version(3, 0)

    server_agent = None

    connection_id = None

    def __init__(self, reader, writer):
        self._courier = Courier(reader, writer, self.fail)
        self._tx = None
        self._failure_handlers = {}

    async def __ainit__(self, auth):
        args = {
            "scheme": "none",
            "user_agent": self.default_user_agent(),
        }
        if auth:
            args.update({
                "scheme": "basic",
                "principal": auth[0],  # TODO
                "credentials": auth[1],  # TODO
            })
        response = self._courier.write_hello(args)
        await self._courier.send()
        summary = await response.get_summary()
        if summary.success:
            self.server_agent = summary.metadata.get("server")
            self.connection_id = summary.metadata.get("connection_id")
            # TODO: verify genuine product
        else:
            await super().close()
            code = summary.metadata.get("code")
            message = summary.metadata.get("message")
            failure = BoltFailure(message, self.remote_address, code, response)
            self.fail(failure)

    async def close(self):
        if self.closed:
            return
        if not self.broken:
            self._courier.write_goodbye()
            try:
                await self._courier.send()
            except BoltConnectionBroken:
                pass
        await super().close()

    @property
    def ready(self):
        """ If true, this flag indicates that there is no transaction
        in progress, and one may be started.
        """
        return not self._tx or self._tx.closed

    def _assert_open(self):
        if self.closed:
            raise BoltConnectionClosed("Connection has been closed", self.remote_address)
        if self.broken:
            raise BoltConnectionBroken("Connection is broken", self.remote_address)

    def _assert_ready(self):
        self._assert_open()
        if not self.ready:
            # TODO: add transaction identifier
            raise BoltTransactionError("A transaction is already in progress on "
                                       "this connection", self.remote_address)

    async def reset(self, force=False):
        self._assert_open()
        if force or not self.ready:
            self._courier.write_reset()
        if self._courier.requests_to_send:
            await self._courier.send()
        if self._courier.responses_to_fetch:
            await self._courier.fetch()

    async def run(self, cypher, parameters=None, discard=False, readonly=False,
                  bookmarks=None, timeout=None, metadata=None):
        self._assert_ready()
        self._tx = Transaction(self._courier, readonly=readonly, bookmarks=bookmarks,
                               timeout=timeout, metadata=metadata)
        return await self._tx.run(cypher, parameters, discard=discard)

    async def begin(self, readonly=False, bookmarks=None,
                    timeout=None, metadata=None):
        self._assert_ready()
        self._tx = await Transaction.begin(self._courier, readonly=readonly, bookmarks=bookmarks,
                                           timeout=timeout, metadata=metadata)
        return self._tx

    async def run_tx(self, f, args=None, kwargs=None, readonly=False,
                     bookmarks=None, timeout=None, metadata=None):
        self._assert_open()
        tx = await self.begin(readonly=readonly, bookmarks=bookmarks,
                              timeout=None, metadata=metadata)
        if not iscoroutinefunction(f):
            raise TypeError("Transaction function must be awaitable")
        try:
            value = await f(tx, *(args or ()), **(kwargs or {}))
        except Exception:
            await tx.rollback()
            raise
        else:
            await tx.commit()
            return value

    async def get_routing_table(self, context=None):
        try:
            result = await self.run("CALL dbms.cluster.routing.getRoutingTable({context})",
                                    {"context": dict(context or {})})
            record = await result.single()
            if not record:
                raise BoltRoutingError("Routing table call returned "
                                       "no data", self.remote_address)
            assert isinstance(record, Record)
            servers = record["servers"]
            ttl = record["ttl"]
            log.debug("[#%04X] S: <ROUTING> servers=%r ttl=%r",
                      self.local_address.port_number, servers, ttl)
            return RoutingTable.parse_routing_info(servers, ttl)
        except BoltFailure as error:
            if error.title == "ProcedureNotFound":
                raise BoltRoutingError("Server does not support "
                                       "routing", self.remote_address) from error
            else:
                raise
        except ValueError as error:
            raise BoltRoutingError("Invalid routing table", self.remote_address) from error

    def fail(self, failure):
        t = type(failure)
        handler = self.get_failure_handler(t)
        if callable(handler):
            # TODO: fix "requires two params, only one was given" error
            handler(failure)
        else:
            raise failure

    def get_failure_handler(self, cls):
        return self._failure_handlers.get(cls)

    def set_failure_handler(self, cls, f):
        self._failure_handlers[cls] = f

    def del_failure_handler(self, cls):
        try:
            del self._failure_handlers[cls]
        except KeyError:
            pass
