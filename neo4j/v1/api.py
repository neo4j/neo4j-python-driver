#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2017 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
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
from random import random
from threading import RLock
from time import time, sleep
from warnings import warn

from neo4j.bolt import ProtocolError, ServiceUnavailable
from neo4j.compat import unicode, urlparse
from neo4j.exceptions import CypherError, TransientError

from .exceptions import DriverError, SessionError, SessionExpired, TransactionError

_warned_about_transaction_bookmarks = False

READ_ACCESS = "READ"
WRITE_ACCESS = "WRITE"

DEFAULT_MAX_RETRY_TIME = 30.0
INITIAL_RETRY_DELAY = 1.0
RETRY_DELAY_MULTIPLIER = 2.0
RETRY_DELAY_JITTER_FACTOR = 0.2


def last_bookmark(b0, b1):
    """ Return the latest of two bookmarks by looking for the maximum
    integer value following the last colon in the bookmark string.
    """
    n = [None, None]
    _, _, n[0] = b0.rpartition(":")
    _, _, n[1] = b1.rpartition(":")
    for i in range(2):
        try:
            n[i] = int(n[i])
        except ValueError:
            raise ValueError("Invalid bookmark: {}".format(b0))
    return b0 if n[0] > n[1] else b1


def retry_delay_generator(initial_delay, multiplier, jitter_factor):
    delay = initial_delay
    while True:
        jitter = jitter_factor * delay
        yield delay - jitter + (2 * jitter * random())
        delay *= multiplier


class ValueSystem(object):
    def hydrate(self, values):
        """ Hydrate values from raw representations into client objects.
        """


class GraphDatabase(object):
    """ The `GraphDatabase` class provides access to all graph
    database functionality. This class is primarily used to construct a
    :class:`.Driver` instance, using the :meth:`.driver` method.
    """

    uri_schemes = {}

    value_systems = {}

    @classmethod
    def driver(cls, uri, **config):
        """ Acquire a :class:`.Driver` instance for the given URI and
        configuration. The URI scheme determines the Driver implementation
        that will be returned. Options are:

            ``bolt``
              Returns a :class:`.DirectDriver`.

            ``bolt+routing``
              Returns a :class:`.RoutingDriver`.

        :param uri: URI for a graph database service
        :param config: configuration and authentication details (valid keys are listed below)

            `auth`
              An authentication token for the server, for example
              ``("neo4j", "password")``.

            `der_encoded_server_certificate`
              The server certificate in DER format, if required.

            `encrypted`
              A boolean flag to determine whether encryption should be used.
              Defaults to :const:`True`.

            `trust`
              Trust level: one of :attr:`.TRUST_ALL_CERTIFICATES` (default) or
              :attr:`.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES`.

            `user_agent`
              A custom user agent string, if required.

        """
        parsed = urlparse(uri)
        try:
            driver_class = cls.uri_schemes[parsed.scheme]
        except KeyError:
            raise ProtocolError("URI scheme %r not supported" % parsed.scheme)
        else:
            return driver_class(uri, **config)


class Driver(object):
    """ The base class for all `Driver` implementations. A Driver is an accessor for
    a specific graph database. It is typically thread-safe, acts as a template for
    :class:`.Session` creation and hosts a connection pool.

    All configuration and authentication settings are held immutably by the
    Driver. Should different settings be required, a new Driver instance
    should be created via the :meth:`.GraphDatabase.driver` method.
    """

    #: Connection pool
    _pool = None

    #: Indicator of driver closure.
    _closed = False

    _lock = None

    def __init__(self, pool, **config):
        self._lock = RLock()
        self._pool = pool
        self._max_retry_time = config.get("max_retry_time", DEFAULT_MAX_RETRY_TIME)

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def session(self, access_mode=None, **parameters):
        """ Create a new session using a connection from the driver connection
        pool. Session creation is a lightweight operation and sessions are
        not thread safe, therefore a session should generally be short-lived
        within a single thread.

        :param access_mode: access mode for this session (read or write)
        :param parameters: set of parameters for this session:

        `bookmark`
            A bookmark after which this session should begin.

        `bookmarks`
            A collection of bookmarks after which this session should begin.

        `max_retry_time`
            The maximum time after which to stop attempting retries of failed
            transactions.

        :returns: new :class:`.Session` object
        """
        if self.closed():
            raise DriverError("Driver closed")

    def close(self):
        """ Shut down, closing any open connections that were spawned by
        this Driver.
        """
        if self._lock is None:
            return
        with self._lock:
            if not self._closed:
                self._closed = True
                if self._pool is not None:
                    self._pool.close()
                    self._pool = None

    def closed(self):
        with self._lock:
            return self._closed


class Session(object):
    """ A `Session` is a logical context for transactional units of work.
    It typically wraps a TCP connection and should generally be constructed
    using the :meth:`.Driver.session` method.

    Sessions are not thread safe and can recycle connections via a
    :class:`.Driver` connection pool. As such, they should be considered
    lightweight and disposable.

    Typically, Session instances will be created and destroyed within a
    `with` context. For example::

        with driver.session() as session:
            result = session.run("MATCH (a:Person) RETURN a.name")
            # do something with the result...

    """

    # The current connection.
    _connection = None

    # The access mode for the current connection.
    _connection_access_mode = None

    # The current :class:`.Transaction` instance, if any.
    _transaction = None

    # The last result received.
    _last_result = None

    # The collection of bookmarks after which the next
    # :class:`.Transaction` should be carried out.
    _bookmarks = ()

    # Default maximum time to keep retrying failed transactions.
    _max_retry_time = DEFAULT_MAX_RETRY_TIME

    _closed = False

    def __init__(self, acquirer, access_mode, **parameters):
        self._acquirer = acquirer
        self._default_access_mode = access_mode
        for key, value in parameters.items():
            if key == "bookmark":
                self._bookmarks = [value] if value else []
            elif key == "bookmarks":
                self._bookmarks = value or []
            elif key == "max_retry_time":
                self._max_retry_time = value
            else:
                pass  # for compatibility

    def __del__(self):
        try:
            self.close()
        except (SessionError, ServiceUnavailable):
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _connect(self, access_mode=None):
        if access_mode is None:
            access_mode = self._default_access_mode
        if self._connection:
            if access_mode == self._connection_access_mode:
                return
            self._disconnect(sync=True)
        self._connection = self._acquirer(access_mode)
        self._connection_access_mode = access_mode

    def _disconnect(self, sync):
        if self._connection:
            if sync:
                self._connection.sync()
            if self._connection:
                self._connection.in_use = False
                self._connection = None
            self._connection_access_mode = None

    def close(self):
        """ Close the session. This will release any borrowed resources,
        such as connections, and will roll back any outstanding transactions.
        """
        try:
            if self.has_transaction():
                try:
                    self.rollback_transaction()
                except (CypherError, TransactionError, SessionError, ServiceUnavailable):
                    pass
        finally:
            self._closed = True
        self._disconnect(sync=True)

    def closed(self):
        """ Indicator for whether or not this session has been closed.

        :returns: :const:`True` if closed, :const:`False` otherwise.
        """
        return self._closed

    def run(self, statement, parameters=None, **kwparameters):
        """ Run a Cypher statement within an auto-commit transaction.

        The statement is sent and the result header received
        immediately but the :class:`.StatementResult` content is
        fetched lazily as consumed by the client application.

        If a statement is executed before a previous
        :class:`.StatementResult` in the same :class:`.Session` has
        been fully consumed, the first result will be fully fetched
        and buffered. Note therefore that the generally recommended
        pattern of usage is to fully consume one result before
        executing a subsequent statement. If two results need to be
        consumed in parallel, multiple :class:`.Session` objects
        can be used as an alternative to result buffering.

        For more usage details, see :meth:`.Transaction.run`.

        :param statement: template Cypher statement
        :param parameters: dictionary of parameters
        :param kwparameters: additional keyword parameters
        :returns: :class:`.StatementResult` object
        """
        if self.closed():
            raise SessionError("Session closed")
        if not statement:
            raise ValueError("Cannot run an empty statement")

        if not self.has_transaction():
            self._connect()

        result = self.__run__(fix_statement(statement), fix_parameters(parameters, **kwparameters))

        if not self.has_transaction():
            self._connection.send()
            self._connection.fetch()

        return result

    def send(self):
        """ Send all outstanding requests.
        """
        if self._connection:
            self._connection.send()

    def fetch(self):
        """ Attempt to fetch at least one more record.

        :returns: number of records fetched
        """
        if self._connection:
            detail_count, _ = self._connection.fetch()
            return detail_count
        return 0

    def sync(self):
        """ Carry out a full send and receive.

        :returns: number of records fetched
        """
        if self._connection:
            detail_count, _ = self._connection.sync()
            return detail_count
        return 0

    def detach(self, result):
        """ Detach a result from this session by fetching and buffering any
        remaining records.

        :param result:
        :returns: number of records fetched
        """
        count = 0

        self.send()
        fetch = self.fetch
        while result.attached():
            count += fetch()

        if self._last_result is result:
            self._last_result = None
            if not self.has_transaction():
                self._disconnect(sync=False)

        return count

    def last_bookmark(self):
        """ The bookmark returned by the last :class:`.Transaction`.
        """
        last = None
        for bookmark in self._bookmarks:
            if last is None:
                last = bookmark
            else:
                last = last_bookmark(last, bookmark)
        return last

    def has_transaction(self):
        return bool(self._transaction)

    def _create_transaction(self):
        self._transaction = Transaction(self, on_close=self._destroy_transaction)

    def _destroy_transaction(self):
        self._transaction = None

    def begin_transaction(self, bookmark=None):
        """ Create a new :class:`.Transaction` within this session.
        Calling this method with a bookmark is equivalent to

        :param bookmark: a bookmark to which the server should
                         synchronise before beginning the transaction
        :returns: new :class:`.Transaction` instance.
        :raise: :class:`.TransactionError` if a transaction is already open
        """
        if self.has_transaction():
            raise TransactionError("Explicit transaction already open")

        if bookmark is not None:
            global _warned_about_transaction_bookmarks
            if not _warned_about_transaction_bookmarks:
                from warnings import warn
                warn("Passing bookmarks at transaction level is deprecated", category=DeprecationWarning, stacklevel=2)
                _warned_about_transaction_bookmarks = True
            self._bookmarks = [bookmark]

        self._create_transaction()
        self._connect()
        self.__begin__()
        return self._transaction

    def commit_transaction(self):
        """ Commit the current transaction.

        :returns: the bookmark returned from the server, if any
        :raise: :class:`.TransactionError` if no transaction is currently open
        """
        if not self.has_transaction():
            raise TransactionError("No transaction to commit")
        self._transaction = None
        result = self.__commit__()
        result.consume()
        bookmark = self.__bookmark__(result)
        self._bookmarks = [bookmark]
        return bookmark

    def rollback_transaction(self):
        """ Rollback the current transaction.

        :raise: :class:`.TransactionError` if no transaction is currently open
        """
        if not self.has_transaction():
            raise TransactionError("No transaction to rollback")
        self._transaction = None
        self.__rollback__().consume()

    def _run_transaction(self, access_mode, unit_of_work, *args, **kwargs):
        if not callable(unit_of_work):
            raise TypeError("Unit of work is not callable")
        retry_delay = retry_delay_generator(INITIAL_RETRY_DELAY,
                                            RETRY_DELAY_MULTIPLIER,
                                            RETRY_DELAY_JITTER_FACTOR)
        last_error = None
        t0 = time()
        while True:
            try:
                self._connect(access_mode)
                self._create_transaction()
                self.__begin__()
                with self._transaction as tx:
                    return unit_of_work(tx, *args, **kwargs)
            except (ServiceUnavailable, SessionExpired) as error:
                last_error = error
            except TransientError as error:
                if is_retriable_transient_error(error):
                    last_error = error
                else:
                    raise error
            t1 = time()
            if t1 - t0 > self._max_retry_time:
                break
            sleep(next(retry_delay))
        raise last_error

    def read_transaction(self, unit_of_work, *args, **kwargs):
        return self._run_transaction(READ_ACCESS, unit_of_work, *args, **kwargs)

    def write_transaction(self, unit_of_work, *args, **kwargs):
        return self._run_transaction(WRITE_ACCESS, unit_of_work, *args, **kwargs)

    def __run__(self, statement, parameters):
        pass

    def __begin__(self):
        pass

    def __commit__(self):
        pass

    def __rollback__(self):
        pass

    def __bookmark__(self, result):
        pass


def is_retriable_transient_error(error):
    """
    :type error: TransientError
    """
    return not (error.code in ("Neo.TransientError.Transaction.Terminated",
                               "Neo.TransientError.Transaction.LockClientStopped"))


class Transaction(object):
    """ Container for multiple Cypher queries to be executed within
    a single context. Transactions can be used within a :py:const:`with`
    block where the value of :attr:`.success` will determine whether
    the transaction is committed or rolled back on :meth:`.Transaction.close`::

        with session.begin_transaction() as tx:
            pass

    """

    #: When set, the transaction will be committed on close, otherwise it
    #: will be rolled back. This attribute can be set in user code
    #: multiple times before a transaction completes with only the final
    #: value taking effect.
    success = None

    _closed = False

    def __init__(self, session, on_close):
        self.session = session
        self.on_close = on_close

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.success is None:
            self.success = not bool(exc_type)
        self.close()

    def run(self, statement, parameters=None, **kwparameters):
        """ Run a Cypher statement within the context of this transaction.

        The statement is sent to the server lazily, when its result is
        consumed. To force the statement to be sent to the server, use
        the :meth:`.Transaction.sync` method.

        Cypher is typically expressed as a statement template plus a
        set of named parameters. In Python, parameters may be expressed
        through a dictionary of parameters, through individual parameter
        arguments, or as a mixture of both. For example, the `run`
        statements below are all equivalent::

            >>> statement = "CREATE (a:Person {name:{name}, age:{age}})"
            >>> tx.run(statement, {"name": "Alice", "age": 33})
            >>> tx.run(statement, {"name": "Alice"}, age=33)
            >>> tx.run(statement, name="Alice", age=33)

        Parameter values can be of any type supported by the Neo4j type
        system. In Python, this includes :class:`bool`, :class:`int`,
        :class:`str`, :class:`list` and :class:`dict`. Note however that
        :class:`list` properties must be homogenous.

        :param statement: template Cypher statement
        :param parameters: dictionary of parameters
        :param kwparameters: additional keyword parameters
        :returns: :class:`.StatementResult` object
        """
        if self.closed():
            raise TransactionError("Transaction closed")
        return self.session.run(statement, parameters, **kwparameters)

    def sync(self):
        """ Force any queued statements to be sent to the server and
        all related results to be fetched and buffered.

        :raise TransactionError: if the transaction is closed
        """
        if self.closed():
            raise TransactionError("Transaction closed")
        self.session.sync()

    def commit(self):
        """ Mark this transaction as successful and close in order to
        trigger a COMMIT.

        :raise TransactionError: if already closed
        """
        if self.closed():
            raise TransactionError("Transaction closed")
        self.success = True
        self.close()

    def rollback(self):
        """ Mark this transaction as unsuccessful and close in order to
        trigger a ROLLBACK.

        :raise TransactionError: if already closed
        """
        if self.closed():
            raise TransactionError("Transaction closed")
        self.success = False
        self.close()

    def close(self):
        """ Close this transaction, triggering either a COMMIT or a ROLLBACK.
        """
        if not self.closed():
            try:
                self.sync()
            except CypherError:
                self.success = False
                raise
            finally:
                if self.success:
                    self.session.commit_transaction()
                else:
                    self.session.rollback_transaction()
                self._closed = True
                self.on_close()

    def closed(self):
        """ Indicator to show whether the transaction has been closed.
        :returns: :const:`True` if closed, :const:`False` otherwise.
        """
        return self._closed


class StatementResult(object):
    """ A handler for the result of Cypher statement execution. Instances
    of this class are typically constructed and returned by
    :meth:`.Session.run` and :meth:`.Transaction.run`.
    """

    #: The statement text that was executed to produce this result.
    statement = None

    #: Dictionary of parameters passed with the statement.
    parameters = None

    value_system = None

    zipper = zip

    _session = None

    _keys = None

    _records = None

    _summary = None

    def __init__(self, session):
        self._session = session
        self._records = deque()

    def __iter__(self):
        return self.records()

    def attached(self):
        """ Indicator for whether or not this result is still attached to
        a :class:`.Session`.

        :returns: :const:`True` if still attached, :const:`False` otherwise
        """
        return self._session and not self._session.closed()

    def detach(self):
        """ Detach this result from its parent session by fetching the
        remainder of this result from the network into the buffer.

        :returns: number of records fetched
        """
        if self.attached():
            return self._session.detach(self)
        else:
            return 0

    def keys(self):
        """ The keys for the records in this result.

        :returns: tuple of key names
        """
        if self._keys is not None:
            return self._keys
        if self.attached():
            self._session.send()
        while self.attached() and self._keys is None:
            self._session.fetch()
        return self._keys

    def records(self):
        """ Generator for records obtained from this result.

        :yields: iterable of :class:`.Record` objects
        """
        hydrate = self.value_system.hydrate
        zipper = self.zipper
        keys = self.keys()
        records = self._records
        pop_first_record = records.popleft
        while records:
            values = pop_first_record()
            yield zipper(keys, hydrate(values))
        attached = self.attached
        if attached():
            self._session.send()
        while attached():
            self._session.fetch()
            while records:
                values = pop_first_record()
                yield zipper(keys, hydrate(values))

    def summary(self):
        """ Obtain the summary of this result, buffering any remaining records.

        :returns: The :class:`.ResultSummary` for this result
        """
        self.detach()
        return self._summary

    def consume(self):
        """ Consume the remainder of this result and return the summary.

        .. NOTE:: It is generally recommended to use :meth:`.summary`
                  instead of this method.

        :returns: The :class:`.ResultSummary` for this result
        """
        if self.attached():
            list(self)
        return self.summary()

    def single(self):
        """ Obtain the next and only remaining record from this result.

        A warning is generated if more than one record is available but
        the first of these is still returned.

        :returns: the next :class:`.Record` or :const:`None` if none remain
        :warns: if more than one record is available
        """
        records = list(self)
        size = len(records)
        if size == 0:
            return None
        if size != 1:
            warn("Expected a result with a single record, but this result contains %d" % size)
        return records[0]

    def peek(self):
        """ Obtain the next record from this result without consuming it.
        This leaves the record in the buffer for further processing.

        :returns: the next :class:`.Record` or :const:`None` if none remain
        """
        hydrate = self.value_system.hydrate
        zipper = self.zipper
        keys = self.keys()
        records = self._records
        if records:
            values = records[0]
            return zipper(keys, hydrate(values))
        if not self.attached():
            return None
        if self.attached():
            self._session.send()
        while self.attached() and not records:
            self._session.fetch()
            if records:
                values = records[0]
                return zipper(keys, hydrate(values))
        return None

    def data(self):
        """ Return the remainder of the result as a list of dictionaries.
        """
        return [dict(record) for record in self]


def fix_statement(statement):
    if isinstance(statement, bytes):
        statement = statement.decode("UTF-8")
    return statement


def fix_parameters(parameters=None, **kwparameters):
    params_in = parameters or {}
    params_in.update(kwparameters)
    params_out = {}
    for key, value in params_in.items():
        if isinstance(key, bytes):
            key = key.decode("UTF-8")
        params_out[key] = value
    return params_out
