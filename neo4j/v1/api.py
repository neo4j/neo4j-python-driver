#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2018 "Neo4j,"
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
from random import random
from time import sleep
from warnings import warn

from neo4j.bolt.connection import RUN, PULL_ALL
from neo4j.bolt.response import Response
from neo4j.exceptions import ServiceUnavailable
from neo4j.compat import urlparse, ustr
from neo4j.exceptions import CypherError, TransientError
from neo4j.config import default_config
from neo4j.compat import perf_counter

from .exceptions import DriverError, SessionError, SessionExpired, TransactionError

_warned_about_transaction_bookmarks = False

READ_ACCESS = "READ"
WRITE_ACCESS = "WRITE"

INITIAL_RETRY_DELAY = 1.0
RETRY_DELAY_MULTIPLIER = 2.0
RETRY_DELAY_JITTER_FACTOR = 0.2

BOOKMARK_PREFIX = "neo4j:bookmark:v1:tx"


def last_bookmark(b0, b1):
    """ Return the latest of two bookmarks.
    """
    return b0 if _bookmark_value(b0) > _bookmark_value(b1) else b1


def _bookmark_value(b):
    """Return the int value of the given bookmark.
    """
    if b is None or not b.startswith(BOOKMARK_PREFIX):
        raise ValueError("Invalid bookmark: {}".format(b))

    value_string = b[len(BOOKMARK_PREFIX):]
    try:
        return int(value_string)
    except ValueError:
        raise ValueError("Invalid bookmark: {}".format(b))


def retry_delay_generator(initial_delay, multiplier, jitter_factor):
    delay = initial_delay
    while True:
        jitter = jitter_factor * delay
        yield delay - jitter + (2 * jitter * random())
        delay *= multiplier


def is_retriable_transient_error(error):
    """
    :type error: TransientError
    """
    return not (error.code in ("Neo.TransientError.Transaction.Terminated",
                               "Neo.TransientError.Transaction.LockClientStopped"))


class GraphDatabase(object):
    """ Accessor for :class:`.Driver` construction.
    """

    @classmethod
    def driver(cls, uri, **config):
        """ Create a :class:`.Driver` object. Calling this method provides
        identical functionality to constructing a :class:`.Driver` or
        :class:`.Driver` subclass instance directly.
        """
        return Driver(uri, **config)


class Driver(object):
    """ Base class for all types of :class:`.Driver`, instances of which are
    used as the primary access point to Neo4j.

    :param uri: URI for a graph database service
    :param config: configuration and authentication details (valid keys are listed below)
    """

    #: Overridden by subclasses to specify the URI scheme owned by that
    #: class.
    uri_scheme = None

    #: Connection pool
    _pool = None

    #: Indicator of driver closure.
    _closed = False

    @classmethod
    def _check_uri(cls, uri):
        """ Check whether a URI is compatible with a :class:`.Driver`
        subclass. When called from a subclass, execution simply passes
        through if the URI scheme is valid for that class. If invalid,
        a `ValueError` is raised.

        :param uri: URI to check for compatibility
        :raise: `ValueError` if URI scheme is incompatible
        """
        parsed = urlparse(uri)
        if parsed.scheme != cls.uri_scheme:
            raise ValueError("%s objects require the %r URI scheme" % (cls.__name__, cls.uri_scheme))

    def __new__(cls, uri, **config):
        parsed = urlparse(uri)
        for subclass in Driver.__subclasses__():
            if parsed.scheme == subclass.uri_scheme:
                return subclass(uri, **config)
        raise ValueError("URI scheme %r not supported" % parsed.scheme)

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def session(self, access_mode=None, **parameters):
        """ Create a new :class:`.Session` object based on this
        :class:`.Driver`.

        :param access_mode: default access mode (read or write) for
                            transactions in this session
        :param parameters: custom session parameters (see
                           :class:`.Session` for details)
        :returns: new :class:`.Session` object
        """
        if self.closed():
            raise DriverError("Driver closed")

    def close(self):
        """ Shut down, closing any open connections that were spawned by
        this :class:`.Driver`.
        """
        if not self._closed:
            self._closed = True
            if self._pool is not None:
                self._pool.close()
                self._pool = None

    def closed(self):
        return self._closed


class Session(object):
    """ A :class:`.Session` is a logical context for transactional units
    of work. Connections are drawn from the :class:`.Driver` connection
    pool as required.

    Session creation is a lightweight operation and sessions are not thread
    safe. Therefore a session should generally be short-lived, and not
    span multiple threads.

    In general, sessions will be created and destroyed within a `with`
    context. For example::

        with driver.session() as session:
            result = session.run("MATCH (a:Person) RETURN a.name")
            # do something with the result...

    :param acquirer: callback function for acquiring new connections
                     with a given access mode
    :param access_mode: default access mode (read or write) for
                        transactions in this session
    :param parameters: custom session parameters, including:

        `bookmark`
            A single bookmark after which this session should begin.
            (Deprecated, use `bookmarks` instead)

        `bookmarks`
            A collection of bookmarks after which this session should begin.

        `max_retry_time`
            The maximum time after which to stop attempting retries of failed
            transactions.

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
    _max_retry_time = default_config["max_retry_time"]

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
                try:
                    self._connection.sync()
                except (SessionError, ServiceUnavailable):
                    pass
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

        result = self.__run__(statement, dict(parameters or {}, **kwparameters))

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
        rollback_result = self.__rollback__()
        try:
            rollback_result.consume()
        except ServiceUnavailable:
            pass

    def _run_transaction(self, access_mode, unit_of_work, *args, **kwargs):
        if not callable(unit_of_work):
            raise TypeError("Unit of work is not callable")
        retry_delay = retry_delay_generator(INITIAL_RETRY_DELAY,
                                            RETRY_DELAY_MULTIPLIER,
                                            RETRY_DELAY_JITTER_FACTOR)
        errors = []
        t0 = perf_counter()
        while True:
            try:
                self._create_transaction()
                self._connect(access_mode)
                self.__begin__()
                tx = self._transaction
                try:
                    result = unit_of_work(tx, *args, **kwargs)
                except:
                    if tx.success is None:
                        tx.success = False
                    raise
                else:
                    if tx.success is None:
                        tx.success = True
                finally:
                    tx.close()
            except (ServiceUnavailable, SessionExpired) as error:
                errors.append(error)
            except TransientError as error:
                if is_retriable_transient_error(error):
                    errors.append(error)
                else:
                    raise
            else:
                return result
            t1 = perf_counter()
            if t1 - t0 > self._max_retry_time:
                break
            sleep(next(retry_delay))
        if errors:
            raise errors[-1]
        else:
            raise ServiceUnavailable("Transaction failed")

    def read_transaction(self, unit_of_work, *args, **kwargs):
        return self._run_transaction(READ_ACCESS, unit_of_work, *args, **kwargs)

    def write_transaction(self, unit_of_work, *args, **kwargs):
        return self._run_transaction(WRITE_ACCESS, unit_of_work, *args, **kwargs)

    def _run(self, statement, parameters):
        from neo4j.v1.result import BoltStatementResult
        from neo4j.v1.types import fix_parameters
        if self.closed():
            raise SessionError("Session closed")

        run_response = Response(self._connection)
        pull_all_response = Response(self._connection)
        self._last_result = result = BoltStatementResult(self, run_response, pull_all_response)
        result.statement = ustr(statement)
        result.parameters = fix_parameters(parameters, self._connection.protocol_version,
                                           supports_bytes=self._connection.server.supports_bytes())

        self._connection.append(RUN, (result.statement, result.parameters), response=run_response)
        self._connection.append(PULL_ALL, response=pull_all_response)

        return result

    def __run__(self, statement, parameters):
        return self._run(statement, parameters)

    def __begin__(self):
        if self._bookmarks:
            parameters = {"bookmark": self.last_bookmark(), "bookmarks": self._bookmarks}
        else:
            parameters = {}
        return self.__run__(u"BEGIN", parameters)

    def __commit__(self):
        return self.__run__(u"COMMIT", {})

    def __rollback__(self):
        return self.__run__(u"ROLLBACK", {})

    def __bookmark__(self, result):
        summary = result.summary()
        return summary.metadata.get("bookmark")


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

    zipper = zip

    def __init__(self, session, hydrant):
        self._session = session
        self._hydrant = hydrant
        self._keys = None
        self._records = deque()
        self._summary = None

    def __iter__(self):
        return self.records()

    @property
    def session(self):
        return self._session

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
        records = self._records
        next_record = records.popleft
        while records:
            yield next_record()
        attached = self.attached
        if attached():
            self._session.send()
        while attached():
            self._session.fetch()
            while records:
                yield next_record()

    def summary(self):
        """ Obtain the summary of this result, buffering any remaining records.

        :returns: The :class:`.ResultSummary` for this result
        """
        self.detach()
        return self._summary

    def consume(self):
        """ Consume the remainder of this result and return the summary.

        :returns: The :class:`.ResultSummary` for this result
        """
        if self.attached():
            for _ in self:
                pass
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
        hydrate = self._hydrant.hydrate
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

    def graph(self):
        """ Return a Graph instance containing all the graph objects
        in the result. After calling this method, the result becomes
        detached, buffering all remaining records.

        :returns: result graph
        """
        self.detach()
        return self._hydrant.graph
