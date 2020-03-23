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
from logging import getLogger
from random import random
from time import perf_counter, sleep
from warnings import warn

from neo4j.conf import SessionConfig
from neo4j.api import READ_ACCESS, WRITE_ACCESS
from neo4j.data import DataHydrator, DataDehydrator
from neo4j.exceptions import (
    Neo4jError,
    ServiceUnavailable,
    TransientError,
    SessionExpired,
    TransactionError,
)
from neo4j._exceptions import BoltIncompleteCommitError
from neo4j.work import Workspace
from neo4j.work.summary import ResultSummary


log = getLogger("neo4j")


class Session(Workspace):
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

    :param pool: connection pool instance
    :param config: session config instance

    """

    # The current connection.
    _connection = None

    # The current :class:`.Transaction` instance, if any.
    _transaction = None

    # The last result received.
    _last_result = None

    # The set of bookmarks after which the next
    # :class:`.Transaction` should be carried out.
    _bookmarks_in = None

    # The bookmark returned from the last commit.
    _bookmark_out = None

    def __init__(self, pool, session_config):
        super().__init__(pool, session_config)
        assert isinstance(session_config, SessionConfig)
        self._bookmarks_in = tuple(session_config.bookmarks)

    def __del__(self):
        try:
            self.close()
        except OSError:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _connect(self, access_mode):
        if access_mode is None:
            access_mode = self._config.default_access_mode
        if self._connection:
            log.warning("FIXME: should always disconnect before connect")
            self._connection.send_all()
            self._connection.fetch_all()
            self._disconnect()
        self._connection = self._pool.acquire(access_mode, timeout=self._config.connection_acquisition_timeout)

    def _disconnect(self):
        if self._connection:
            self._connection.in_use = False
            self._connection = None

    def close(self):
        """ Close the session. This will release any borrowed resources,
        such as connections, and will roll back any outstanding transactions.
        """
        if self._connection:
            if self._transaction:
                self._connection.rollback()
                self._transaction = None
            try:
                self._connection.send_all()
                self._connection.fetch_all()
            except (Neo4jError, TransactionError, ServiceUnavailable, SessionExpired):
                pass
            finally:
                self._disconnect()

    def run(self, query, parameters=None, **kwparameters):
        """ Run a Cypher query within an auto-commit transaction.

        The query is sent and the result header received
        immediately but the :class:`neo4j.Result` content is
        fetched lazily as consumed by the client application.

        If a query is executed before a previous
        :class:`neo4j.Result` in the same :class:`.Session` has
        been fully consumed, the first result will be fully fetched
        and buffered. Note therefore that the generally recommended
        pattern of usage is to fully consume one result before
        executing a subsequent query. If two results need to be
        consumed in parallel, multiple :class:`.Session` objects
        can be used as an alternative to result buffering.

        For more usage details, see :meth:`.Transaction.run`.

        :param query: Cypher query
        :param parameters: dictionary of parameters
        :param kwparameters: additional keyword parameters
        :returns: :class:`neo4j.Result` object
        """
        if not query:
            raise ValueError("Cannot run an empty query")
        if not isinstance(query, (str, Query)):
            raise TypeError("query must be a string or a Query instance")

        if not self._connection:
            self._connect(self._config.default_access_mode)
        cx = self._connection
        protocol_version = cx.PROTOCOL_VERSION
        server = cx.server

        has_transaction = self.has_transaction()

        query_text = str(query)
        query_metadata = getattr(query, "metadata", None)
        query_timeout = getattr(query, "timeout", None)
        parameters = DataDehydrator.fix_parameters(dict(parameters or {}, **kwparameters))

        def fail(_):
            self._close_transaction()

        hydrant = DataHydrator()
        result_metadata = {
            "query": query_text,
            "parameters": parameters,
            "server": server,
            "protocol_version": protocol_version,
        }
        run_metadata = {
            "metadata": query_metadata,
            "timeout": query_timeout,
            "on_success": result_metadata.update,
            "on_failure": fail,
        }

        def done(summary_metadata):
            result_metadata.update(summary_metadata)
            bookmark = result_metadata.get("bookmark")
            if bookmark:
                self._bookmarks_in = tuple([bookmark])
                self._bookmark_out = bookmark

        self._last_result = result = Result(self, hydrant, result_metadata)

        if has_transaction:
            if query_metadata:
                raise ValueError("Metadata can only be attached at transaction level")
            if query_timeout:
                raise ValueError("Timeouts only apply at transaction level")
            # TODO: fail if explicit database name has been set
        else:
            run_metadata["bookmarks"] = self._bookmarks_in

        # TODO: capture ValueError and surface as SessionError/TransactionError if
        # TODO: explicit database selection has been made
        cx.run(query_text, parameters, **run_metadata)
        cx.pull(
            on_records=lambda records: result._records.extend(
                hydrant.hydrate_records(result.keys(), records)),
            on_success=done,
            on_failure=fail,
            on_summary=lambda: result.detach(sync=False),
        )

        if not has_transaction:
            self._connection.send_all()
            self._connection.fetch_message()

        return result

    def send(self):
        """ Send all outstanding requests.
        """
        if self._connection:
            self._connection.send_all()

    def fetch(self):
        """ Attempt to fetch at least one more record.

        :returns: number of records fetched
        """
        if self._connection:
            detail_count, _ = self._connection.fetch_message()
            return detail_count

        return 0

    def sync(self):
        """ Carry out a full send and receive.

        :returns: number of records fetched
        """
        if self._connection:
            self._connection.send_all()
            detail_count, _ = self._connection.fetch_all()
            return detail_count

        return 0

    def detach(self, result, sync=True):
        """ Detach a result from this session by fetching and buffering any
        remaining records.

        :param result:
        :param sync:
        :returns: number of records fetched
        """
        count = 0

        if sync and result.attached():
            self.send()
            fetch = self.fetch
            while result.attached():
                count += fetch()

        if self._last_result is result:
            self._last_result = None
            if not self.has_transaction():
                self._disconnect()

        result._session = None
        return count

    def next_bookmarks(self):
        """ The set of bookmarks to be passed into the next
        :class:`.Transaction`.
        """
        return self._bookmarks_in

    def last_bookmark(self):
        """ The bookmark returned by the last :class:`.Transaction`.
        """
        return self._bookmark_out

    def has_transaction(self):
        return bool(self._transaction)

    def _close_transaction(self):
        self._transaction = None

    def begin_transaction(self, bookmark=None, metadata=None, timeout=None):
        """ Create a new :class:`.Transaction` within this session.
        Calling this method with a bookmark is equivalent to

        :param bookmark: a bookmark to which the server should
                         synchronise before beginning the transaction
        :param metadata:
        :param timeout:
        :returns: new :class:`.Transaction` instance.
        :raise: :class:`.TransactionError` if a transaction is already open
        """
        if self.has_transaction():
            raise TransactionError("Explicit transaction already open")

        self._open_transaction(metadata=metadata, timeout=timeout)
        return self._transaction

    def _open_transaction(self, access_mode=None, metadata=None, timeout=None):
        self._transaction = Transaction(self, on_close=self._close_transaction)
        self._connect(access_mode)
        # TODO: capture ValueError and surface as SessionError/TransactionError if
        # TODO: explicit database selection has been made
        self._connection.begin(bookmarks=self._bookmarks_in, metadata=metadata, timeout=timeout)

    def commit_transaction(self):
        """ Commit the current transaction.

        :returns: the bookmark returned from the server, if any
        :raise: :class:`.TransactionError` if no transaction is currently open
        """
        if not self._transaction:
            raise TransactionError("No transaction to commit")
        metadata = {}
        try:
            self._connection.commit(on_success=metadata.update)
            self._connection.send_all()
            self._connection.fetch_all()
        except BoltIncompleteCommitError:
            raise ServiceUnavailable("Connection closed during commit")
        finally:
            self._disconnect()
            self._transaction = None
        bookmark = metadata.get("bookmark")
        self._bookmarks_in = tuple([bookmark])
        self._bookmark_out = bookmark
        return bookmark

    def rollback_transaction(self):
        """ Rollback the current transaction.

        :raise: :class:`.TransactionError` if no transaction is currently open
        """
        if not self._transaction:
            raise TransactionError("No transaction to rollback")
        cx = self._connection
        if cx:
            metadata = {}
            try:
                cx.rollback(on_success=metadata.update)
                cx.send_all()
                cx.fetch_all()
            finally:
                self._disconnect()
                self._transaction = None

    def _run_transaction(self, access_mode, unit_of_work, *args, **kwargs):

        if not callable(unit_of_work):
            raise TypeError("Unit of work is not callable")

        metadata = getattr(unit_of_work, "metadata", None)
        timeout = getattr(unit_of_work, "timeout", None)

        retry_delay = retry_delay_generator(self._config.initial_retry_delay,
                                            self._config.retry_delay_multiplier,
                                            self._config.retry_delay_jitter_factor)
        errors = []
        t0 = perf_counter()
        while True:
            try:
                self._open_transaction(access_mode, metadata, timeout)
                tx = self._transaction
                try:
                    result = unit_of_work(tx, *args, **kwargs)
                except Exception:
                    tx._success = False
                    raise
                else:
                    if tx._success is None:
                        tx._success = True
                finally:
                    tx._close()
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
            if t1 - t0 > self._config.max_retry_time:
                break
            delay = next(retry_delay)
            log.warning("Transaction failed and will be retried in {}s "
                        "({})".format(delay, "; ".join(errors[-1].args)))
            sleep(delay)
        if errors:
            raise errors[-1]
        else:
            raise ServiceUnavailable("Transaction failed")

    def read_transaction(self, unit_of_work, *args, **kwargs):
        """
        Execute a unit of work in a managed read transaction.
        This transaction will automatically be committed unless an exception is thrown during query execution or by the user code.

        Managed transactions should not generally be explicitly committed (via tx.commit()).

        Example:

        def do_cypher(tx, cypher):
            result = tx.run(cypher)
            # consume result
            return 1

        session.read_transaction(do_cypher, "RETURN 1")

        :param unit_of_work: A function that takes a transaction as an argument and do work with the transaction. unit_of_work(tx, *args, **kwargs)
        :param args: arguments for the unit_of_work function
        :param kwargs: key word arguments for the unit_of_work function
        :return: a result as returned by the given unit of work
        """
        return self._run_transaction(READ_ACCESS, unit_of_work, *args, **kwargs)

    def write_transaction(self, unit_of_work, *args, **kwargs):
        """
        Execute a unit of work in a managed write transaction.
        This transaction will automatically be committed unless an exception is thrown during query execution or by the user code.

        Managed transactions should not generally be explicitly committed (via tx.commit()).

        Example:

        def do_cypher(tx, cypher):
            result = tx.run(cypher)
            # consume result
            return 1

        session.write_transaction(do_cypher, "RETURN 1")

        :param unit_of_work: A function that takes a transaction as an argument and do work with the transaction. unit_of_work(tx, *args, **kwargs)
        :param args: key word arguments for the unit_of_work function
        :param kwargs: key word arguments for the unit_of_work function
        :return: a result as returned by the given unit of work
        """
        return self._run_transaction(WRITE_ACCESS, unit_of_work, *args, **kwargs)


class Transaction:
    """ Container for multiple Cypher queries to be executed within
    a single context. Transactions can be used within a :py:const:`with`
    block where the transaction is committed or rolled back on based on
    whether or not an exception is raised::

        with session.begin_transaction() as tx:
            pass

    """

    #: When set, the transaction will be committed on close, otherwise it
    #: will be rolled back. This attribute can be set in user code
    #: multiple times before a transaction completes, with only the final
    #: value taking effect.
    #
    # This became internal with Neo4j 4.0, when the server-side
    # transaction semantics changed.
    #
    _success = None

    _closed = False

    def __init__(self, session, on_close):
        self.session = session
        self.on_close = on_close

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._closed:
            return
        if self._success is None:
            self._success = not bool(exc_type)
        self._close()

    def run(self, query, parameters=None, **kwparameters):
        """ Run a Cypher query within the context of this transaction.

        The query is sent to the server lazily, when its result is
        consumed. To force the query to be sent to the server, use
        the :meth:`.Transaction.sync` method.

        Cypher is typically expressed as a query template plus a
        set of named parameters. In Python, parameters may be expressed
        through a dictionary of parameters, through individual parameter
        arguments, or as a mixture of both. For example, the `run`
        queries below are all equivalent::

            >>> query = "CREATE (a:Person {name:{name}, age:{age}})"
            >>> tx.run(query, {"name": "Alice", "age": 33})
            >>> tx.run(query, {"name": "Alice"}, age=33)
            >>> tx.run(query, name="Alice", age=33)

        Parameter values can be of any type supported by the Neo4j type
        system. In Python, this includes :class:`bool`, :class:`int`,
        :class:`str`, :class:`list` and :class:`dict`. Note however that
        :class:`list` properties must be homogenous.

        :param query: template Cypher query
        :param parameters: dictionary of parameters
        :param kwparameters: additional keyword parameters
        :returns: :class:`neo4j.Result` object
        :raise TransactionError: if the transaction is closed
        """
        self._assert_open()
        return self.session.run(query, parameters, **kwparameters)

    def sync(self):
        """ Force any queued queries to be sent to the server and
        all related results to be fetched and buffered.

        :raise TransactionError: if the transaction is closed
        """
        self._assert_open()
        self.session.sync()

    def commit(self):
        """ Mark this transaction as successful and close in order to
        trigger a COMMIT. This is functionally equivalent to::

        :raise TransactionError: if already closed
        """
        self._success = True
        self._close()

    def rollback(self):
        """ Mark this transaction as unsuccessful and close in order to
        trigger a ROLLBACK. This is functionally equivalent to::

        :raise TransactionError: if already closed
        """
        self._success = False
        self._close()

    def _close(self):
        """ Close this transaction, triggering either a COMMIT or a ROLLBACK.

        :raise TransactionError: if already closed
        """
        self._assert_open()
        try:
            self.sync()
        except Neo4jError:
            self._success = False
            raise
        finally:
            if self.session.has_transaction():
                if self._success:
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

    def _assert_open(self):
        if self._closed:
            raise TransactionError("Transaction closed")


class Query:
    """ Create a new query.

    :param text: The query text.
    :type str:
    :param metadata: metadata attached to the query.
    :type dict:
    :param timeout: seconds.
    :type int:
    """
    def __init__(self, text, metadata=None, timeout=None):
        self.text = text

        self.metadata = metadata
        self.timeout = timeout

    def __str__(self):
        return str(self.text)


class Result:
    """ A handler for the result of Cypher query execution. Instances
    of this class are typically constructed and returned by
    :meth:`.Session.run` and :meth:`.Transaction.run`.
    """

    def __init__(self, session, hydrant, metadata):
        self._session = session
        self._hydrant = hydrant
        self._metadata = metadata
        self._records = deque()
        self._summary = None

    def __iter__(self):
        return self.records()

    @property
    def session(self):
        """ The :class:`.Session` to which this result is attached, if any.
        """
        return self._session

    def attached(self):
        """ Indicator for whether or not this result is still attached to
        an open :class:`.Session`.
        """
        return self._session

    def detach(self, sync=True):
        """ Detach this result from its parent session by fetching the
        remainder of this result from the network into the buffer.

        :returns: number of records fetched
        """
        if self.attached():
            return self._session.detach(self, sync=sync)
        else:
            return 0

    def keys(self):
        """ The keys for the records in this result.

        :returns: tuple of key names
        """
        try:
            return self._metadata["fields"]
        except KeyError:
            if self.attached():
                self._session.send()
            while self.attached() and "fields" not in self._metadata:
                self._session.fetch()
            return self._metadata.get("fields")

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

        :returns: The :class:`neo4j.ResultSummary` for this result
        """
        self.detach()
        if self._summary is None:
            self._summary = ResultSummary(**self._metadata)
        return self._summary

    def consume(self):
        """ Consume the remainder of this result and return the summary.

        :returns: The :class:`neo4j.ResultSummary` for this result
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
        records = self._records
        if records:
            return records[0]
        if not self.attached():
            return None
        if self.attached():
            self._session.send()
        while self.attached() and not records:
            self._session.fetch()
            if records:
                return records[0]
        return None

    def graph(self):
        """ Return a Graph instance containing all the graph objects
        in the result. After calling this method, the result becomes
        detached, buffering all remaining records.

        :returns: result graph
        """
        self.detach()
        return self._hydrant.graph

    def value(self, item=0, default=None):
        """ Return the remainder of the result as a list of values.

        :param item: field to return for each remaining record
        :param default: default value, used if the index of key is unavailable
        :returns: list of individual values
        """
        return [record.value(item, default) for record in self.records()]

    def values(self, *items):
        """ Return the remainder of the result as a list of tuples.

        :param items: fields to return for each remaining record
        :returns: list of value tuples
        """
        return [record.values(*items) for record in self.records()]

    def data(self, *items):
        """ Return the remainder of the result as a list of dictionaries.

        :param items: fields to return for each remaining record
        :returns: list of dictionaries
        """
        return [record.data(*items) for record in self.records()]


def unit_of_work(metadata=None, timeout=None):
    """ This function is a decorator for transaction functions that allows
    extra control over how the transaction is carried out.

    For example, a timeout (in seconds) may be applied::

        @unit_of_work(timeout=25.0)
        def count_people(tx):
            return tx.run("MATCH (a:Person) RETURN count(a)").single().value()

    :param metadata: metadata attached to the query.
    :type dict:
    :param timeout: seconds.
    :type int:
    """

    def wrapper(f):

        def wrapped(*args, **kwargs):
            return f(*args, **kwargs)

        wrapped.metadata = metadata
        wrapped.timeout = timeout
        return wrapped

    return wrapper


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
