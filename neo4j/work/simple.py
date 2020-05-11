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
from neo4j.io._bolt3 import Bolt3

log = getLogger("neo4j")


class Session(Workspace):
    """A :class:`.Session` is a logical context for transactional units
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

    # The current auto-transaction result, if any.
    _autoResult = None

    # The last result received.
    #_last_result = None

    # The set of bookmarks after which the next
    # :class:`.Transaction` should be carried out.
    _bookmarks_in = None

    # The bookmark returned from the last commit.
    _bookmark_out = None

    # The state this session is in.
    _state = None

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

    def __exit__(self, exception_type, exception_value, traceback):
        # TODO: Fix better state logic
        if exception_type is None:
            self._state = "graceful_close_state"
        else:
            self._state = "error_state"
        self.close()

    def _connect(self, access_mode, database):
        if access_mode is None:
            access_mode = self._config.default_access_mode
        if self._connection:
            log.warning("FIXME: should always disconnect before connect")
            self._connection.send_all()
            self._connection.fetch_all()
            self._disconnect()
        self._connection = self._pool.acquire(access_mode=access_mode, timeout=self._config.connection_acquisition_timeout, database=database)

    def _disconnect(self):
        if self._connection:
            self._connection.in_use = False
            self._connection = None

    def close(self):
        """Close the session. This will release any borrowed resources, such as connections, and will roll back any outstanding transactions.
        """
        if self._connection:
            if self._autoResult:
                if self._state != "error_state":
                    try:
                        self._autoResult.consume()
                    except Exception as e:
                        # TODO: Investigate potential non graceful close states
                        self._autoResult = None
                        self._state = "error_state"

            if self._transaction:
                self._connection.rollback()
                self._transaction = None

            try:
                self._connection.send_all()
                self._connection.fetch_all()
                # TODO: Investigate potential non graceful close states
            except Neo4jError:
                pass
            except TransactionError:
                pass
            except ServiceUnavailable:
                pass
            except SessionExpired:
                pass
            finally:
                self._disconnect()

            self._state = None

    #def discard_all(self):
    #    """Internal API.
    #    Add a BOLT Message, to discard the remaining records, to the outgoing messages.
    #    Use session.send() to send the buffered outgoing messages.
    #    """
    #    def fail(_):
    #        self._close_transaction()

    #    def on_success_done(summary_metadata):
    #        self._last_result._metadata.update(summary_metadata)
    #        bookmark = self._last_result._metadata.get("bookmark")
    #        if bookmark:
    #            self._bookmarks_in = tuple([bookmark])
    #            self._bookmark_out = bookmark

    #    # BOLT DISCARD
    #    self._connection.discard(
    #        n=-1,
    #        # on_records=handlers.get("on_records"),
    #        on_success=on_success_done,
    #        on_failure=fail,
    #        on_summary=lambda: self._last_result.detach(sync=False),
    #    )

#     def pull(self):
#         """Internal API.
#         Add a BOLT Message, to pull more records, to the outgoing messages.
#         Use session.send() to send the buffered outgoing messages.
#         """
#         def fail(_):
#             self._close_transaction()
# 
#         def on_success_done(summary_metadata):
#             self._last_result._metadata.update(summary_metadata)
#             bookmark = self._last_result._metadata.get("bookmark")
#             if bookmark:
#                 self._bookmarks_in = tuple([bookmark])
#                 self._bookmark_out = bookmark
# 
#         def on_records_extend_records(records):
#             hydrant = self._last_result._hydrant
#             self._last_result._records.extend(hydrant.hydrate_records(self._last_result.keys(), records))
# 
#         # BOLT PULL
#         self._connection.pull(
#             n=self._config.fetch_size,
#             on_records=on_records_extend_records,
#             on_success=on_success_done,
#             on_failure=fail,
#             on_summary=lambda: self._last_result.detach(sync=False),
#         )

#    def connection_state(self):
#        """Internal API.
#
#        :return: The state of the BOLT connection.
#        """
#        if self._connection:
#            return self._connection.state
#
#    def set_connection_state(self, state):
#        """Internal API.
#
#        :param: The state for the BOLT connection.
#        :type: str
#        """
#        if self._connection:
#            self._connection.state = state

    def run(self, query, parameters=None, **kwparameters):
        """Run a Cypher query within an auto-commit transaction.

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

        if self._transaction:
            # Explicit transactions must be handled explicitly
            raise "hell"

        if self._autoResult:
            self._autoResult._detach()
            self._autoResult = None

        if not self._connection:
            self._connect(self._config.default_access_mode, database=self._config.database)
        cx = self._connection
        protocol_version = cx.PROTOCOL_VERSION
        server_info = cx.server_info

        # TODO: Should only handle auto-commit tx!!!!
        #has_transaction = self.has_transaction()

        #query_text = str(query)
        #query_metadata = getattr(query, "metadata", None)
        #query_timeout = getattr(query, "timeout", None)
        #parameters = DataDehydrator.fix_parameters(dict(parameters or {}, **kwparameters))

        #def fail(_):
        #    self._close_transaction()

        hydrant = DataHydrator()
        #result_metadata = {
        #    "query": query_text,
        #    "parameters": parameters,
        #    "server": server_info,
        #    "protocol_version": protocol_version,
        #}
        #run_metadata = {
        #    "metadata": query_metadata,
        #    "timeout": query_timeout,
        #    "on_success": result_metadata.update,
        #    "on_failure": fail,
        #}

        #run_metadata["bookmarks"] = self._bookmarks_in
        #bookmarks = run_metadata.get("bookmarks", self._config.bookmarks)

        # Create auto-commit transaction and run query on it
        self._autoResult = Result(cx, hydrant)
        self._autoResult.run(query, parameters, self._config.database, self._config.default_access_mode, self._bookmarks_in, **kwparameters)
        return result


        #self._last_result = result = Result(self, hydrant, result_metadata)  # The result object that consumes messages from the server

        #access_mode = None
        #db = None
        #bookmarks = None

        #if has_transaction:
        #    # Explicit Transaction Run does not carry any extra values. RUN "query" {parameters} {extra}
        #    if query_metadata:
        #        raise ValueError("Metadata can only be attached at transaction level")
        #    if query_timeout:
        #        raise ValueError("Timeouts only apply at transaction level")
        #    access_mode = None
        #    db = None
        #    bookmarks = None
        #else:
        #access_mode = self._config.default_access_mode
        #db = self._config.database

        # BOLT RUN
        #cx.run(
        #    query_text,
        #    parameters=parameters,
        #    mode=access_mode,
        #    bookmarks=bookmarks,
        #    metadata=run_metadata["metadata"],
        #    timeout=run_metadata["timeout"],
        #    db=db,
        #    on_success=run_metadata["on_success"],
        #    on_failure=run_metadata["on_failure"],
        #)

        #self.pull()

        #if not has_transaction:
        #    self._connection.send_all()
        #    self._connection.fetch_message()

        #return result

#    def send(self):
#        """Send all outstanding requests.
#        """
#        if self._connection:
#            self._connection.send_all()

    # TODO: Remove
    #def fetch(self):
        """Attempt to fetch at least one more record.

        :returns: number of records fetched
        """
    #    if self._connection:
    #        detail_count, _ = self._connection.fetch_message()
    #        return detail_count

    #    return 0

#    def sync(self):
#        """Carry out a full send and receive.
#
#        :returns: number of records fetched
#        """
#        if self._connection:
#            self._connection.send_all()
#            detail_count, _ = self._connection.fetch_all()
#            return detail_count
#
#        return 0

    # TODO: Remove
#     def detach(self, result, sync=True):
#         """Detach a result from this session by fetching and buffering any remaining records.
# 
#         :param result:
#         :param sync:
#         :returns: number of records fetched
#         """
#         count = 0
# 
#         if sync and result.attached():
#             self.send()
#             fetch = self.fetch
#             while result.attached():
#                 count += fetch()
# 
#         if self._last_result is result:
#             if not self.has_transaction():
#                 self._disconnect()
# 
#         result._session = None
#         return count

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
        if self._transaction:
            self._transaction._close()
            # TODO: Collect outgoing bookmark
            self._transaction = None

    def begin_transaction(self, metadata=None, timeout=None):
        """ Begin a new unmanaged transaction. Creates a new :class:`.Transaction` within this session.
            At most one transaction may exist in a session at any point in time.
            To maintain multiple concurrent transactions, use multiple concurrent sessions.

        :param metadata:
        :param timeout:

        :returns: A new transaction instance.
        :rtype: :class:`neo4j.Transaction`

        :raises TransactionError: :class:`neo4j.exceptions.TransactionError` if a transaction is already open.
        """
        # TODO: Implement TransactionConfig consumption

        if self._autoResult:
            self._autoResult._detach()
            self._autoResult = None

        if self._transaction:
            raise TransactionError("Explicit transaction already open")

        self._open_transaction(access_mode=self._config.default_access_mode, database=self._config.database, metadata=metadata, timeout=timeout)
        return self._transaction

    def _open_transaction(self, *, access_mode, database, metadata=None, timeout=None):
        #self._transaction = Transaction(self, on_close=self._close_transaction)
        self._connect(access_mode=access_mode, database=database)
        self._transaction = Transaction(self._connection)
        #self._connection.begin(bookmarks=self._bookmarks_in, metadata=metadata, timeout=timeout, mode=access_mode, db=database)
        self._transaction._begin(dtabase, self._bookmarks_in, access_mode, metadata, timeout)

    def commit_transaction(self):
        """ Commit the current transaction.

        :returns: the bookmark returned from the server, if any
        :rtype: :class: `neo4j.Bookmark`

        :raises TransactionError: :class:`neo4j.exceptions.TransactionError` if no transaction is currently open
        """
        if not self._transaction:
            raise TransactionError("No transaction to commit")
        #metadata = {}
        #try:
        #    self._connection.commit(on_success=metadata.update)
        #    self._connection.send_all()
        #    self._connection.fetch_all()
        #except BoltIncompleteCommitError:
        #    raise ServiceUnavailable("Connection closed during commit")
        #finally:
        #    self._disconnect()
        #    self._transaction = None
        #bookmark = metadata.get("bookmark")
        #self._bookmarks_in = tuple([bookmark])
        #self._bookmark_out = bookmark
        try:
            bookmark = self._transaction.commit()
        finally:
            self._close_transaction()
            self._disconnect()

        return bookmark

    def rollback_transaction(self):
        """ Rollback the current transaction.

        :raise: :class:`.TransactionError` if no transaction is currently open
        """
        if not self._transaction:
            raise TransactionError("No transaction to rollback")

        try:
            self._transaction.rollback()
        finally:
            self._close_transaction()
            self._disconnect()

        #cx = self._connection
        #if cx:
        #    metadata = {}
        #    try:
        #        cx.rollback(on_success=metadata.update)
        #        cx.send_all()
        #        cx.fetch_all()
        #    finally:
        #        self._disconnect()
        #        self._transaction = None

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
                self._open_transaction(access_mode=access_mode, database=self._config.database, metadata=metadata, timeout=timeout)
                tx = self._transaction
                try:
                    result = unit_of_work(tx, *args, **kwargs)
                except Exception:
                    #tx._success = False
                    tx.rollback()
                    raise
                else:
                    #if tx._success is None:
                    #    tx._success = True
                    tx.commit()
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
            if t1 - t0 > self._config.max_transaction_retry_time:
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


# class Transaction:
#     """ Container for multiple Cypher queries to be executed within
#     a single context. Transactions can be used within a :py:const:`with`
#     block where the transaction is committed or rolled back on based on
#     whether or not an exception is raised::
# 
#         with session.begin_transaction() as tx:
#             pass
# 
#     """
# 
#     #: When set, the transaction will be committed on close, otherwise it
#     #: will be rolled back. This attribute can be set in user code
#     #: multiple times before a transaction completes, with only the final
#     #: value taking effect.
#     #
#     # This became internal with Neo4j 4.0, when the server-side
#     # transaction semantics changed.
#     #
#     _success = None
# 
#     _closed = False
# 
#     def __init__(self, session, on_close):
#         self.session = session
#         self.on_close = on_close
# 
#     def __enter__(self):
#         return self
# 
#     def __exit__(self, exc_type, exc_value, traceback):
#         if self._closed:
#             return
#         if self._success is None:
#             self._success = not bool(exc_type)
#         self._close()
# 
#     def run(self, query, parameters=None, **kwparameters):
#         """ Run a Cypher query within the context of this transaction.
# 
#         The query is sent to the server lazily, when its result is
#         consumed. To force the query to be sent to the server, use
#         the :meth:`.Transaction.sync` method.
# 
#         Cypher is typically expressed as a query template plus a
#         set of named parameters. In Python, parameters may be expressed
#         through a dictionary of parameters, through individual parameter
#         arguments, or as a mixture of both. For example, the `run`
#         queries below are all equivalent::
# 
#             >>> query = "CREATE (a:Person {name:{name}, age:{age}})"
#             >>> tx.run(query, {"name": "Alice", "age": 33})
#             >>> tx.run(query, {"name": "Alice"}, age=33)
#             >>> tx.run(query, name="Alice", age=33)
# 
#         Parameter values can be of any type supported by the Neo4j type
#         system. In Python, this includes :class:`bool`, :class:`int`,
#         :class:`str`, :class:`list` and :class:`dict`. Note however that
#         :class:`list` properties must be homogenous.
# 
#         :param query: template Cypher query
#         :param parameters: dictionary of parameters
#         :param kwparameters: additional keyword parameters
#         :returns: :class:`neo4j.Result` object
#         :raise TransactionError: if the transaction is closed
#         """
#         self._assert_open()
#         return self.session.run(query, parameters, **kwparameters)
# 
#     def sync(self):
#         """ Force any queued queries to be sent to the server and
#         all related results to be fetched and buffered.
# 
#         :raise TransactionError: if the transaction is closed
#         """
#         self._assert_open()
#         self.session.sync()
# 
#     def commit(self):
#         """ Mark this transaction as successful and close in order to
#         trigger a COMMIT. This is functionally equivalent to::
# 
#         :raise TransactionError: if already closed
#         """
#         self._success = True
#         self._close()
# 
#     def rollback(self):
#         """ Mark this transaction as unsuccessful and close in order to
#         trigger a ROLLBACK. This is functionally equivalent to::
# 
#         :raise TransactionError: if already closed
#         """
#         self._success = False
#         self._close()
# 
#     def _close(self):
#         """ Close this transaction, triggering either a COMMIT or a ROLLBACK.
# 
#         :raise TransactionError: if already closed
#         """
#         self._assert_open()
#         try:
#             self.sync()
#         except Neo4jError:
#             self._success = False
#             raise
#         finally:
#             if self.session.has_transaction():
#                 if self._success:
#                     self.session.commit_transaction()
#                 else:
#                     self.session.rollback_transaction()
#             self._closed = True
#             self.on_close()
# 
#     def closed(self):
#         """ Indicator to show whether the transaction has been closed.
#         :returns: :const:`True` if closed, :const:`False` otherwise.
#         """
#         return self._closed
# 
#     def _assert_open(self):
#         if self._closed:
#             raise TransactionError("Transaction closed")


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


def unit_of_work(metadata=None, timeout=None):
    """This function is a decorator for transaction functions that allows extra control over how the transaction is carried out.

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
    return not (error.code in ("Neo.TransientError.Transaction.Terminated", "Neo.TransientError.Transaction.LockClientStopped"))
