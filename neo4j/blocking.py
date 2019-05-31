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


from collections import deque, namedtuple
from logging import getLogger
from random import random
from time import perf_counter, sleep
from warnings import warn

from neo4j import READ_ACCESS, WRITE_ACCESS
from neo4j.config import default_config
from neobolt.exceptions import (
    ConnectionExpired,
    CypherError,
    IncompleteCommitError,
    ServiceUnavailable,
    TransientError,
)
from neo4j.types import PackStreamHydrator, PackStreamDehydrator


INITIAL_RETRY_DELAY = 1.0
RETRY_DELAY_MULTIPLIER = 2.0
RETRY_DELAY_JITTER_FACTOR = 0.2

STATEMENT_TYPE_READ_ONLY = "r"
STATEMENT_TYPE_READ_WRITE = "rw"
STATEMENT_TYPE_WRITE_ONLY = "w"
STATEMENT_TYPE_SCHEMA_WRITE = "s"


log = getLogger("neo4j")


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

    # The current :class:`.Transaction` instance, if any.
    _transaction = None

    # The last result received.
    _last_result = None

    # The set of bookmarks after which the next
    # :class:`.Transaction` should be carried out.
    _bookmarks_in = None

    # The bookmark returned from the last commit.
    _bookmark_out = None

    # Default maximum time to keep retrying failed transactions.
    _max_retry_time = default_config["max_retry_time"]

    _closed = False

    def __init__(self, acquirer, **parameters):
        self._acquirer = acquirer
        self._default_access_mode = parameters.get("access_mode")
        for key, value in parameters.items():
            if key == "bookmark":
                if value:
                    self._bookmarks_in = tuple([value])
            elif key == "bookmarks":
                if value:
                    self._bookmarks_in = tuple(value)
            elif key == "max_retry_time":
                self._max_retry_time = value
            else:
                pass  # for compatibility

    def __del__(self):
        try:
            self.close()
        except:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _connect(self, access_mode=None):
        if access_mode is None:
            access_mode = self._default_access_mode
        if self._connection:
            log.warning("FIXME: should always disconnect before connect")
            self._connection.send_all()
            self._connection.fetch_all()
            self._disconnect()
        self._connection = self._acquirer(access_mode)

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
            except (ConnectionExpired, CypherError, TransactionError,
                    ServiceUnavailable, SessionError):
                pass
            finally:
                self._disconnect()
        self._closed = True

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
        from neobolt.exceptions import ConnectionExpired

        self._assert_open()
        if not statement:
            raise ValueError("Cannot run an empty statement")
        if not isinstance(statement, (str, Statement)):
            raise TypeError("Statement must be a string or a Statement instance")

        if not self._connection:
            self._connect()
        cx = self._connection
        protocol_version = cx.protocol_version
        server = cx.server

        has_transaction = self.has_transaction()

        statement_text = str(statement)
        statement_metadata = getattr(statement, "metadata", None)
        statement_timeout = getattr(statement, "timeout", None)
        parameters = fix_parameters(dict(parameters or {}, **kwparameters), protocol_version)

        def fail(_):
            self._close_transaction()

        hydrant = PackStreamHydrator(protocol_version)
        result_metadata = {
            "statement": statement_text,
            "parameters": parameters,
            "server": server,
            "protocol_version": protocol_version,
        }
        run_metadata = {
            "metadata": statement_metadata,
            "timeout": statement_timeout,
            "on_success": result_metadata.update,
            "on_failure": fail,
        }

        def done(summary_metadata):
            result_metadata.update(summary_metadata)
            bookmark = result_metadata.get("bookmark")
            if bookmark:
                self._bookmarks_in = tuple([bookmark])
                self._bookmark_out = bookmark

        self._last_result = result = BoltStatementResult(self, hydrant, result_metadata)

        if has_transaction:
            if statement_metadata:
                raise ValueError("Metadata can only be attached at transaction level")
            if statement_timeout:
                raise ValueError("Timeouts only apply at transaction level")
        else:
            run_metadata["bookmarks"] = self._bookmarks_in

        cx.run(statement_text, parameters, **run_metadata)
        cx.pull_all(
            on_records=lambda records: result._records.extend(
                hydrant.hydrate_records(result.keys(), records)),
            on_success=done,
            on_failure=fail,
            on_summary=lambda: result.detach(sync=False),
        )

        if not has_transaction:
            try:
                self._connection.send_all()
                self._connection.fetch_message()
            except ConnectionExpired as error:
                raise SessionExpired(*error.args)

        return result

    def send(self):
        """ Send all outstanding requests.
        """
        from neobolt.exceptions import ConnectionExpired
        if self._connection:
            try:
                self._connection.send_all()
            except ConnectionExpired as error:
                raise SessionExpired(*error.args)

    def fetch(self):
        """ Attempt to fetch at least one more record.

        :returns: number of records fetched
        """
        from neobolt.exceptions import ConnectionExpired
        if self._connection:
            try:
                detail_count, _ = self._connection.fetch_message()
            except ConnectionExpired as error:
                raise SessionExpired(*error.args)
            else:
                return detail_count
        return 0

    def sync(self):
        """ Carry out a full send and receive.

        :returns: number of records fetched
        """
        from neobolt.exceptions import ConnectionExpired
        if self._connection:
            try:
                self._connection.send_all()
                detail_count, _ = self._connection.fetch_all()
            except ConnectionExpired as error:
                raise SessionExpired(*error.args)
            else:
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
        self._assert_open()
        if self.has_transaction():
            raise TransactionError("Explicit transaction already open")

        self._open_transaction(metadata=metadata, timeout=timeout)
        return self._transaction

    def _open_transaction(self, access_mode=None, metadata=None, timeout=None):
        self._transaction = Transaction(self, on_close=self._close_transaction)
        self._connect(access_mode)
        self._connection.begin(bookmarks=self._bookmarks_in, metadata=metadata, timeout=timeout)

    def commit_transaction(self):
        """ Commit the current transaction.

        :returns: the bookmark returned from the server, if any
        :raise: :class:`.TransactionError` if no transaction is currently open
        """
        self._assert_open()
        if not self._transaction:
            raise TransactionError("No transaction to commit")
        metadata = {}
        try:
            self._connection.commit(on_success=metadata.update)
            self._connection.send_all()
            self._connection.fetch_all()
        except IncompleteCommitError:
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
        self._assert_open()
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
        from neobolt.exceptions import ConnectionExpired, TransientError, ServiceUnavailable

        if not callable(unit_of_work):
            raise TypeError("Unit of work is not callable")

        metadata = getattr(unit_of_work, "metadata", None)
        timeout = getattr(unit_of_work, "timeout", None)

        retry_delay = retry_delay_generator(INITIAL_RETRY_DELAY,
                                            RETRY_DELAY_MULTIPLIER,
                                            RETRY_DELAY_JITTER_FACTOR)
        errors = []
        t0 = perf_counter()
        while True:
            try:
                self._open_transaction(access_mode, metadata, timeout)
                tx = self._transaction
                try:
                    result = unit_of_work(tx, *args, **kwargs)
                except Exception:
                    tx.success = False
                    raise
                else:
                    if tx.success is None:
                        tx.success = True
                finally:
                    tx.close()
            except (ServiceUnavailable, SessionExpired, ConnectionExpired) as error:
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
            delay = next(retry_delay)
            log.warning("Transaction failed and will be retried in {}s "
                        "({})".format(delay, "; ".join(errors[-1].args)))
            sleep(delay)
        if errors:
            raise errors[-1]
        else:
            raise ServiceUnavailable("Transaction failed")

    def read_transaction(self, unit_of_work, *args, **kwargs):
        self._assert_open()
        return self._run_transaction(READ_ACCESS, unit_of_work, *args, **kwargs)

    def write_transaction(self, unit_of_work, *args, **kwargs):
        self._assert_open()
        return self._run_transaction(WRITE_ACCESS, unit_of_work, *args, **kwargs)

    def _assert_open(self):
        if self._closed:
            raise SessionError("Session closed")


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
    #: multiple times before a transaction completes, with only the final
    #: value taking effect.
    success = None

    _closed = False

    def __init__(self, session, on_close):
        self.session = session
        self.on_close = on_close

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._closed:
            return
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
        :raise TransactionError: if the transaction is closed
        """
        self._assert_open()
        return self.session.run(statement, parameters, **kwparameters)

    def sync(self):
        """ Force any queued statements to be sent to the server and
        all related results to be fetched and buffered.

        :raise TransactionError: if the transaction is closed
        """
        self._assert_open()
        self.session.sync()

    def commit(self):
        """ Mark this transaction as successful and close in order to
        trigger a COMMIT. This is functionally equivalent to::

            tx.success = True
            tx.close()

        :raise TransactionError: if already closed
        """
        self.success = True
        self.close()

    def rollback(self):
        """ Mark this transaction as unsuccessful and close in order to
        trigger a ROLLBACK. This is functionally equivalent to::

            tx.success = False
            tx.close()

        :raise TransactionError: if already closed
        """
        self.success = False
        self.close()

    def close(self):
        """ Close this transaction, triggering either a COMMIT or a ROLLBACK,
        depending on the value of :attr:`.success`.

        :raise TransactionError: if already closed
        """
        from neobolt.exceptions import CypherError
        self._assert_open()
        try:
            self.sync()
        except CypherError:
            self.success = False
            raise
        finally:
            if self.session.has_transaction():
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

    def _assert_open(self):
        if self._closed:
            raise TransactionError("Transaction closed")


class Statement(object):

    def __init__(self, text, metadata=None, timeout=None):
        self.text = text
        try:
            self.metadata = metadata
        except TypeError:
            raise TypeError("Metadata must be coercible to a dict")
        try:
            self.timeout = timeout
        except TypeError:
            raise TypeError("Timeout must be specified as a number of seconds")

    def __str__(self):
        return str(self.text)


def fix_parameters(parameters):
    if not parameters:
        return {}
    dehydrator = PackStreamDehydrator()
    try:
        dehydrated, = dehydrator.dehydrate([parameters])
    except TypeError as error:
        value = error.args[0]
        raise TypeError("Parameters of type {} are not supported".format(type(value).__name__))
    else:
        return dehydrated


class StatementResult(object):
    """ A handler for the result of Cypher statement execution. Instances
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
        return self._session and not self._session.closed()

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

        :returns: The :class:`.ResultSummary` for this result
        """
        self.detach()
        if self._summary is None:
            self._summary = BoltStatementResultSummary(**self._metadata)
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


class BoltStatementResult(StatementResult):
    """ A handler for the result of Cypher statement execution.
    """

    def __init__(self, session, hydrant, metadata):
        super(BoltStatementResult, self).__init__(session, hydrant, metadata)

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


class BoltStatementResultSummary(object):
    """ A summary of execution returned with a :class:`.StatementResult` object.
    """

    #: The version of Bolt protocol over which this result was obtained.
    protocol_version = None

    #: The server on which this result was generated.
    server = None

    #: The statement that was executed to produce this result.
    statement = None

    #: Dictionary of parameters passed with the statement.
    parameters = None

    #: The type of statement (``'r'`` = read-only, ``'rw'`` = read/write).
    statement_type = None

    #: A set of statistical information held in a :class:`.Counters` instance.
    counters = None

    #: A :class:`.Plan` instance
    plan = None

    #: A :class:`.ProfiledPlan` instance
    profile = None

    #: The time it took for the server to have the result available.
    result_available_after = None

    #: The time it took for the server to consume the result.
    result_consumed_after = None

    #: Notifications provide extra information for a user executing a statement.
    #: They can be warnings about problematic queries or other valuable information that can be
    #: presented in a client.
    #: Unlike failures or errors, notifications do not affect the execution of a statement.
    notifications = None

    def __init__(self, **metadata):
        self.metadata = metadata
        self.protocol_version = metadata.get("protocol_version")
        self.server = metadata.get("server")
        self.statement = metadata.get("statement")
        self.parameters = metadata.get("parameters")
        self.statement_type = metadata.get("type")
        self.counters = SummaryCounters(metadata.get("stats", {}))
        self.result_available_after = metadata.get("result_available_after")
        self.result_consumed_after = metadata.get("result_consumed_after")
        self.t_first = metadata.get("t_first")
        self.t_last = metadata.get("t_last")
        if "plan" in metadata:
            self.plan = _make_plan(metadata["plan"])
        if "profile" in metadata:
            self.profile = _make_plan(metadata["profile"])
            self.plan = self.profile
        self.notifications = []
        for notification in metadata.get("notifications", []):
            position = notification.get("position")
            if position is not None:
                position = Position(position["offset"], position["line"], position["column"])
            self.notifications.append(Notification(notification["code"], notification["title"],
                                                   notification["description"], notification["severity"], position))


class SummaryCounters(object):
    """ Set of statistics from a Cypher statement execution.
    """

    #:
    nodes_created = 0

    #:
    nodes_deleted = 0

    #:
    relationships_created = 0

    #:
    relationships_deleted = 0

    #:
    properties_set = 0

    #:
    labels_added = 0

    #:
    labels_removed = 0

    #:
    indexes_added = 0

    #:
    indexes_removed = 0

    #:
    constraints_added = 0

    #:
    constraints_removed = 0

    def __init__(self, statistics):
        for key, value in dict(statistics).items():
            key = key.replace("-", "_")
            setattr(self, key, value)

    def __repr__(self):
        return repr(vars(self))

    @property
    def contains_updates(self):
        return bool(self.nodes_created or self.nodes_deleted or
                    self.relationships_created or self.relationships_deleted or
                    self.properties_set or self.labels_added or self.labels_removed or
                    self.indexes_added or self.indexes_removed or
                    self.constraints_added or self.constraints_removed)


#: A plan describes how the database will execute your statement.
#:
#: operator_type:
#:   the name of the operation performed by the plan
#: identifiers:
#:   the list of identifiers used by this plan
#: arguments:
#:   a dictionary of arguments used in the specific operation performed by the plan
#: children:
#:   a list of sub-plans
Plan = namedtuple("Plan", ("operator_type", "identifiers", "arguments", "children"))

#: A profiled plan describes how the database executed your statement.
#:
#: db_hits:
#:   the number of times this part of the plan touched the underlying data stores
#: rows:
#:   the number of records this part of the plan produced
ProfiledPlan = namedtuple("ProfiledPlan", Plan._fields + ("db_hits", "rows"))

#: Representation for notifications found when executing a statement. A
#: notification can be visualized in a client pinpointing problems or
#: other information about the statement.
#:
#: code:
#:   a notification code for the discovered issue.
#: title:
#:   a short summary of the notification
#: description:
#:   a long description of the notification
#: severity:
#:   the severity level of the notification
#: position:
#:   the position in the statement where this notification points to, if relevant.
Notification = namedtuple("Notification", ("code", "title", "description", "severity", "position"))

#: A position within a statement, consisting of offset, line and column.
#:
#: offset:
#:   the character offset referred to by this position; offset numbers start at 0
#: line:
#:   the line number referred to by the position; line numbers start at 1
#: column:
#:   the column number referred to by the position; column numbers start at 1
Position = namedtuple("Position", ("offset", "line", "column"))


def _make_plan(plan_dict):
    """ Construct a Plan or ProfiledPlan from a dictionary of metadata values.

    :param plan_dict:
    :return:
    """
    operator_type = plan_dict["operatorType"]
    identifiers = plan_dict.get("identifiers", [])
    arguments = plan_dict.get("args", [])
    children = [_make_plan(child) for child in plan_dict.get("children", [])]
    if "dbHits" in plan_dict or "rows" in plan_dict:
        db_hits = plan_dict.get("dbHits", 0)
        rows = plan_dict.get("rows", 0)
        return ProfiledPlan(operator_type, identifiers, arguments, children, db_hits, rows)
    else:
        return Plan(operator_type, identifiers, arguments, children)


class SessionError(Exception):
    """ Raised when an error occurs while using a session.
    """

    def __init__(self, session, *args, **kwargs):
        super(SessionError, self).__init__(*args, **kwargs)
        self.session = session


class SessionExpired(SessionError):
    """ Raised when no a session is no longer able to fulfil
    the purpose described by its original parameters.
    """

    def __init__(self, session, *args, **kwargs):
        super(SessionExpired, self).__init__(session, *args, **kwargs)


class TransactionError(Exception):
    """ Raised when an error occurs while using a transaction.
    """

    def __init__(self, transaction, *args, **kwargs):
        super(TransactionError, self).__init__(*args, **kwargs)
        self.transaction = transaction


def unit_of_work(metadata=None, timeout=None):
    """ This function is a decorator for transaction functions that allows
    extra control over how the transaction is carried out.

    For example, a timeout (in seconds) may be applied::

        @unit_of_work(timeout=25.0)
        def count_people(tx):
            return tx.run("MATCH (a:Person) RETURN count(a)").single().value()

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
