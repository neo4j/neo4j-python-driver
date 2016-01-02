#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2015 "Neo Technology,"
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

"""
This module contains the main Bolt driver components as well as several
helper and exception classes. The main entry point is the `GraphDatabase`
class which can be used to obtain `Driver` instances that are used for
managing sessions.
"""


from __future__ import division

from collections import deque, namedtuple

from .compat import integer, perf_counter, string, urlparse
from .connection import connect, Response, RUN, PULL_ALL
from .exceptions import CypherError
from .typesystem import hydrated


STATEMENT_TYPE_READ_ONLY = "r"
STATEMENT_TYPE_READ_WRITE = "rw"
STATEMENT_TYPE_WRITE_ONLY = "w"
STATEMENT_TYPE_SCHEMA_WRITE = "sw"


Latency = namedtuple("Latency", ("overall", "network", "wait"))


class BenchTest(object):

    init = None
    start_send = None
    end_send = None
    start_recv = None
    end_recv = None
    done = None

    def latency(self):
        return Latency(self.done - self.init,
                       self.end_recv - self.start_send,
                       self.start_recv - self.end_send)


class GraphDatabase(object):
    """ The :class:`.GraphDatabase` class provides access to all graph
    database functionality. This is primarily used to construct a driver
    instance, using the :meth:`.driver` method.
    """

    @staticmethod
    def driver(url, **config):
        """ Acquire a :class:`.Driver` instance for the given URL and
        configuration:

            >>> from neo4j.v1 import GraphDatabase
            >>> driver = GraphDatabase.driver("bolt://localhost")

        """
        return Driver(url, **config)


class Driver(object):
    """ Accessor for a specific graph database resource.
    """

    def __init__(self, url, **config):
        self.url = url
        parsed = urlparse(self.url)
        if parsed.scheme == "bolt":
            self.host = parsed.hostname
            self.port = parsed.port
        else:
            raise ValueError("Unsupported URL scheme: %s" % parsed.scheme)
        self.config = config
        self.sessions = deque()

    def session(self):
        """ Create a new session based on the graph database details
        specified within this driver:

            >>> session = driver.session()

        """
        try:
            return self.sessions.pop()
        except IndexError:
            return Session(self)


class Result(list):
    """ A handler for the result of Cypher statement execution.
    """

    #: The statement that was executed to produce this result.
    statement = None

    #: Dictionary of parameters passed with the statement.
    parameters = None

    def __init__(self, session, statement, parameters):
        super(Result, self).__init__()
        self.session = session
        self.statement = statement
        self.parameters = parameters
        self.keys = None
        self.complete = False
        self.summary = None
        self.bench_test = None

    def on_header(self, metadata):
        """ Called on receipt of the result header.
        """
        self.keys = metadata["fields"]
        if self.bench_test:
            self.bench_test.start_recv = perf_counter()

    def on_record(self, values):
        """ Called on receipt of each result record.
        """
        self.append(Record(self.keys, tuple(map(hydrated, values))))

    def on_footer(self, metadata):
        """ Called on receipt of the result footer.
        """
        self.complete = True
        self.summary = ResultSummary(self.statement, self.parameters, **metadata)
        if self.bench_test:
            self.bench_test.end_recv = perf_counter()

    def on_failure(self, metadata):
        """ Called on execution failure.
        """
        raise CypherError(metadata)

    def consume(self):
        """ Consume the remainder of this result, triggering all appropriate
        callback functions.
        """
        fetch_next = self.session.connection.fetch_next
        while not self.complete:
            fetch_next()

    def summarize(self):
        """ Consume the remainder of this result and produce a summary.

        :rtype: ResultSummary
        """
        self.consume()
        return self.summary


class ResultSummary(object):
    """ A summary of execution returned with a :class:`.Result` object.
    """

    #: The statement that was executed to produce this result.
    statement = None

    #: Dictionary of parameters passed with the statement.
    parameters = None

    #: The type of statement (``'r'`` = read-only, ``'rw'`` = read/write).
    statement_type = None

    #: A set of statistical information held in a :class:`.StatementStatistics` instance.
    statistics = None

    #: A :class:`.Plan` instance
    plan = None

    #: A :class:`.ProfiledPlan` instance
    profile = None

    #: Notifications provide extra information for a user executing a statement.
    #: They can be warnings about problematic queries or other valuable information that can be
    #: presented in a client.
    #: Unlike failures or errors, notifications do not affect the execution of a statement.
    notifications = None

    def __init__(self, statement, parameters, **metadata):
        self.statement = statement
        self.parameters = parameters
        self.statement_type = metadata.get("type")
        self.statistics = StatementStatistics(metadata.get("stats", {}))
        if "plan" in metadata:
            self.plan = make_plan(metadata["plan"])
        if "profile" in metadata:
            self.profile = make_plan(metadata["profile"])
            self.plan = self.profile
        self.notifications = []
        for notification in metadata.get("notifications", []):
            position = notification.get("position")
            if position is not None:
                position = Position(position["offset"], position["line"], position["column"])
            self.notifications.append(Notification(notification["code"], notification["title"],
                                                   notification["description"], position))


class StatementStatistics(object):
    """ Set of statistics from a Cypher statement execution.
    """

    #:
    contains_updates = False

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
#: position:
#:   the position in the statement where this notification points to, if relevant.
Notification = namedtuple("Notification", ("code", "title", "description", "position"))

#: A position within a statement, consisting of offset, line and column.
#:
#: offset:
#:   the character offset referred to by this position; offset numbers start at 0
#: line:
#:   the line number referred to by the position; line numbers start at 1
#: column:
#:   the column number referred to by the position; column numbers start at 1
Position = namedtuple("Position", ("offset", "line", "column"))


def make_plan(plan_dict):
    """ Construct a Plan or ProfiledPlan from a dictionary of metadata values.

    :param plan_dict:
    :return:
    """
    operator_type = plan_dict["operatorType"]
    identifiers = plan_dict.get("identifiers", [])
    arguments = plan_dict.get("args", [])
    children = [make_plan(child) for child in plan_dict.get("children", [])]
    if "dbHits" in plan_dict or "rows" in plan_dict:
        db_hits = plan_dict.get("dbHits", 0)
        rows = plan_dict.get("rows", 0)
        return ProfiledPlan(operator_type, identifiers, arguments, children, db_hits, rows)
    else:
        return Plan(operator_type, identifiers, arguments, children)


class Session(object):
    """ Logical session carried out over an established TCP connection.
    Sessions should generally be constructed using the :meth:`.Driver.session`
    method.
    """

    def __init__(self, driver):
        self.driver = driver
        self.connection = connect(driver.host, driver.port, **driver.config)
        self.transaction = None
        self.bench_tests = []

    def __del__(self):
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def run(self, statement, parameters=None):
        """ Run a parameterised Cypher statement.

        :param statement: Cypher statement to execute
        :param parameters: dictionary of parameters
        :return: Cypher result
        :rtype: :class:`.Result`
        """

        # Ensure the statement is a Unicode value
        if isinstance(statement, bytes):
            statement = statement.decode("UTF-8")

        params = {}
        for key, value in (parameters or {}).items():
            if isinstance(key, bytes):
                key = key.decode("UTF-8")
            if isinstance(value, bytes):
                params[key] = value.decode("UTF-8")
            else:
                params[key] = value
        parameters = params

        t = BenchTest()
        t.init = perf_counter()

        result = Result(self, statement, parameters)
        result.bench_test = t

        run_response = Response(self.connection)
        run_response.on_success = result.on_header
        run_response.on_failure = result.on_failure

        pull_all_response = Response(self.connection)
        pull_all_response.on_record = result.on_record
        pull_all_response.on_success = result.on_footer
        pull_all_response.on_failure = result.on_failure

        self.connection.append(RUN, (statement, parameters), response=run_response)
        self.connection.append(PULL_ALL, response=pull_all_response)
        t.start_send = perf_counter()
        self.connection.send()
        t.end_send = perf_counter()

        result.consume()

        t.done = perf_counter()
        self.bench_tests.append(t)

        return result

    def close(self):
        """ Return this session to the driver pool it came from.
        """
        self.driver.sessions.appendleft(self)

    def begin_transaction(self):
        """ Create a new :class:`.Transaction` within this session.

        :return: new :class:`.Transaction` instance.
        """
        assert not self.transaction
        self.transaction = Transaction(self)
        return self.transaction


class Transaction(object):
    """ Container for multiple Cypher queries to be executed within
    a single context. Transactions can be used within a :py:const:`with`
    block where the value of :attr:`.success` will determine whether
    the transaction is committed or rolled back on :meth:`.Transaction.close`::

        with session.new_transaction() as tx:
            pass

    """

    #: When closed, the transaction will be committed if marked as successful
    #: and rolled back otherwise. This attribute can be set in user code
    #: multiple times before a transaction completes with only the final
    #: value taking effect.
    success = False

    #: Indicator to show whether the transaction has been closed, either
    #: with commit or rollback.
    closed = False

    def __init__(self, session):
        self.session = session
        self.session.run("BEGIN")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def run(self, statement, parameters=None):
        """ Run a Cypher statement within the context of this transaction.

        :param statement:
        :param parameters:
        :return:
        """
        assert not self.closed
        return self.session.run(statement, parameters)

    def commit(self):
        """ Mark this transaction as successful and close in order to
        trigger a COMMIT.
        """
        self.success = True
        self.close()

    def rollback(self):
        """ Mark this transaction as unsuccessful and close in order to
        trigger a ROLLBACK.
        """
        self.success = False
        self.close()

    def close(self):
        """ Close this transaction, triggering either a COMMIT or a ROLLBACK.
        """
        assert not self.closed
        if self.success:
            self.session.run("COMMIT")
        else:
            self.session.run("ROLLBACK")
        self.closed = True
        self.session.transaction = None

class Record(object):
    """ Record is an ordered collection of fields.

    A Record object is used for storing result values along with field names.
    Fields can be accessed by numeric or named index (``record[0]`` or
    ``record["field"]``).
    """

    def __init__(self, keys, values):
        self._keys = tuple(keys)
        self._values = tuple(values)

    def keys(self):
        """ Return the keys (key names) of the record
        """
        return self._keys

    def values(self):
        """ Return the values of the record
        """
        return self._values

    def items(self):
        """ Return the fields of the record as a list of key and value tuples
        """
        return zip(self._keys, self._values)

    def index(self, key):
        """ Return the index of the given key
        """
        try:
            return self._keys.index(key)
        except ValueError:
            raise KeyError(key)

    def __record__(self):
        return self

    def __contains__(self, key):
        return self._keys.__contains__(key)

    def __iter__(self):
        return iter(self._keys)

    def copy(self):
        return Record(self._keys, self._values)

    def __getitem__(self, item):
        if isinstance(item, string):
            return self._values[self.index(item)]
        elif isinstance(item, integer):
            return self._values[item]
        else:
            raise TypeError(item)

    def __len__(self):
        return len(self._keys)

    def __repr__(self):
        values = self._values
        s = []
        for i, field in enumerate(self._keys):
            s.append("%s=%r" % (field, values[i]))
        return "<Record %s>" % " ".join(s)

    def __hash__(self):
        return hash(self._keys) ^ hash(self._values)

    def __eq__(self, other):
        try:
            return self._keys == tuple(other.keys()) and self._values == tuple(other.values())
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

def record(obj):
    """ Obtain an immutable record for the given object
    (either by calling obj.__record__() or by copying out the record data)
    """
    try:
        return obj.__record__()
    except AttributeError:
        keys = obj.keys()
        values = []
        for key in keys:
            values.append(obj[key])
        return Record(keys, values)



