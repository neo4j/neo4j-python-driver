#!/usr/bin/env python
# -*- coding: utf-8 -*-

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

from collections import namedtuple
import logging

from .compat import integer, perf_counter, string, urlparse
from .connection import connect, ResponseSubscriber
from .exceptions import CypherError
from .typesystem import hydrated

# Signature bytes for each message type
INIT = b"\x01"             # 0000 0001 // INIT <user_agent>
ACK_FAILURE = b"\x0F"      # 0000 1111 // ACK_FAILURE
RUN = b"\x10"              # 0001 0000 // RUN <statement> <parameters>
DISCARD_ALL = b"\x2F"      # 0010 1111 // DISCARD *
PULL_ALL = b"\x3F"         # 0011 1111 // PULL *
SUCCESS = b"\x70"          # 0111 0000 // SUCCESS <metadata>
RECORD = b"\x71"           # 0111 0001 // RECORD <value>
IGNORED = b"\x7E"          # 0111 1110 // IGNORED <metadata>
FAILURE = b"\x7F"          # 0111 1111 // FAILURE <metadata>

# Set up logger
log = logging.getLogger("neo4j")
log_debug = log.debug
log_info = log.info
log_warning = log.warning
log_error = log.error


Latency = namedtuple("Latency", ["overall", "network", "wait"])


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

            >>> from neo4j import GraphDatabase
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

    def session(self, **config):
        """ Create a new session based on the graph database details
        specified within this driver:

            >>> session = driver.session()

        """
        return Session(connect(self.host, self.port, **config))


class Session(object):
    """ Logical session carried out over an established TCP connection.
    Sessions should generally be constructed using the :meth:`.Driver.session`
    method.
    """

    def __init__(self, connection):
        self.connection = connection
        self.connection.init("neo4j-python/0.0")
        self.transaction = None
        self.bench_tests = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def run(self, statement, parameters=None):
        """ Run a parameterised Cypher statement.

        :param statement: Cypher statement to execute
        :param parameters: dictionary of parameters
        :return: list of :class:`.Record` objects
        """
        t = BenchTest()

        # Ensure the statement is a Unicode value
        if isinstance(statement, bytes):
            statement = statement.decode("UTF-8")

        parameters = dict(parameters or {})

        t.init = perf_counter()

        fields = []
        records = []

        def on_header(metadata):
            fields.extend(metadata["fields"])
            t.start_recv = perf_counter()

        def on_record(values):
            records.append(Record(fields, tuple(map(hydrated, values))))

        def on_footer(metadata):
            t.end_recv = perf_counter()

        def on_failure(metadata):
            raise CypherError("FAILURE")

        run_subscriber = ResponseSubscriber(self.connection)
        run_subscriber.on_success = on_header
        run_subscriber.on_failure = on_failure

        pull_all_subscriber = ResponseSubscriber(self.connection)
        pull_all_subscriber.on_record = on_record
        pull_all_subscriber.on_success = on_footer
        pull_all_subscriber.on_failure = on_failure

        if __debug__:
            log_info("C: RUN %r %r", statement, parameters)
            log_info("C: PULL_ALL")
        self.connection.append(RUN, (statement, parameters), subscriber=run_subscriber)
        self.connection.append(PULL_ALL, subscriber=pull_all_subscriber)
        t.start_send = perf_counter()
        self.connection.send()
        t.end_send = perf_counter()

        run_subscriber.consume()
        pull_all_subscriber.consume()

        t.done = perf_counter()
        self.bench_tests.append(t)

        return records

    def close(self):
        """ Shut down and close the session.
        """
        self.connection.close()

    def new_transaction(self):
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
    """ Record object for storing result values along with field names.
    Fields can be accessed by numeric or named index (``record[0]`` or
    ``record["field"]``) or by attribute (``record.field``).
    """

    def __init__(self, fields, values):
        self.__fields__ = fields
        self.__values__ = values

    def __repr__(self):
        values = self.__values__
        s = []
        for i, field in enumerate(self.__fields__):
            s.append("%s=%r" % (field, values[i]))
        return "<Record %s>" % " ".join(s)

    def __eq__(self, other):
        try:
            return vars(self) == vars(other)
        except TypeError:
            return tuple(self) == tuple(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __len__(self):
        return self.__fields__.__len__()

    def __getitem__(self, item):
        if isinstance(item, string):
            return getattr(self, item)
        elif isinstance(item, integer):
            return getattr(self, self.__fields__[item])
        else:
            raise TypeError(item)

    def __getattr__(self, item):
        try:
            i = self.__fields__.index(item)
        except ValueError:
            raise AttributeError("No field %r" % item)
        else:
            return self.__values__[i]
