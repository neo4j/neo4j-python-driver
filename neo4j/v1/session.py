#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2016 "Neo Technology,"
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
This module contains the main user-facing abstractions. The main entry
point is the `GraphDatabase` class which can be used to obtain `Driver`
instances that are in turn used for managing sessions.
"""


from __future__ import division

from collections import deque

from .bolt import connect, Response, RUN, PULL_ALL
from .compat import integer, string, urlparse
from .constants import DEFAULT_PORT, ENCRYPTED_DEFAULT, TRUST_DEFAULT, TRUST_SIGNED_CERTIFICATES
from .exceptions import CypherError, ProtocolError, ResultError
from .ssl_compat import SSL_AVAILABLE, SSLContext, PROTOCOL_SSLv23, OP_NO_SSLv2, CERT_REQUIRED
from .summary import ResultSummary
from .types import hydrated


DEFAULT_MAX_POOL_SIZE = 50


class AuthToken(object):
    """ Container for auth information
    """

    def __init__(self, scheme, principal, credentials):
        self.scheme = scheme
        self.principal = principal
        self.credentials = credentials


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

    def __init__(self, address, **config):
        if "://" in address:
            parsed = urlparse(address)
            if parsed.scheme == "bolt":
                host = parsed.hostname
                port = parsed.port or DEFAULT_PORT
            else:
                raise ProtocolError("Only the 'bolt' URI scheme is supported [%s]" % address)
        elif ":" in address:
            host, port = address.split(":")
            port = int(port)
        else:
            host = address
            port = DEFAULT_PORT
        self.address = (host, port)
        self.config = config
        self.max_pool_size = config.get("max_pool_size", DEFAULT_MAX_POOL_SIZE)
        self.session_pool = deque()
        try:
            self.encrypted = encrypted = config["encrypted"]
        except KeyError:
            _warn_about_insecure_default()
            self.encrypted = encrypted = ENCRYPTED_DEFAULT
        self.trust = trust = config.get("trust", TRUST_DEFAULT)
        if encrypted:
            if not SSL_AVAILABLE:
                raise RuntimeError("Bolt over TLS is only available in Python 2.7.9+ and Python 3.3+")
            ssl_context = SSLContext(PROTOCOL_SSLv23)
            ssl_context.options |= OP_NO_SSLv2
            if trust >= TRUST_SIGNED_CERTIFICATES:
                ssl_context.verify_mode = CERT_REQUIRED
            ssl_context.set_default_verify_paths()
            self.ssl_context = ssl_context
        else:
            self.ssl_context = None

    def session(self):
        """ Create a new session based on the graph database details
        specified within this driver:

            >>> from neo4j.v1 import GraphDatabase
            >>> driver = GraphDatabase.driver("bolt://localhost")
            >>> session = driver.session()
        """
        session = None
        connected = False
        while not connected:
            try:
                session = self.session_pool.pop()
            except IndexError:
                connection = connect(self.address, self.ssl_context, **self.config)
                session = Session(self, connection)
                connected = True
            else:
                if session.healthy:
                    session.connection.reset()
                    connected = session.healthy
        return session

    def recycle(self, session):
        """ Accept a session for recycling, if healthy.

        :param session:
        :return:
        """
        pool = self.session_pool
        for s in list(pool):  # freezing the pool into a list for iteration allows pool mutation inside the loop
            if not s.healthy:
                pool.remove(s)
        if session.healthy and len(pool) < self.max_pool_size and session not in pool:
            pool.appendleft(session)


class StatementResult(object):
    """ A handler for the result of Cypher statement execution.
    """

    #: The statement text that was executed to produce this result.
    statement = None

    #: Dictionary of parameters passed with the statement.
    parameters = None

    def __init__(self, connection, run_response, pull_all_response):
        super(StatementResult, self).__init__()

        # The Connection instance behind this result.
        self.connection = connection

        # The keys for the records in the result stream. These are
        # lazily populated on request.
        self._keys = None

        # Buffer for incoming records to be queued before yielding. If
        # the result is used immediately, this buffer will be ignored.
        self._buffer = deque()

        # The result summary (populated after the records have been
        # fully consumed).
        self._summary = None

        # Flag to indicate whether the entire stream has been consumed
        # from the network (but not necessarily yielded).
        self._consumed = False

        def on_header(metadata):
            # Called on receipt of the result header.
            self._keys = metadata["fields"]

        def on_record(values):
            # Called on receipt of each result record.
            self._buffer.append(values)

        def on_footer(metadata):
            # Called on receipt of the result footer.
            self._summary = ResultSummary(self.statement, self.parameters, **metadata)
            self._consumed = True

        def on_failure(metadata):
            # Called on execution failure.
            self._consumed = True
            raise CypherError(metadata)

        run_response.on_success = on_header
        run_response.on_failure = on_failure

        pull_all_response.on_record = on_record
        pull_all_response.on_success = on_footer
        pull_all_response.on_failure = on_failure

    def __iter__(self):
        while self._buffer:
            values = self._buffer.popleft()
            yield Record(self.keys(), tuple(map(hydrated, values)))
        while not self._consumed:
            self.connection.fetch()
            while self._buffer:
                values = self._buffer.popleft()
                yield Record(self.keys(), tuple(map(hydrated, values)))

    def keys(self):
        """ Return the keys for the records.
        """
        # Fetch messages until we have the header or a failure
        while self._keys is None and not self._consumed:
            self.connection.fetch()
        return tuple(self._keys)

    def buffer(self):
        if self.connection and not self.connection.closed:
            while not self._consumed:
                self.connection.fetch()
            self.connection = None

    def consume(self):
        """ Consume the remainder of this result and return the
        summary.
        """
        if self.connection and not self.connection.closed:
            list(self)
            self.connection = None
        return self._summary

    def single(self):
        """ Return the next record, failing if none or more than one remain.
        """
        records = list(self)
        num_records = len(records)
        if num_records == 0:
            raise ResultError("Cannot retrieve a single record, because this result is empty.")
        elif num_records != 1:
            raise ResultError("Expected a result with a single record, but this result contains at least one more.")
        else:
            return records[0]

    def peek(self):
        """ Return the next record without advancing the cursor. Fails
        if no records remain.
        """
        if self._buffer:
            values = self._buffer[0]
            return Record(self.keys(), tuple(map(hydrated, values)))
        while not self._buffer and not self._consumed:
            self.connection.fetch()
            if self._buffer:
                values = self._buffer[0]
                return Record(self.keys(), tuple(map(hydrated, values)))
        raise ResultError("End of stream")


class Session(object):
    """ Logical session carried out over an established TCP connection.
    Sessions should generally be constructed using the :meth:`.Driver.session`
    method.
    """

    def __init__(self, driver, connection):
        self.driver = driver
        self.connection = connection
        self.transaction = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    @property
    def healthy(self):
        """ Return ``True`` if this session is healthy, ``False`` if
        unhealthy and ``None`` if closed.
        """
        return self.connection.healthy

    def run(self, statement, parameters=None):
        """ Run a parameterised Cypher statement.

        :param statement: Cypher statement to execute
        :param parameters: dictionary of parameters
        :return: Cypher result
        :rtype: :class:`.StatementResult`
        """
        if self.transaction:
            raise ProtocolError("Statements cannot be run directly on a session with an open transaction;"
                                " either run from within the transaction or use a different session.")
        return run(self.connection, statement, parameters)

    def close(self):
        """ Recycle this session through the driver it came from.
        """
        if self.connection and not self.connection.closed:
            self.connection.fetch_all()
        if self.transaction:
            self.transaction.close()
        self.driver.recycle(self)

    def begin_transaction(self):
        """ Create a new :class:`.Transaction` within this session.

        :return: new :class:`.Transaction` instance.
        """
        if self.transaction:
            raise ProtocolError("You cannot begin a transaction on a session with an open transaction;"
                                " either run from within the transaction or use a different session.")

        def clear_transaction():
            self.transaction = None

        self.transaction = Transaction(self.connection, on_close=clear_transaction)
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

    def __init__(self, connection, on_close):
        self.connection = connection
        self.on_close = on_close
        run(self.connection, "BEGIN")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_value:
            self.success = False
        self.close()

    def run(self, statement, parameters=None):
        """ Run a Cypher statement within the context of this transaction.

        :param statement: Cypher statement
        :param parameters: dictionary of parameters
        :return: result object
        """
        assert not self.closed
        return run(self.connection, statement, parameters)

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
            run(self.connection, "COMMIT")
        else:
            run(self.connection, "ROLLBACK")
        self.closed = True
        self.on_close()


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


def basic_auth(user, password):
    """ Generate a basic auth token for a given user and password.

    :param user: user name
    :param password: current password
    :return: auth token for use with :meth:`GraphDatabase.driver`
    """
    return AuthToken("basic", user, password)


def run(connection, statement, parameters=None):
    """ Run a Cypher statement on a given connection.

    :param connection: connection to carry the request and response
    :param statement: Cypher statement
    :param parameters: optional dictionary of parameters
    :return: statement result
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

    run_response = Response(connection)
    pull_all_response = Response(connection)
    result = StatementResult(connection, run_response, pull_all_response)
    result.statement = statement
    result.parameters = parameters

    connection.append(RUN, (statement, parameters), response=run_response)
    connection.append(PULL_ALL, response=pull_all_response)
    connection.send()

    return result


_warned_about_insecure_default = False


def _warn_about_insecure_default():
    global _warned_about_insecure_default
    if not SSL_AVAILABLE and not _warned_about_insecure_default:
        from warnings import warn
        warn("Bolt over TLS is only available in Python 2.7.9+ and Python 3.3+ "
             "so communications are not secure")
        _warned_about_insecure_default = True
