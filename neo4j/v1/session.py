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

from collections import deque

from neo4j.bolt.connection import Response, RUN, PULL_ALL, ServiceUnavailable
from neo4j.compat import integer, string

from .summary import ResultSummary
from .types import hydrated


class Session(object):
    """ Logical session carried out over an established TCP connection.
    Sessions should generally be constructed using the :meth:`.Driver.session`
    method.
    """

    transaction = None

    last_bookmark = None

    def __init__(self, connection, access_mode=None):
        self.connection = connection
        self.access_mode = access_mode

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def run(self, statement, parameters=None, **kwparameters):
        """ Run a parameterised Cypher statement. If an explicit transaction
        has been created, the statement will be executed within that
        transactional context. Otherwise, this will take place within an
        auto-commit transaction.

        :param statement: Cypher statement to execute
        :param parameters: dictionary of parameters
        :return: Cypher result
        :rtype: :class:`.StatementResult`
        """
        self.last_bookmark = None

        statement = _norm_statement(statement)
        parameters = _norm_parameters(parameters, **kwparameters)

        run_response = Response(self.connection)
        pull_all_response = Response(self.connection)
        result = StatementResult(self, run_response, pull_all_response)
        result.statement = statement
        result.parameters = parameters

        self.connection.append(RUN, (statement, parameters), response=run_response)
        self.connection.append(PULL_ALL, response=pull_all_response)
        self.connection.send()

        return result

    def fetch(self):
        try:
            return self.connection.fetch()
        except ServiceUnavailable as cause:
            self.connection.in_use = False
            self.connection = None
            if self.access_mode:
                exception = SessionExpired(self, "Session %r is no longer valid for "
                                           "%r work" % (self, self.access_mode))
                exception.__cause__ = cause
                raise exception
            else:
                raise

    def close(self):
        """ Close the session.
        """
        if self.transaction:
            self.transaction.close()
        if self.connection:
            if not self.connection.closed:
                self.connection.sync()
            self.connection.in_use = False
            self.connection = None

    def begin_transaction(self, bookmark=None):
        """ Create a new :class:`.Transaction` within this session.

        :param bookmark: a bookmark to which the server should
                         synchronise before beginning the transaction
        :return: new :class:`.Transaction` instance.
        """
        if self.transaction:
            raise TransactionError("Explicit transaction already open")

        def clear_transaction():
            self.transaction = None

        parameters = {}
        if bookmark is not None:
            parameters["bookmark"] = bookmark

        self.run("BEGIN", parameters)
        self.transaction = Transaction(self, on_close=clear_transaction)
        return self.transaction

    def commit_transaction(self):
        result = self.run("COMMIT")
        self.connection.sync()
        summary = result.summary()
        self.last_bookmark = summary.metadata.get("bookmark")

    def rollback_transaction(self):
        self.run("ROLLBACK")
        self.connection.sync()


class Transaction(object):
    """ Container for multiple Cypher queries to be executed within
    a single context. Transactions can be used within a :py:const:`with`
    block where the value of :attr:`.success` will determine whether
    the transaction is committed or rolled back on :meth:`.Transaction.close`::

        with session.begin_transaction() as tx:
            pass

    """

    #: When closed, the transaction will be committed if marked as successful
    #: and rolled back otherwise. This attribute can be set in user code
    #: multiple times before a transaction completes with only the final
    #: value taking effect.
    success = None

    #: Indicator to show whether the transaction has been closed, either
    #: with commit or rollback.
    closed = False

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

        :param statement: Cypher statement
        :param parameters: dictionary of parameters
        :return: result object
        """
        assert not self.closed
        return self.session.run(statement, parameters, **kwparameters)

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
            self.session.commit_transaction()
        else:
            self.session.rollback_transaction()
        self.closed = True
        self.on_close()


class StatementResult(object):
    """ A handler for the result of Cypher statement execution.
    """

    #: The statement text that was executed to produce this result.
    statement = None

    #: Dictionary of parameters passed with the statement.
    parameters = None

    def __init__(self, session, run_response, pull_all_response):
        super(StatementResult, self).__init__()

        # The Session behind this result. When all data has been
        # received, this is set to :const:`None` and can therefore
        # be used as a "consumed" indicator.
        self.session = session

        # The keys for the records in the result stream. These are
        # lazily populated on request.
        self._keys = None

        # Buffer for incoming records to be queued before yielding. If
        # the result is used immediately, this buffer will be ignored.
        self._buffer = deque()

        # The result summary (populated after the records have been
        # fully consumed).
        self._summary = None

        def on_header(metadata):
            # Called on receipt of the result header.
            self._keys = metadata["fields"]

        def on_record(values):
            # Called on receipt of each result record.
            self._buffer.append(values)

        def on_footer(metadata):
            # Called on receipt of the result footer.
            self._summary = ResultSummary(self.statement, self.parameters, **metadata)
            self.session = None

        def on_failure(metadata):
            # Called on execution failure.
            self.session.connection.acknowledge_failure()
            self.session = None
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
        while self.online():
            self.session.fetch()
            while self._buffer:
                values = self._buffer.popleft()
                yield Record(self.keys(), tuple(map(hydrated, values)))

    def online(self):
        """ True if this result is still attached to an active Session.
        """
        return self.session and not self.session.connection.closed

    def keys(self):
        """ Return the keys for the records.
        """
        # Fetch messages until we have the header or a failure
        while self._keys is None and self.online():
            self.session.fetch()
        return tuple(self._keys)

    def buffer(self):
        """ Fetch the remainder of the result from the network and buffer
        it for future consumption.
        """
        while self.online():
            self.session.fetch()

    def consume(self):
        """ Consume the remainder of this result and return the summary.
        """
        if self.online():
            list(self)
        return self._summary

    def summary(self):
        """ Return the summary, buffering any remaining records.
        """
        self.buffer()
        return self._summary

    def single(self):
        """ Return the next record, failing if none or more than one remain.
        """
        records = list(self)
        num_records = len(records)
        if num_records == 0:
            raise ResultError("Cannot retrieve a single record, because this result is empty.")
        elif num_records != 1:
            raise ResultError("Expected a result with a single record, but this result contains "
                              "at least one more.")
        else:
            return records[0]

    def peek(self):
        """ Return the next record without advancing the cursor. Fails
        if no records remain.
        """
        if self._buffer:
            values = self._buffer[0]
            return Record(self.keys(), tuple(map(hydrated, values)))
        while not self._buffer and self.online():
            self.session.fetch()
            if self._buffer:
                values = self._buffer[0]
                return Record(self.keys(), tuple(map(hydrated, values)))
        raise ResultError("End of stream")


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


def _norm_statement(statement):
    if isinstance(statement, bytes):
        statement = statement.decode("UTF-8")
    return statement


def _norm_parameters(parameters=None, **kwparameters):
    params_in = parameters or {}
    params_in.update(kwparameters)
    params_out = {}
    for key, value in params_in.items():
        if isinstance(key, bytes):
            key = key.decode("UTF-8")
        if isinstance(value, bytes):
            params_out[key] = value.decode("UTF-8")
        else:
            params_out[key] = value
    return params_out


class CypherError(Exception):
    """ Raised when the Cypher engine returns an error to the client.
    """

    code = None
    message = None

    def __init__(self, data):
        super(CypherError, self).__init__(data.get("message"))
        for key, value in data.items():
            if not key.startswith("_"):
                setattr(self, key, value)


class TransactionError(Exception):
    """ Raised when an error occurs while using a transaction.
    """


class ResultError(Exception):
    """ Raised when an error occurs while consuming a result.
    """


class SessionExpired(Exception):
    """ Raised when no a session is no longer able to fulfil
    its purpose.
    """

    def __init__(self, session, *args, **kwargs):
        self.session = session
        super(SessionExpired, self).__init__(*args, **kwargs)
