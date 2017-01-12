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
from warnings import warn

from neo4j.bolt import ProtocolError, ServiceUnavailable
from neo4j.compat import urlparse


READ_ACCESS = "READ"
WRITE_ACCESS = "WRITE"
DEFAULT_ACCESS = WRITE_ACCESS


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

    pool = None

    def __init__(self, pool):
        self.pool = pool

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def session(self, access_mode=None):
        """ Create a new session using a connection from the driver connection
        pool. Session creation is a lightweight operation and sessions are
        not thread safe, therefore a session should generally be short-lived
        within a single thread.

        :param access_mode:
        :return: new :class:`.Session` object
        """
        pass

    def close(self):
        """ Shut down, closing any open connections that were spawned by
        this Driver.
        """
        if self.pool:
            self.pool.close()
            self.pool = None


class Session(object):
    """ Logical session carried out over an established TCP connection.
    Sessions should generally be constructed using the :meth:`.Driver.session`
    method.
    """

    transaction = None

    last_bookmark = None

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        """ Close the session.
        """
        if self.transaction:
            try:
                self.rollback_transaction()
            except (CypherError, TransactionError, ServiceUnavailable):
                pass

    def closed(self):
        """ Return true if the session is closed, false otherwise.
        """

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

    def fetch(self):
        """ Fetch the next message if available and return
        the number of messages fetched.
        """
        return 0

    def sync(self):
        """ Full send and receive. Return the total number
        of records received.
        """
        return 0

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

        self.transaction = Transaction(self, on_close=clear_transaction)
        return self.transaction

    def commit_transaction(self):
        if not self.transaction:
            raise TransactionError("No transaction to commit")
        self.transaction = None

    def rollback_transaction(self):
        if not self.transaction:
            raise TransactionError("No transaction to rollback")
        self.transaction = None


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

        :param statement: Cypher statement
        :param parameters: dictionary of parameters
        :return: result object
        """
        if self.closed():
            raise TransactionError("Cannot use closed transaction")
        return self.session.run(statement, parameters, **kwparameters)

    def sync(self):
        if self.closed():
            raise TransactionError("Cannot use closed transaction")
        self.session.sync()

    def commit(self):
        """ Mark this transaction as successful and close in order to
        trigger a COMMIT.
        """
        if self.closed():
            raise TransactionError("Cannot use closed transaction")
        self.success = True
        self.close()

    def rollback(self):
        """ Mark this transaction as unsuccessful and close in order to
        trigger a ROLLBACK.
        """
        if self.closed():
            raise TransactionError("Cannot use closed transaction")
        self.success = False
        self.close()

    def close(self):
        """ Close this transaction, triggering either a COMMIT or a ROLLBACK.
        """
        if not self.closed():
            if self.success:
                self.session.commit_transaction()
            else:
                self.session.rollback_transaction()
            self._closed = True
            self.on_close()

    def closed(self):
        return self._closed


class StatementResult(object):
    """ A handler for the result of Cypher statement execution.
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

    def online(self):
        """ True if this result is still attached to an active Session.
        """
        return self._session and not self._session.closed()

    def fetch(self):
        """ Fetch another record, if available. Return the number fetched.
        """
        if self.online():
            return self._session.fetch()
        else:
            return 0

    def buffer(self):
        """ Fetch the remainder of the result from the network and buffer
        it for future consumption.
        """
        while self.online():
            self.fetch()

    def keys(self):
        """ Return the keys for the records.
        """
        while self._keys is None and self.online():
            self.fetch()
        return self._keys

    def records(self):
        """ Yield records.

        :return:
        """
        hydrate = self.value_system.hydrate
        zipper = self.zipper
        keys = self.keys()
        records = self._records
        while records:
            values = records.popleft()
            yield zipper(keys, hydrate(values))
        while self.online():
            self.fetch()
            while records:
                values = records.popleft()
                yield zipper(keys, hydrate(values))

    def summary(self):
        """ Return the summary, buffering any remaining records.
        """
        self.buffer()
        return self._summary

    def consume(self):
        """ Consume the remainder of this result and return the summary.
        """
        if self.online():
            list(self)
        return self.summary()

    def single(self):
        """ Return the next record, failing if none or more than one remain.
        """
        records = list(self)
        size = len(records)
        if size == 0:
            return None
        if size != 1:
            warn("Expected a result with a single record, but this result contains %d" % size)
        return records[0]

    def peek(self):
        """ Return the next record without advancing the cursor. Returns
        :const:`None` if no records remain.
        """
        hydrate = self.value_system.hydrate
        zipper = self.zipper
        keys = self.keys()
        records = self._records
        if records:
            values = records[0]
            return zipper(keys, hydrate(values))
        while not records and self.online():
            self.fetch()
            if records:
                values = records[0]
                return zipper(keys, hydrate(values))
        return None


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


class SessionError(Exception):
    """ Raised when an error occurs while using a session.
    """


class TransactionError(Exception):
    """ Raised when an error occurs while using a transaction.
    """


class SessionExpired(SessionError):
    """ Raised when no a session is no longer able to fulfil
    its purpose.
    """

    def __init__(self, session, *args, **kwargs):
        self.session = session
        super(SessionExpired, self).__init__(*args, **kwargs)


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
        if isinstance(value, bytes):
            params_out[key] = value.decode("UTF-8")
        else:
            params_out[key] = value
    return params_out
