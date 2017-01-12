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

from neo4j.bolt.connection import connect, ConnectionPool, DEFAULT_PORT, \
    PULL_ALL, Response, RUN, ServiceUnavailable
from neo4j.compat import urlparse

from .api import GraphDatabase, Driver, Session, StatementResult, \
    READ_ACCESS, WRITE_ACCESS, \
    fix_statement, fix_parameters, \
    CypherError, SessionExpired, SessionError
from .routing import RoutingConnectionPool
from .security import SecurityPlan, Unauthorized
from .summary import ResultSummary
from .types import Record


class DirectDriver(Driver):
    """ A :class:`.DirectDriver` is created from a ``bolt`` URI and addresses
    a single database instance. This provides basic connectivity to any
    database service topology.
    """

    def __init__(self, uri, **config):
        parsed = urlparse(uri)
        self.address = (parsed.hostname, parsed.port or DEFAULT_PORT)
        self.security_plan = security_plan = SecurityPlan.build(**config)
        self.encrypted = security_plan.encrypted
        pool = ConnectionPool(lambda a: connect(a, security_plan.ssl_context, **config))
        Driver.__init__(self, pool)

    def session(self, access_mode=None):
        try:
            return BoltSession(self.pool.acquire(self.address))
        except ServiceUnavailable as error:
            if error.code == "Neo.ClientError.Security.Unauthorized":
                raise Unauthorized(error.args[0])
            raise


GraphDatabase.uri_schemes["bolt"] = DirectDriver


class RoutingDriver(Driver):
    """ A :class:`.RoutingDriver` is created from a ``bolt+routing`` URI. The
    routing behaviour works in tandem with Neo4j's causal clustering feature
    by directing read and write behaviour to appropriate cluster members.
    """

    def __init__(self, uri, **config):
        parsed = urlparse(uri)
        initial_address = (parsed.hostname, parsed.port or DEFAULT_PORT)
        self.security_plan = security_plan = SecurityPlan.build(**config)
        self.encrypted = security_plan.encrypted
        if not security_plan.routing_compatible:
            # this error message is case-specific as there is only one incompatible
            # scenario right now
            raise ValueError("TRUST_ON_FIRST_USE is not compatible with routing")

        def connector(a):
            return connect(a, security_plan.ssl_context, **config)

        pool = RoutingConnectionPool(connector, initial_address)
        try:
            pool.update_routing_table()
        except:
            pool.close()
            raise
        else:
            Driver.__init__(self, pool)

    def session(self, access_mode=None):
        if access_mode == READ_ACCESS:
            connection = self.pool.acquire_for_read()
        elif access_mode == WRITE_ACCESS:
            connection = self.pool.acquire_for_write()
        else:
            connection = self.pool.acquire_for_write()
        return BoltSession(connection, access_mode)


GraphDatabase.uri_schemes["bolt+routing"] = RoutingDriver


class BoltSession(Session):
    """ Logical session carried out over an established TCP connection.
    Sessions should generally be constructed using the :meth:`.Driver.session`
    method.
    """

    def __init__(self, connection, access_mode=None):
        self.connection = connection
        self.access_mode = access_mode

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        super(BoltSession, self).close()
        if self.connection:
            if not self.connection.closed:
                self.sync()
            self.connection.in_use = False
            self.connection = None

    def closed(self):
        return self.connection is None or self.connection.closed

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
        if not self.connection:
            raise SessionError("This session is closed.")

        self.last_bookmark = None

        statement = fix_statement(statement)
        parameters = fix_parameters(parameters, **kwparameters)

        run_response = Response(self.connection)
        pull_all_response = Response(self.connection)
        result = BoltStatementResult(self, run_response, pull_all_response)
        result.statement = statement
        result.parameters = parameters

        self.connection.append(RUN, (statement, parameters), response=run_response)
        self.connection.append(PULL_ALL, response=pull_all_response)
        self.connection.send()

        return result

    def fetch(self):
        """ Fetch the next message if available and return
        the number of messages fetched (one or zero).
        """
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

    def sync(self):
        self.connection.sync()

    def begin_transaction(self, bookmark=None):
        transaction = super(BoltSession, self).begin_transaction(bookmark)
        parameters = {}
        if bookmark is not None:
            parameters["bookmark"] = bookmark
        self.run("BEGIN", parameters)
        return transaction

    def commit_transaction(self):
        super(BoltSession, self).commit_transaction()
        result = self.run("COMMIT")
        self.sync()
        summary = result.summary()
        self.last_bookmark = summary.metadata.get("bookmark")

    def rollback_transaction(self):
        super(BoltSession, self).rollback_transaction()
        self.run("ROLLBACK")
        self.sync()


class BoltStatementResult(StatementResult):
    """ A handler for the result of Cypher statement execution.
    """

    error_class = CypherError

    value_system = GraphDatabase.value_systems["packstream"]

    zipper = Record

    def __init__(self, session, run_response, pull_all_response):
        super(BoltStatementResult, self).__init__(session)

        all_metadata = {}

        def on_header(metadata):
            # Called on receipt of the result header.
            all_metadata.update(metadata)
            self._keys = tuple(metadata["fields"])

        def on_record(values):
            # Called on receipt of each result record.
            self._records.append(values)

        def on_footer(metadata):
            # Called on receipt of the result footer.
            all_metadata.update(metadata)
            self._summary = ResultSummary(self.statement, self.parameters, **all_metadata)
            self._session = None

        def on_failure(metadata):
            # Called on execution failure.
            self._session.connection.acknowledge_failure()
            self._session = None
            raise self.error_class(metadata)

        run_response.on_success = on_header
        run_response.on_failure = on_failure

        pull_all_response.on_record = on_record
        pull_all_response.on_success = on_footer
        pull_all_response.on_failure = on_failure
