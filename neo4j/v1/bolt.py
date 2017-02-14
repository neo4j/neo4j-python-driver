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


from neo4j.bolt import PULL_ALL, Response, RUN

from .api import GraphDatabase, Session, StatementResult, \
    fix_statement, fix_parameters
from .exceptions import CypherError, SessionError
from .summary import ResultSummary
from .types import Record


class BoltSession(Session):

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

        if not self.transaction:
            self.connection.sync()

        return result

    def send(self):
        self.connection.send()

    def fetch(self):
        detail_count, _ = self.connection.fetch()
        return detail_count

    def sync(self):
        detail_count, _ = self.connection.sync()
        return detail_count

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
        return self.last_bookmark

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

        def on_records(records):
            # Called on receipt of one or more result records.
            self._records.extend(records)

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

        pull_all_response.on_records = on_records
        pull_all_response.on_success = on_footer
        pull_all_response.on_failure = on_failure
