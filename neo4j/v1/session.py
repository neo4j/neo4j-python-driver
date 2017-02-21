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


from neo4j.bolt import Response, RUN, PULL_ALL
from neo4j.v1.api import Session, fix_statement, fix_parameters
from neo4j.v1.result import BoltStatementResult


class BoltSession(Session):

    _connection = None

    _last_result = None

    def __init__(self, connector):
        self._connector = connector

    def _connect(self):
        if not self._connection:
            self._connection = self._connector()

    def _disconnect(self):
        if self._connection:
            self._connection.in_use = False
            self._connection = None

    def close(self):
        super(BoltSession, self).close()
        if self._connection:
            self._connection.sync()
            self._disconnect()

    def run(self, statement, parameters=None, **kwparameters):
        super(BoltSession, self).run(statement, parameters, **kwparameters)
        self._connect()

        statement = fix_statement(statement)
        parameters = fix_parameters(parameters, **kwparameters)

        run_response = Response(self._connection)
        pull_all_response = Response(self._connection)
        self._last_result = result = BoltStatementResult(self, run_response, pull_all_response)
        result.statement = statement
        result.parameters = parameters

        self._connection.append(RUN, (statement, parameters), response=run_response)
        self._connection.append(PULL_ALL, response=pull_all_response)

        if not self.transaction:
            self._connection.send()
            self._connection.fetch()

        return result

    def send(self):
        if self._connection:
            self._connection.send()

    def fetch(self):
        if self._connection:
            detail_count, _ = self._connection.fetch()
            return detail_count
        else:
            return 0

    def sync(self):
        if self._connection:
            detail_count, _ = self._connection.sync()
            return detail_count
        else:
            return 0

    def detach(self, result):
        count = super(BoltSession, self).detach(result)
        if self._last_result is result:
            self._last_result = None
            if not self.transaction:
                self._disconnect()
        return count

    def begin_transaction(self, bookmark=None):
        transaction = super(BoltSession, self).begin_transaction(bookmark)
        parameters = {}
        if self.bookmark is not None:
            parameters["bookmark"] = self.bookmark
        self.run("BEGIN", parameters)
        return transaction

    def commit_transaction(self):
        super(BoltSession, self).commit_transaction()
        summary = self.run("COMMIT").summary()
        self.bookmark = summary.metadata.get("bookmark")
        return self.bookmark

    def rollback_transaction(self):
        super(BoltSession, self).rollback_transaction()
        self.run("ROLLBACK").consume()
