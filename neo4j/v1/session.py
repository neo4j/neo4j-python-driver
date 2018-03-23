#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2018 "Neo Technology,"
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
from neo4j.compat import ustr
from neo4j.v1.api import Session
from neo4j.v1.exceptions import SessionError
from neo4j.v1.result import BoltStatementResult
from neo4j.v1.types import PackStreamDehydrator


def _fix_parameters(parameters, protocol_version, **kwargs):
    if not parameters:
        return {}
    dehydrator = PackStreamDehydrator(protocol_version, **kwargs)
    try:
        dehydrated, = dehydrator.dehydrate([parameters])
    except TypeError as error:
        value = error.args[0]
        raise TypeError("Parameters of type {} are not supported".format(type(value).__name__))
    else:
        return dehydrated


class BoltSession(Session):

    def _run(self, statement, parameters):
        if self.closed():
            raise SessionError("Session closed")

        run_response = Response(self._connection)
        pull_all_response = Response(self._connection)
        self._last_result = result = BoltStatementResult(self, run_response, pull_all_response)
        result.statement = ustr(statement)
        result.parameters = _fix_parameters(parameters, self._connection.protocol_version,
                                            supports_bytes=self._connection.server.supports_bytes())

        self._connection.append(RUN, (result.statement, result.parameters), response=run_response)
        self._connection.append(PULL_ALL, response=pull_all_response)

        return result

    def __run__(self, statement, parameters):
        return self._run(statement, parameters)

    def __begin__(self):
        if self._bookmarks:
            parameters = {"bookmark": self.last_bookmark(), "bookmarks": self._bookmarks}
        else:
            parameters = {}
        return self.__run__(u"BEGIN", parameters)

    def __commit__(self):
        return self.__run__(u"COMMIT", {})

    def __rollback__(self):
        return self.__run__(u"ROLLBACK", {})

    def __bookmark__(self, result):
        summary = result.summary()
        return summary.metadata.get("bookmark")
