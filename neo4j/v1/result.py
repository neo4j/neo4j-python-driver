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


from collections import namedtuple

from neo4j.exceptions import CypherError
from neo4j.v1.api import GraphDatabase, StatementResult
from neo4j.v1.types import Record


STATEMENT_TYPE_READ_ONLY = "r"
STATEMENT_TYPE_READ_WRITE = "rw"
STATEMENT_TYPE_WRITE_ONLY = "w"
STATEMENT_TYPE_SCHEMA_WRITE = "s"


class BoltStatementResult(StatementResult):
    """ A handler for the result of Cypher statement execution.
    """

    value_system = GraphDatabase.value_systems["packstream"]

    zipper = Record

    def __init__(self, session, run_response, pull_all_response):
        super(BoltStatementResult, self).__init__(session)

        all_metadata = {}

        def on_header(metadata):
            # Called on receipt of the result header.
            all_metadata.update(metadata)
            self._keys = tuple(metadata.get("fields", ()))

        def on_records(records):
            # Called on receipt of one or more result records.
            self._records.extend(records)

        def on_footer(metadata):
            # Called on receipt of the result footer.
            all_metadata.update(metadata, statement=self.statement, parameters=self.parameters,
                                server=self._session._connection.server)
            self._summary = BoltStatementResultSummary(**all_metadata)
            self._session, session_ = None, self._session
            session_.detach(self)

        def on_failure(metadata):
            # Called on execution failure.
            self._session._connection.acknowledge_failure()
            on_footer(metadata)
            raise CypherError.hydrate(**metadata)

        run_response.on_success = on_header
        run_response.on_failure = on_failure

        pull_all_response.on_records = on_records
        pull_all_response.on_success = on_footer
        pull_all_response.on_failure = on_failure


class BoltStatementResultSummary(object):
    """ A summary of execution returned with a :class:`.StatementResult` object.
    """

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
        self.server = metadata.get("server")
        self.statement = metadata.get("statement")
        self.parameters = metadata.get("parameters")
        self.statement_type = metadata.get("type")
        self.counters = SummaryCounters(metadata.get("stats", {}))
        self.result_available_after = metadata.get("result_available_after")
        self.result_consumed_after = metadata.get("result_consumed_after")
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
