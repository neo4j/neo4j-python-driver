#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2020 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
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

BOLT_VERSION_1 = 1
BOLT_VERSION_2 = 2
BOLT_VERSION_3 = 3
BOLT_VERSION_4 = 4

# TODO: This logic should be inside the Bolt subclasses, because it can change depending on Bolt Protocol Version.


class ResultSummary:
    """ A summary of execution returned with a :class:`.Result` object.
    """

    #: A :class:`neo4j.ServerInfo` instance. Provides some basic information of the server where the result is obtained from.
    server = None

    #: The database name where this summary is obtained from.
    database = None

    #: The query that was executed to produce this result.
    query = None

    #: Dictionary of parameters passed with the statement.
    parameters = None

    #: A string that describes the type of query (``'r'`` = read-only, ``'rw'`` = read/write).
    query_type = None

    #: A :class:`neo4j.SummaryCounters` instance. Counters for operations the query triggered.
    counters = None

    #: Dictionary that describes how the database will execute the query.
    plan = None

    #: Dictionary that describes how the database executed the query.
    profile = None

    #: The time it took for the server to have the result available. (milliseconds)
    result_available_after = None

    #: The time it took for the server to consume the result. (milliseconds)
    result_consumed_after = None

    #: A list of Dictionaries containing notification information.
    #: Notifications provide extra information for a user executing a statement.
    #: They can be warnings about problematic queries or other valuable information that can be
    #: presented in a client.
    #: Unlike failures or errors, notifications do not affect the execution of a statement.
    notifications = None

    def __init__(self, **metadata):
        self.metadata = metadata
        self.server = metadata.get("server")
        self.database = metadata.get("db")
        self.query = metadata.get("query")
        self.parameters = metadata.get("parameters")
        self.query_type = metadata.get("type")
        self.plan = metadata.get("plan")
        self.profile = metadata.get("profile")
        self.notifications = metadata.get("notifications")
        self.counters = SummaryCounters(metadata.get("stats", {}))
        if self.server.protocol_version[0] < BOLT_VERSION_3:
            self.result_available_after = metadata.get("result_available_after")
            self.result_consumed_after = metadata.get("result_consumed_after")
        else:
            self.result_available_after = metadata.get("t_first")
            self.result_consumed_after = metadata.get("t_last")


class SummaryCounters:
    """ Contains counters for various operations that a query triggered.
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

    #:
    system_updates = 0

    def __init__(self, statistics):
        for key, value in dict(statistics).items():
            key = key.replace("-", "_")
            setattr(self, key, value)

    def __repr__(self):
        return repr(vars(self))

    @property
    def contains_updates(self):
        """True if any of the counters except for system_updates, are greater than 0. Otherwise False."""
        return bool(self.nodes_created or self.nodes_deleted or
                    self.relationships_created or self.relationships_deleted or
                    self.properties_set or self.labels_added or self.labels_removed or
                    self.indexes_added or self.indexes_removed or
                    self.constraints_added or self.constraints_removed)

    @property
    def contains_system_updates(self):
        """True if the system database was updated, otherwise False."""
        return self.system_updates > 0
