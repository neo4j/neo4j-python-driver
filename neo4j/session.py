#!/usr/bin/env python
# -*- encoding: UTF-8 -*-

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

import logging

from .compat import integer, perf_counter, string, urlparse
from .connection import connect
from .exceptions import CypherError, ProtocolError
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


class Record(object):
    """ Record object for storing result values along with field names.
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


class Session(object):
    """ Logical session carried out over an established TCP connection.
    Sessions should generally be constructed using the :meth:`.Driver.session`
    method.
    """

    def __init__(self, connection):
        self.connection = connection
        self.connection.init("neo4j-python/0.0")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def run(self, statement, parameters=None):
        """ Run a parameterised Cypher statement.
        """
        # Collect six checkpoint times for each run
        # INIT | START_SEND | END_SEND | START_RECV | END_RECV | RETURN
        t = [perf_counter(), None, None, None, None, None]

        recv_message = self.connection.recv_message

        # Ensure the statement is a Unicode value
        if isinstance(statement, bytes):
            statement = statement.decode("UTF-8")

        parameters = dict(parameters or {})
        if __debug__:
            log_info("C: RUN %r %r", statement, parameters)
            log_info("C: PULL_ALL")
        t[1], t[2] = self.connection.send_messages((RUN, (statement, parameters)),
                                                   (PULL_ALL, ()))

        signature, (data,) = recv_message(t)
        if signature == SUCCESS:
            fields = data["fields"]
            if __debug__: log_info("S: SUCCESS %r", data)
        else:
            if __debug__: log_info("S: FAILURE %r", data)
            raise CypherError(data)

        records = []
        append = records.append
        more = True
        while more:
            signature, (data,) = recv_message()
            if signature == RECORD:
                if __debug__: log_info("S: RECORD %r", data)
                append(Record(fields, tuple(map(hydrated, data))))
            elif signature == SUCCESS:
                if __debug__: log_info("S: SUCCESS %r", data)
                more = False
            else:
                if __debug__: log_info("S: FAILURE %r", data)
                raise CypherError(data)

        self.connection.append_times(t)

        return records

    def close(self):
        """ Shut down and close the connection.
        """
        self.connection.close()


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
