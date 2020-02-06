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


""" Internal module for all Bolt exception classes.
"""


from os import strerror


class BoltError(Exception):
    """ Base class for all Bolt protocol errors.
    """

    def __init__(self, message, address):
        super().__init__(message)
        self.address = address


class BoltConnectionError(BoltError):
    """ Raised when a connection fails.
    """

    def __init__(self, message, address):
        msg = (
        "Connection Failed. "
        "Please ensure that your database is listening on the correct host and port and that you have enabled encryption if required. "
        "Note that the default encryption setting has changed in Neo4j 4.0. See the docs for more information. "
        "{}")

        super().__init__(msg.format(message), address)

    def __str__(self):
        s = super().__str__()
        errno = self.errno
        if errno:
            s += " (code {}: {})".format(errno, strerror(errno))
        return s

    @property
    def errno(self):
        try:
            return self.__cause__.errno
        except AttributeError:
            return None


class BoltSecurityError(BoltConnectionError):
    """ Raised when a connection fails for security reasons.
    """

    def __str__(self):
        return "[{}] {}".format(self.__cause__.__class__.__name__, super().__str__())


class BoltConnectionBroken(BoltConnectionError):
    """ Raised when an established connection is lost or when an
    attempt is made to use a connection that has previously broken.
    """

    # TODO: add details of outstanding commits (if any), plus maybe other requests outstanding


class BoltConnectionClosed(BoltConnectionError):
    """ Raised when an attempt is made to use a connection that has
    been closed locally.
    """


class BoltHandshakeError(BoltError):
    """ Raised when a handshake completes unsuccessfully.
    """

    def __init__(self, message, address, request_data, response_data):
        super().__init__(message, address)
        self.request_data = request_data
        self.response_data = response_data


class BoltTransactionError(BoltError):
    """ Raised when a fault occurs with a transaction, or when a
    transaction is used incorrectly.
    """
    # TODO: pass the transaction object in as an argument


class BoltRoutingError(BoltError):
    """ Raised when a fault occurs with obtaining a routing table.
    """


class BoltFailure(BoltError):
    """ Holds a Cypher failure.
    """

    #:
    code = None

    #:
    classification = None

    #:
    category = None

    #:
    title = None

    #: Flag to indicate whether an error is safe to retry or not. False
    #: (not retryable) by default. This can be overridden by instances
    #: or subclasses.
    transient = False

    @classmethod
    def _find_subclass(cls, predicate, default=None):
        if hasattr(cls, "__subclasses__"):
            for subclass in cls.__subclasses__():
                if predicate(subclass):
                    return subclass
        return default

    def __new__(cls, message, address, code, response):
        code_parts = code.split(".")
        classification = code_parts[1]
        c1 = cls._find_subclass(lambda k: k.__name__ == classification, cls)
        assert issubclass(c1, cls)
        c2 = c1._find_subclass(lambda k: k.code == code, c1)
        assert issubclass(c2, c1)
        return super().__new__(c2, message, address)

    def __init__(self, message, address, code, response):
        super().__init__(message, address)
        self.code = code
        code_parts = self.code.split(".")
        self.classification = code_parts[1]
        self.category = code_parts[2]
        self.title = code_parts[3]
        self.response = response

    def __str__(self):
        return "[{}.{}] {}".format(self.category, self.title, super().__str__())

    @property
    def result(self):
        """ The Result object to which this failure is attached (if any).
        """
        try:
            return self.response.result
        except AttributeError:
            return None

    @property
    def transaction(self):
        try:
            return self.result.transaction
        except AttributeError:
            return None


class BoltIncompleteCommitError(BoltError):
    """ Raised when a disconnection occurs while still waiting for a commit
    response. For non-idempotent write transactions, this leaves the data
    in an unknown state with regard to whether the transaction completed
    successfully or not.
    """


class BoltProtocolError(BoltError):
    """ Raised when an unexpected or unsupported protocol event occurs.
    """