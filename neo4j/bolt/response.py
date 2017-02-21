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


from neo4j.exceptions import AuthError, ServiceUnavailable, ProtocolError


class Response(object):
    """ Subscriber object for a full response (zero or
    more detail messages followed by one summary message).
    """

    def __init__(self, connection):
        self.connection = connection
        self.complete = False

    def on_records(self, records):
        """ Called when one or more RECORD messages have been received.
        """

    def on_success(self, metadata):
        """ Called when a SUCCESS message has been received.
        """

    def on_failure(self, metadata):
        """ Called when a FAILURE message has been received.
        """

    def on_ignored(self, metadata=None):
        """ Called when an IGNORED message has been received.
        """


class InitResponse(Response):

    def on_success(self, metadata):
        super(InitResponse, self).on_success(metadata)
        self.connection.server.version = metadata.get("server")

    def on_failure(self, metadata):
        code = metadata.get("code")
        message = metadata.get("message", "Connection initialisation failed")
        if code == "Neo.ClientError.Security.Unauthorized":
            raise AuthError(message)
        else:
            raise ServiceUnavailable(message)


class AckFailureResponse(Response):

    def on_failure(self, metadata):
        raise ProtocolError("ACK_FAILURE failed")


class ResetResponse(Response):

    def on_failure(self, metadata):
        raise ProtocolError("RESET failed")
