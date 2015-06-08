#!/usr/bin/env python
#! -*- encoding: UTF-8 -*-

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


try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

from neo4j.session import *
from neo4j.v1 import *
from neo4j.v1.types import *


SocketSession.subclasses = [SocketSessionV1, None, None, None]


def session(url):
    """ Return a Session object appropriate for the URL specified.

    :param url: the URL of the database server to which to connect
    :return: a Session subclass instance for a supported protocol version
    """
    parsed = urlparse(url)
    scheme = parsed.scheme
    if scheme == "neo4j":
        return SocketSession.create(parsed.hostname, parsed.port)
    else:
        raise ValueError("Unsupported URL scheme %r" % scheme)
