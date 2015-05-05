#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2015 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
#
# This file is part of Neo4j.
#
# Neo4j is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

from neo4j.socketsession import *
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
