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
from socket import getaddrinfo, gaierror, SOCK_STREAM, IPPROTO_TCP

from neo4j.compat import urlparse
from neo4j.exceptions import AddressError

try:
    from urllib.parse import parse_qs
except ImportError:
    from urlparse import parse_qs


VALID_IPv4_SEGMENTS = [str(i).encode("latin1") for i in range(0x100)]
VALID_IPv6_SEGMENT_CHARS = b"0123456789abcdef"


def is_ipv4_address(string):
    if not isinstance(string, bytes):
        string = str(string).encode("latin1")
    segments = string.split(b".")
    return len(segments) == 4 and all(segment in VALID_IPv4_SEGMENTS for segment in segments)


def is_ipv6_address(string):
    if not isinstance(string, bytes):
        string = str(string).encode("latin1")
    segments = string.lower().split(b":")
    return 3 <= len(segments) <= 8 and all(all(c in VALID_IPv6_SEGMENT_CHARS for c in segment) for segment in segments)


def is_ip_address(string):
    return is_ipv4_address(string) or is_ipv6_address(string)


IPv4SocketAddress = namedtuple("Address", ["host", "port"])
IPv6SocketAddress = namedtuple("Address", ["host", "port", "flow_info", "scope_id"])


class SocketAddress(object):

    @classmethod
    def from_socket(cls, socket):
        address = socket.getpeername()
        if len(address) == 2:
            return IPv4SocketAddress(*address)
        elif len(address) == 4:
            return IPv6SocketAddress(*address)
        else:
            raise ValueError("Unsupported address {!r}".format(address))

    @classmethod
    def from_uri(cls, uri, default_port=0):
        parsed = urlparse(uri)
        if parsed.netloc.startswith("["):
            return IPv6SocketAddress(parsed.hostname, parsed.port or default_port, 0, 0)
        else:
            return IPv4SocketAddress(parsed.hostname, parsed.port or default_port)

    @classmethod
    def parse(cls, string, default_port=0):
        """ Parse a string address, such as 'localhost:1234' or
        '[::1]:7687'.
        """
        return cls.from_uri("//{}".format(string), default_port)

    @classmethod
    def parse_routing_context(cls, uri):
        query = urlparse(uri).query
        if not query:
            return {}

        context = {}
        parameters = parse_qs(query, True)
        for key in parameters:
            value_list = parameters[key]
            if len(value_list) != 1:
                raise ValueError("Duplicated query parameters with key '%s', value '%s' found in URL '%s'" % (key, value_list, uri))
            value = value_list[0]
            if not value:
                raise ValueError("Invalid parameters:'%s=%s' in URI '%s'." % (key, value, uri))
            context[key] = value
        return context


def resolve(socket_address):
    try:
        return [address for _, _, _, _, address in
                getaddrinfo(socket_address[0], socket_address[1], 0, SOCK_STREAM, IPPROTO_TCP)]
    except gaierror:
        raise AddressError("Cannot resolve address {!r}".format(socket_address[0]))
