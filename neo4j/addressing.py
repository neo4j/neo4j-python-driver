#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2019 "Neo4j,"
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


from socket import (
    getaddrinfo,
    getservbyname,
    SOCK_STREAM,
    AF_INET,
    AF_INET6,
)


class Address(tuple):

    @classmethod
    def from_socket(cls, socket):
        address = socket.getpeername()
        return cls(address)

    @classmethod
    def parse(cls, s, default_host=None, default_port=None):
        if not isinstance(s, str):
            raise TypeError("Address.parse requires a string argument")
        if s.startswith("["):
            # IPv6
            host, _, port = s[1:].rpartition("]")
            port = port.lstrip(":")
            try:
                port = int(port)
            except (TypeError, ValueError):
                pass
            return cls((host or default_host or "localhost",
                        port or default_port or 0, 0, 0))
        else:
            # IPv4
            host, _, port = s.partition(":")
            try:
                port = int(port)
            except (TypeError, ValueError):
                pass
            return cls((host or default_host or "localhost",
                        port or default_port or 0))

    @classmethod
    def parse_list(cls, *s, default_host=None, default_port=None):
        """ Parse a string containing one or more socket addresses, each
        separated by whitespace.
        """
        if not all(isinstance(s0, str) for s0 in s):
            raise TypeError("Address.parse_list requires a string argument")
        return [Address.parse(a, default_host, default_port)
                for a in " ".join(s).split()]

    def __new__(cls, iterable):
        if type(iterable) is cls:
            return cls
        n_parts = len(iterable)
        inst = tuple.__new__(cls, iterable)
        if n_parts == 2:
            inst.__class__ = IPv4Address
        elif n_parts == 4:
            inst.__class__ = IPv6Address
        else:
            raise ValueError("Addresses must consist of either "
                             "two parts (IPv4) or four parts (IPv6)")
        return inst

    #: Address family (AF_INET or AF_INET6)
    family = None

    def __repr__(self):
        return "{}({!r})".format(self.__class__.__name__, tuple(self))

    @property
    def host(self):
        return self[0]

    @property
    def port(self):
        return self[1]

    def resolve(self, family=0):
        # TODO: custom resolver argument
        try:
            info = getaddrinfo(self.host, self.port, family, SOCK_STREAM)
        except OSError:
            raise ValueError("Cannot resolve address {}".format(self))
        else:
            resolved = []
            for fam, _, _, _, addr in info:
                if fam == AF_INET6 and addr[3] != 0:
                    # skip any IPv6 addresses with a non-zero scope id
                    # as these appear to cause problems on some platforms
                    continue
                if addr not in resolved:
                    resolved.append(Address(addr))
            return resolved

    @property
    def port_number(self):
        try:
            return getservbyname(self[1])
        except (OSError, TypeError):
            # OSError: service/proto not found
            # TypeError: getservbyname() argument 1 must be str, not X
            try:
                return int(self[1])
            except (TypeError, ValueError) as e:
                raise type(e)("Unknown port value %r" % self[1])


class IPv4Address(Address):

    family = AF_INET

    def __str__(self):
        return "{}:{}".format(*self)


class IPv6Address(Address):

    family = AF_INET6

    def __str__(self):
        return "[{}]:{}".format(*self)


# TODO: deprecate
class AddressList(list):
    """ A list of socket addresses, each as a tuple of the format expected by
    the built-in `socket.connect` method.
    """

    @classmethod
    def parse(cls, s, default_host=None, default_port=None):
        """ Parse a string containing one or more socket addresses, each
        separated by whitespace.
        """
        if isinstance(s, str):
            return cls([Address.parse(a, default_host, default_port)
                        for a in s.split()])
        else:
            raise TypeError("AddressList.parse requires a string argument")

    def __init__(self, iterable=None):
        super().__init__(map(Address, iterable or ()))

    def __str__(self):
        return " ".join(str(Address(_)) for _ in self)

    def __repr__(self):
        return "{}({!r})".format(self.__class__.__name__, list(self))

    def dns_resolve(self, family=0):
        """ Resolve all addresses into one or more resolved address tuples
        using DNS. Each host name will resolve into one or more IP addresses,
        limited by the given address `family` (if any). Each port value
        (either integer or string) will resolve into an integer port value
        (e.g. 'http' will resolve to 80).

            >>> a = AddressList([("localhost", "http")])
            >>> a.dns_resolve()
            >>> a
            AddressList([('::1', 80, 0, 0), ('127.0.0.1', 80)])

        """
        resolved = []
        for address in iter(self):
            host = address[0]
            port = address[1]
            try:
                info = getaddrinfo(host, port, family, SOCK_STREAM)
            except OSError:
                raise ValueError("Cannot resolve address {!r}".format(address))
            else:
                for _, _, _, _, addr in info:
                    if len(address) == 4 and address[3] != 0:
                        # skip any IPv6 addresses with a non-zero scope id
                        # as these appear to cause problems on some platforms
                        continue
                    if addr not in resolved:
                        resolved.append(addr)
        self[:] = resolved

    def custom_resolve(self, resolver):
        """ Perform custom resolution on the contained addresses using a
        resolver function.

        :return:
        """
        if not callable(resolver):
            return
        new_addresses = []
        for address in iter(self):
            for new_address in resolver(address):
                new_addresses.append(new_address)
        self[:] = new_addresses
