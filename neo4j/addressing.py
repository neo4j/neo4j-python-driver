# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from __future__ import annotations

import logging
import typing as t
from socket import (
    AddressFamily,
    AF_INET,
    AF_INET6,
    getservbyname,
)


if t.TYPE_CHECKING:
    import typing_extensions as te


log = logging.getLogger("neo4j")


_T = t.TypeVar("_T")


if t.TYPE_CHECKING:

    class _WithPeerName(te.Protocol):
        def getpeername(self) -> tuple: ...


class _AddressMeta(type(tuple)):  # type: ignore[misc]

    def __init__(cls, *args, **kwargs):
        super().__init__(*args, **kwargs)
        cls._ipv4_cls = None
        cls._ipv6_cls = None

    def _subclass_by_family(self, family):
        subclasses = [
            sc for sc in self.__subclasses__()
            if (sc.__module__ == self.__module__
                and getattr(sc, "family", None) == family)
        ]
        if len(subclasses) != 1:
            raise ValueError(
                "Class {} needs exactly one direct subclass with attribute "
                "`family == {}` within this module. "
                "Found: {}".format(self, family, subclasses)
            )
        return subclasses[0]

    @property
    def ipv4_cls(self):
        if self._ipv4_cls is None:
            self._ipv4_cls = self._subclass_by_family(AF_INET)
        return self._ipv4_cls

    @property
    def ipv6_cls(self):
        if self._ipv6_cls is None:
            self._ipv6_cls = self._subclass_by_family(AF_INET6)
        return self._ipv6_cls


class Address(tuple, metaclass=_AddressMeta):

    @classmethod
    def from_socket(
        cls: t.Type[_TAddress],
        socket: _WithPeerName
    ) -> _TAddress:
        address = socket.getpeername()
        return cls(address)

    @classmethod
    def parse(
        cls: t.Type[_TAddress],
        s: str,
        default_host: t.Optional[str] = None,
        default_port: t.Optional[int] = None
    ) -> _TAddress:
        if not isinstance(s, str):
            raise TypeError("Address.parse requires a string argument")
        if s.startswith("["):
            # IPv6
            port: t.Union[str, int]
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
    def parse_list(
        cls: t.Type[_TAddress],
        *s: str,
        default_host: t.Optional[str] = None,
        default_port: t.Optional[int] = None
    ) -> t.List[_TAddress]:
        """ Parse a string containing one or more socket addresses, each
        separated by whitespace.
        """
        if not all(isinstance(s0, str) for s0 in s):
            raise TypeError("Address.parse_list requires a string argument")
        return [cls.parse(a, default_host, default_port)
                for a in " ".join(s).split()]

    def __new__(cls, iterable: t.Collection) -> Address:
        if isinstance(iterable, cls):
            return iterable
        n_parts = len(iterable)
        inst = tuple.__new__(cls, iterable)
        if n_parts == 2:
            inst.__class__ = cls.ipv4_cls
        elif n_parts == 4:
            inst.__class__ = cls.ipv6_cls
        else:
            raise ValueError("Addresses must consist of either "
                             "two parts (IPv4) or four parts (IPv6)")
        return inst

    #: Address family (AF_INET or AF_INET6)
    family: t.Optional[AddressFamily] = None

    def __repr__(self):
        return "{}({!r})".format(self.__class__.__name__, tuple(self))

    @property
    def host_name(self) -> str:
        return self[0]

    @property
    def host(self) -> str:
        return self[0]

    @property
    def port(self) -> int:
        return self[1]

    @property
    def unresolved(self) -> Address:
        return self

    @property
    def port_number(self) -> int:
        try:
            return getservbyname(self[1])
        except (OSError, TypeError):
            # OSError: service/proto not found
            # TypeError: getservbyname() argument 1 must be str, not X
            try:
                return int(self[1])
            except (TypeError, ValueError) as e:
                raise type(e)("Unknown port value %r" % self[1])


_TAddress = t.TypeVar("_TAddress", bound=Address)


class IPv4Address(Address):

    family = AF_INET

    def __str__(self) -> str:
        return "{}:{}".format(*self)


class IPv6Address(Address):

    family = AF_INET6

    def __str__(self) -> str:
        return "[{}]:{}".format(*self)


class ResolvedAddress(Address):

    _host_name: str

    @property
    def host_name(self) -> str:
        return self._host_name

    @property
    def unresolved(self) -> Address:
        return super().__new__(Address, (self._host_name, *self[1:]))

    def __new__(cls, iterable, *, host_name: str) -> ResolvedAddress:
        new = super().__new__(cls, iterable)
        new = t.cast(ResolvedAddress, new)
        new._host_name = host_name
        return new


class ResolvedIPv4Address(IPv4Address, ResolvedAddress):
    pass


class ResolvedIPv6Address(IPv6Address, ResolvedAddress):
    pass
