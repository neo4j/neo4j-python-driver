# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
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

import dataclasses
import math
import re

from . import _typing as t
from .api import ServerInfo


if t.TYPE_CHECKING:
    from .addressing import Address

__all__ = [
    "BoltProtocolVersion",
]


class BoltProtocolVersion:
    version: tuple[int, int]

    def __init__(self, major: int, minor: int) -> None:
        if major < 0 or minor < 0:
            raise ValueError(
                "Major and minor versions must be non-negative integers. "
                f"Found {major}, {minor}."
            )
        self.version = (major, minor)

    @property
    def major(self) -> int:
        return self.version[0]

    @property
    def minor(self) -> int:
        return self.version[1]

    def __hash__(self) -> int:
        return hash(self.version)

    def __iter__(self) -> t.Iterator[int]:
        return iter(self.version)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, BoltProtocolVersion):
            return self.version == other.version
        if isinstance(other, tuple):
            return self.version == other
        return NotImplemented

    def __ne__(self, other: object) -> bool:
        if isinstance(other, BoltProtocolVersion):
            return self.version != other.version
        if isinstance(other, tuple):
            return self.version != other
        return NotImplemented

    def __lt__(self, other: BoltProtocolVersion | tuple) -> bool:
        if isinstance(other, BoltProtocolVersion):
            return self.version < other.version
        if isinstance(other, tuple):
            return self.version < other
        return NotImplemented

    def __le__(self, other: BoltProtocolVersion | tuple) -> bool:
        if isinstance(other, BoltProtocolVersion):
            return self.version <= other.version
        if isinstance(other, tuple):
            return self.version <= other
        return NotImplemented

    def __gt__(self, other: BoltProtocolVersion | tuple) -> bool:
        if isinstance(other, BoltProtocolVersion):
            return self.version > other.version
        if isinstance(other, tuple):
            return self.version > other
        return NotImplemented

    def __ge__(self, other: BoltProtocolVersion | tuple) -> bool:
        if isinstance(other, BoltProtocolVersion):
            return self.version >= other.version
        if isinstance(other, tuple):
            return self.version >= other
        return NotImplemented

    def __repr__(self) -> str:
        return f"BoltProtocolVersion({self.major}, {self.minor})"

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}"


_VERSION_RE = re.compile(r"^(\d+)\.(\d+)")


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True)
class HTTPServerInfo:
    neo4j_version: str
    parsed_neo4j_version: tuple[int, int] | None = dataclasses.field(
        init=False, default=None
    )
    server_agent: str = dataclasses.field(init=False)
    bolt_version: str | None
    parsed_bolt_version: BoltProtocolVersion | None = dataclasses.field(
        init=False, default=None
    )

    def __post_init__(self):
        match = _VERSION_RE.match(self.neo4j_version)
        if match is not None:
            parsed_version = tuple(map(int, match.groups()))
            object.__setattr__(self, "parsed_neo4j_version", parsed_version)
        if self.bolt_version is not None:
            match = _VERSION_RE.match(self.bolt_version)
            if match is not None:
                parsed_version = BoltProtocolVersion(*map(int, match.groups()))
                object.__setattr__(self, "parsed_bolt_version", parsed_version)
        server_agent = f"Neo4j/{self.neo4j_version}"
        object.__setattr__(self, "server_agent", server_agent)

    def as_server_info(
        self, address: Address, connection_id: str
    ) -> ServerInfo:
        protocol_version = (0, 0)
        if self.parsed_bolt_version is not None:
            protocol_version = self.parsed_bolt_version.version
        server_info = ServerInfo(address, protocol_version=protocol_version)
        server_info.update(
            {
                "protocol_version": ".".join(map(str, protocol_version)),
                "connection_id": connection_id,
                "server": self.server_agent,
            }
        )
        return server_info


def min_timeout(*timeouts: float | None) -> float | None:
    """Return the minimum timeout from an iterable of timeouts."""
    return min(
        (
            to
            for to in timeouts
            if to is not None and not math.isnan(to) and to >= 0
        ),
        default=None,
    )
