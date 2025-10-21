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

import os
import typing as t


if t.TYPE_CHECKING:
    import typing_extensions as te


__all__ = [
    "BLOCKED_TESTKIT_FEATURES",
    "EXTRA_TESTKIT_FEATURES",
    "VERSION",
]


def _get_time_warp_version() -> tuple[float, ...]:
    time_warp_env = os.environ.get("DRIVER_TIME_WARP")
    if not time_warp_env:
        return (float("inf"),)
    return tuple(int(e) for e in time_warp_env.split("."))


VERSION: te.Final[tuple[float, ...]] = _get_time_warp_version()


def _get_blocked_testkit_features() -> frozenset[str]:
    blocked: list[str] = []
    return frozenset(blocked)


def _get_extra_testkit_features() -> frozenset[str]:
    extra: list[str] = []
    return frozenset(extra)


BLOCKED_TESTKIT_FEATURES: te.Final[frozenset[str]] = (
    _get_blocked_testkit_features()
)
EXTRA_TESTKIT_FEATURES: te.Final[frozenset[str]] = (
    _get_extra_testkit_features()
)
