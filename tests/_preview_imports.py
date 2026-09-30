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

import typing as t

import pytest


if t.TYPE_CHECKING:
    from neo4j import (
        AsyncHttpDriver,
        HttpDriver,
    )

from neo4j.warnings import PreviewWarning as _PreviewWarning


with pytest.warns(_PreviewWarning, match="Query API/HTTP support"):
    from neo4j import AsyncHttpDriver
with pytest.warns(_PreviewWarning, match="Query API/HTTP support"):
    from neo4j import HttpDriver


__all__ = [
    "AsyncHttpDriver",
    "HttpDriver",
]
