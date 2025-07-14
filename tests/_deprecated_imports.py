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
        NotificationCategory,
        NotificationDisabledCategory,
        SummaryNotification,
    )


with pytest.warns(DeprecationWarning, match="NotificationCategory"):
    from neo4j import NotificationCategory
with pytest.warns(
    DeprecationWarning,
    match="NotificationDisabledCategory",
):
    from neo4j import NotificationDisabledCategory
with pytest.warns(DeprecationWarning, match="SummaryNotification"):
    from neo4j import SummaryNotification


__all__ = [
    "NotificationCategory",
    "NotificationDisabledCategory",
    "SummaryNotification",
]
