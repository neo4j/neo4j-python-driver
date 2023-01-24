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

import typing as t
from enum import Enum


if t.TYPE_CHECKING:
    import typing_extensions as te


class RoutingControl(str, Enum):
    """Selection which cluster members to route a query connect to.

    Inherits from :class:`str` and :class:`Enum`. Hence, every driver API
    accepting a :class:`.RoutingControl` value will also accept a string

        >>> RoutingControl.READERS == "r"
        True
        >>> RoutingControl.WRITERS == "w"
        True

    **This is experimental.**
    It might be changed or removed any time even without prior notice.

    .. seealso::
        :attr:`.AsyncDriver.execute_query`, :attr:`.Driver.execute_query`

    .. versionadded:: 5.5
    """
    READERS = "r"
    WRITERS = "w"


if t.TYPE_CHECKING:
    T_RoutingControl = t.Union[
        RoutingControl,
        te.Literal["r", "w"],
    ]
