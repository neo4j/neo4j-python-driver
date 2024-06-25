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
from itertools import (
    chain,
    combinations,
)


if t.TYPE_CHECKING:
    _T = t.TypeVar("_T")


def powerset(
    iterable: t.Iterable[_T],
    limit: t.Optional[int] = None,
) -> t.Iterable[t.Tuple[_T, ...]]:
    """
    Build the powerset of an iterable.

        >>> tuple(powerset([1, 2, 3]))
        ((), (1,), (2,), (3,), (1, 2), (1, 3), (2, 3), (1, 2, 3))

        >>> tuple(powerset([1, 2, 3], limit=2))
        ((), (1,), (2,), (3,), (1, 2), (1, 3), (2, 3))
    """
    s = list(iterable)
    if limit is None:
        limit = len(s)
    return chain.from_iterable(combinations(s, r) for r in range(limit + 1))
