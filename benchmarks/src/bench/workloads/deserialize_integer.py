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

# * *date*: 2025-10-27
# * *id*: `deserialize-integer`


from __future__ import annotations

from typing import TYPE_CHECKING

from .util import (
    benchmark,
    deserialization_template,
)


if TYPE_CHECKING:
    from .util import Bencher


@benchmark
def deserialize_integer(bencher: Bencher) -> None:
    query = (
        "WITH [0, -16, 127, -17, -128, 128, 32767, -32768, 2147483647, "
        "-2147483648, 9223372036854775807, -9223372036854775808] "
        "AS dataSmall\n"
        "RETURN reduce(l = [], _ in range(1, 500) | l + dataSmall)"
    )
    deserialization_template(bencher, query)
