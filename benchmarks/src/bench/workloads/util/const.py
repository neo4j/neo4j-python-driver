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

from typing import Final


__all__ = [
    "I8_MAX",
    "I8_MIN",
    "I16_MAX",
    "I16_MIN",
    "I32_MAX",
    "I32_MIN",
    "I64_MAX",
    "I64_MIN",
]

I8_MAX: Final[int] = 127
I8_MIN: Final[int] = -128
I16_MAX: Final[int] = 32767
I16_MIN: Final[int] = -32768
I32_MAX: Final[int] = 2147483647
I32_MIN: Final[int] = -2147483648
I64_MAX: Final[int] = 9223372036854775807
I64_MIN: Final[int] = -9223372036854775808
