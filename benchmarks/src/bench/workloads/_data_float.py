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

import struct
from typing import Final


__all__ = [
    "DATA_SMALL",
    "bytes_list_to_float_list",
]


def bytes_list_to_float_list(raw: list[int]) -> list[float]:
    return [struct.unpack("=d", struct.pack("=Q", b))[0] for b in raw]


DATA_SMALL: Final[list[float]] = bytes_list_to_float_list(
    [
        0x0000000000000000,
        0xFFFFFFFFFFFFFFFF,
        0xAAAAAAAAAAAAAAAA,
        0x5555555555555555,
        0x7FF0000000000000,
        0xFFF0000000000000,
    ]
)
