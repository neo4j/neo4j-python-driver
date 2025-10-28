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

from neo4j.vector import Vector

from .util.const import (
    I8_MAX,
    I8_MIN,
    I16_MAX,
    I16_MIN,
    I32_MAX,
    I32_MIN,
    I64_MAX,
    I64_MIN,
)


__all__ = ["DATA"]


DATA: Final[list[Vector]] = [
    Vector(
        [0, 1, I8_MAX, I8_MIN] * 250,
        "i8",
    ),
    Vector(
        [0, 1, I16_MAX, I16_MIN] * 200,
        "i16",
    ),
    Vector(
        [0, 1, I32_MAX, I32_MIN] * 160,
        "i32",
    ),
    Vector(
        [0, 1, I64_MAX, I64_MIN] * 128,
        "i64",
    ),
    Vector(
        [0.0, -0.0, 1.5000001, -1.5000001] * 160,
        "f32",
    ),
    Vector(
        [0.0, -0.0, 1.5000000000000002, -1.5000000000000002] * 128,
        "f64",
    ),
]
