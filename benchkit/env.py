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


__all__ = [
    "Env",
    "env",
]


class Env(t.NamedTuple):
    backend_port: int
    neo4j_host: str
    neo4j_port: int
    neo4j_scheme: str
    neo4j_user: str
    neo4j_pass: str
    driver_debug: bool


env = Env(
    backend_port=int(os.environ.get("TEST_BACKEND_PORT", "9000")),
    neo4j_host=os.environ.get("TEST_NEO4J_HOST", "localhost"),
    neo4j_port=int(os.environ.get("TEST_NEO4J_PORT", "7687")),
    neo4j_scheme=os.environ.get("TEST_NEO4J_SCHEME", "neo4j"),
    neo4j_user=os.environ.get("TEST_NEO4J_USER", "neo4j"),
    neo4j_pass=os.environ.get("TEST_NEO4J_PASS", "password"),
    driver_debug=(
        os.environ.get("TEST_DRIVER_DEBUG", "").lower()
        in {"y", "yes", "true", "1", "on"}
    ),
)
