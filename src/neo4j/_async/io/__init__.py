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


"""
This module contains the low-level functionality required for speaking
Bolt. It is not intended to be used directly by driver users. Instead,
the `session` module provides the main user-facing abstractions.
"""


__all__ = [
    "AcquireAuth",
    "AsyncBolt",
    "AsyncBoltPool",
    "AsyncNeo4jPool",
    "check_supported_server_product",
    "ConnectionErrorHandler",
]


from ._bolt import AsyncBolt
from ._common import (
    check_supported_server_product,
    ConnectionErrorHandler,
)
from ._pool import (
    AcquireAuth,
    AsyncBoltPool,
    AsyncNeo4jPool,
)
