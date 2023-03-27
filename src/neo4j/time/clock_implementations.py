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

# TODO: 6.0 - remove this file


from .._meta import deprecation_warn
from ._clock_implementations import (
    LibCClock,
    PEP564Clock,
    SafeClock,
)


__all__ = [
    "SafeClock",
    "PEP564Clock",
    "LibCClock",
]

deprecation_warn(
    "The module `neo4j.time.clock_implementations` was made internal and will "
    "no longer be available for import in future versions.",
    stack_level=2
)
