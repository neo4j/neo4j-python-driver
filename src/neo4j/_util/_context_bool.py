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


__all__ = ["ContextBool"]


class ContextBool:
    def __init__(self) -> None:
        self._value = False

    def __bool__(self) -> bool:
        return self._value

    def __enter__(self) -> None:
        self._value = True

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self._value = False
