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


from functools import wraps as _wraps


__all__ = [
    "wrap_async",
]


def wrap_async(func):
    @_wraps(func)
    async def wrapper(*args, **kwargs):  # noqa: RUF029
        # [noqa] the hole point of this wrapper is to turn a sync function into
        # an async one for testing purposes
        return func(*args, **kwargs)

    return wrapper
