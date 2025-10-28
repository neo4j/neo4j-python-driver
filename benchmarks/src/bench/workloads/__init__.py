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


# ruff: disable[RUF067]
# import all benchmark modules for them to be picked up by the registry
def _load() -> None:
    import importlib
    import os

    this_dir = os.path.dirname(__file__)
    for module in os.listdir(this_dir):
        if module.startswith("_") or module[-3:] != ".py":
            continue
        importlib.import_module(f".{module[:-3]}", package=__package__)


_load()
del _load
# ruff: enable[RUF067]
