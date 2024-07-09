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


"""
Executed in driver container.
Responsible for building driver and test backend.
"""

import os

from _common import (
    configured_extensions,
    run_python,
)


if __name__ == "__main__":
    run_python(["-m", "pip", "install", "-U", "pip"],
               warning_as_error=False)

    run_python(["-m", "pip", "install", "-Ur", "requirements-dev.txt"],
               warning_as_error=False)
    with configured_extensions():
        run_python(["-m", "pip", "install", ".[pandas,numpy,pyarrow]"],
                   warning_as_error=False)
    run_python(["-m", "tox", "f", "clean"])
