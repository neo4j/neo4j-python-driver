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


"""Building driver and test backend inside driver container."""

import sys

from _common import (
    DRIVER_TIME_WARP,
    run_python,
)


if __name__ == "__main__":
    if DRIVER_TIME_WARP:
        run_python(
            ["-m", "pip", "install", "-U", "--group", "testkit"],
            warning_as_error=False,
        )
        sys.exit(0)

    run_python(
        ["-m", "pip", "install", "-U", "pip"],
        warning_as_error=False,
    )
    run_python(
        ["-m", "pip", "install", "-U", "--group", "packaging"],
        warning_as_error=False,
    )
    run_python(["-m", "build", "."], warning_as_error=True)
    run_python(
        ["-m", "pip", "install", "-U", "--group", "testkit", "-e", "."],
        warning_as_error=False,
    )
