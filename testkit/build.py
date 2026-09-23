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

from _common import (
    DRIVER_TIME_WARP,
    get_python_version,
    run_python,
)


if __name__ == "__main__":
    if not DRIVER_TIME_WARP:
        run_python(
            ["-m", "pip", "install", "-U", "pip", "build"],
            warning_as_error=False,
        )
        # Builds on 3.9+ use setuptools 82.0.1+ which has support for
        # PEP 639 and therefore emits a warning since the license metadata in
        # our `pyproject.toml` follows the old format.
        # Option 1 updating the `pyproject.toml` to the new format is not
        #   viable, as on Python 3.7 and 3.8 no recent enough setuptools
        #   version is available to support the new format.
        # Option 2 to silence the warning specifically is not feasible because
        #   of https://github.com/python/cpython/issues/66733
        # Option 3 sticking to pre PEP 639 versions of setup-tools versions
        #   across all Python versions is a valid alternative, but means no bug
        #   fixes in the build backend for users on recent Python versions.
        # Therefore, we will accept and silence the deprecation warning for
        # now.
        build_with_warnings = get_python_version() < (3, 9)
        run_python(["-m", "build", "."], warning_as_error=build_with_warnings)

    run_python(
        ["-m", "pip", "install", "-Ur", "requirements-dev.txt"],
        warning_as_error=False,
    )
