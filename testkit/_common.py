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


import os
import re
import subprocess
import sys


TEST_BACKEND_VERSION = os.getenv("TEST_BACKEND_VERSION", "python")
# space-separated list of tox factor filters to apply.
# Example: `f1-f2 f3` to add `-f f1-f2 -f f3` to each tox invocation.
TEST_TOX_FACTORS = os.getenv("TEST_TOX_FACTORS", "")
DRIVER_TIME_WARP = os.getenv("DRIVER_TIME_WARP")


def run(args, env=None):
    print(args)
    return subprocess.run(
        args,
        text=True,
        stdout=sys.stdout,
        stderr=sys.stderr,
        check=True,
        env=env,
    )


def get_python_version():
    cmd = [TEST_BACKEND_VERSION, "-V"]
    res = subprocess.check_output(
        cmd, universal_newlines=True, stderr=sys.stderr
    )
    raw_version = re.match(r"(?:.*?)((?:\d+\.)+(?:\d+))", res).group(1)
    return tuple(int(e) for e in raw_version.split("."))


def run_python(args, env=None, warning_as_error=True):
    cmd = [TEST_BACKEND_VERSION, "-u"]
    if warning_as_error:
        cmd += ["-W", "error"]
    cmd += list(args)
    run(cmd, env=env)


def get_tox_factor_args(base_factor: str):
    return tuple(
        arg
        for factor in TEST_TOX_FACTORS.split()
        for arg in ("-f", f"{factor}-{base_factor}")
    ) or ("-f", base_factor)
