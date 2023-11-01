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


import os
import re
import subprocess
import sys


TEST_BACKEND_VERSION = os.getenv("TEST_BACKEND_VERSION", "python")


def run(args, env=None):
    print(args)
    return subprocess.run(
        args, universal_newlines=True, stdout=sys.stdout, stderr=sys.stderr,
        check=True, env=env
    )


def get_python_version():
    cmd = [TEST_BACKEND_VERSION, "-V"]
    res = subprocess.check_output(cmd, universal_newlines=True,
                                  stderr=sys.stderr)
    raw_version = re.match(r"(?:.*?)((?:\d+\.)+(?:\d+))", res).group(1)
    return tuple(int(e) for e in raw_version.split("."))


def run_python(args, env=None, warning_as_error=True):
    cmd = [TEST_BACKEND_VERSION, "-u"]
    if get_python_version() >= (3, 12):
        # Ignore warnings for Python 3.12 for now
        # https://github.com/dateutil/dateutil/issues/1284 needs to be released
        # and propagate through our dependency graph
        warning_as_error = False
    if warning_as_error:
        cmd += ["-W", "error"]
    cmd += list(args)
    run(cmd, env=env)
