#!/usr/bin/env python

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


import sys


if __name__ == "__main__":
    # Until below works
    sys.exit(0)
    cmd = ["python", "-m", "tox", "-c", "tox-performance.ini"]
    uri = "%s://%s:%s" % (
            os.environ["TEST_NEO4J_SCHEME"],
            os.environ["TEST_NEO4J_HOST"],
            os.environ["TEST_NEO4J_PORT"])
    env = {
            "NEO4J_USER": os.environ["TEST_NEO4J_USER"],
            "NEO4J_PASSWORD": os.environ["TEST_NEO4J_PASS"],
            "NEO4J_URI": uri}
    subprocess.check_call(cmd, universal_newlines=True,
                          stderr=subprocess.STDOUT, env=env)
