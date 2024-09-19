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
import subprocess
import sys


if __name__ == "__main__":
    # Until below works
    sys.exit(0)
    cmd = ["python", "-m", "tox", "-vv", "-c", "tox-performance.ini"]
    scheme = os.environ["TEST_NEO4J_SCHEME"]
    host = os.environ["TEST_NEO4J_HOST"]
    port = os.environ["TEST_NEO4J_PORT"]
    uri = f"{scheme}://{host}:{port}"
    env = {
        "NEO4J_USER": os.environ["TEST_NEO4J_USER"],
        "NEO4J_PASSWORD": os.environ["TEST_NEO4J_PASS"],
        "NEO4J_URI": uri,
    }
    subprocess.check_call(
        cmd, universal_newlines=True, stderr=subprocess.STDOUT, env=env
    )
