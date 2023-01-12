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


import io
import logging
import sys


formatter = logging.Formatter("%(asctime)s [%(levelname)-8s] %(message)s")

buffer_handler = logging.StreamHandler(io.StringIO())
buffer_handler.setLevel(logging.DEBUG)
buffer_handler.setFormatter(formatter)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logging.getLogger("neo4j").addHandler(handler)
logging.getLogger("neo4j").addHandler(buffer_handler)
logging.getLogger("neo4j").setLevel(logging.DEBUG)

log = logging.getLogger("testkitbackend")
log.addHandler(handler)
log.setLevel(logging.DEBUG)


__all__ = [
    "buffer_handler",
    "log",
]
