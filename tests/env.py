# Copyright (c) "Neo4j"
# Neo4j Sweden AB [http://neo4j.com]
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


from os import getenv


# Full path of a server package to be used for integration testing
NEO4J_SERVER_PACKAGE = getenv("NEO4J_SERVER_PACKAGE")

# An existing remote server at this URI
NEO4J_SERVER_URI = getenv("NEO4J_URI")

# Name of a user for the currently running server
NEO4J_USER = getenv("NEO4J_USER")

# Password for the currently running server
NEO4J_PASSWORD = getenv("NEO4J_PASSWORD")

NEOCTRL_ARGS = getenv("NEOCTRL_ARGS", "3.4.1")
