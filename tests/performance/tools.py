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


from unittest import SkipTest

from neo4j import GraphDatabase
from neo4j.exceptions import AuthError
from tests.env import (
    NEO4J_PASSWORD,
    NEO4J_SERVER_URI,
    NEO4J_USER,
)


def is_listening(address):
    from socket import create_connection
    try:
        s = create_connection(address)
    except IOError:
        return False
    else:
        s.close()
        return True


class RemoteGraphDatabaseServer(object):
    server_uri = NEO4J_SERVER_URI or "bolt://localhost:7687"
    auth_token = (NEO4J_USER or "neo4j", NEO4J_PASSWORD)
    encrypted = NEO4J_SERVER_URI is not None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    @classmethod
    def start(cls):
        with GraphDatabase.driver(cls.server_uri, auth=cls.auth_token, encrypted=cls.encrypted) as driver:
            try:
                with driver.session():
                    print("Using existing remote server {}\n".format(cls.server_uri))
                    return
            except AuthError as error:
                raise RuntimeError("Failed to authenticate (%s)" % error)
        raise SkipTest("No remote Neo4j server available for %s" % cls.__name__)

    @classmethod
    def stop(cls):
        pass
