#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2016 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import platform
from unittest import skipIf

from neo4j.v1 import GraphDatabase, basic_auth, ProtocolError

from test.util import ServerTestCase, restart_server


auth_token = basic_auth("neo4j", "password")


class ServerRestartTestCase(ServerTestCase):

    # @skipIf(platform.system() == "Windows", "restart testing not supported on Windows")
    # def test_server_shutdown_detection(self):
    #     driver = GraphDatabase.driver("bolt://localhost", auth=auth_token)
    #     session = driver.session()
    #     session.run("RETURN 1").consume()
    #     assert restart_server()
    #     with self.assertRaises(ProtocolError):
    #         session.run("RETURN 1").consume()
    #     session.close()
    pass
