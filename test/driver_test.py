#!/usr/bin/env python
#! -*- encoding: UTF-8 -*-

# Copyright (c) 2002-2015 "Neo Technology,"
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


from unittest import main, TestCase

import neo4j


class RunStatementTestCase(TestCase):

    def test_can_run_statement(self):
        session = neo4j.session("neo4j://localhost")
        session.run("CREATE (n {name:'Bob'})")
        for record in session.run("MATCH (n) RETURN n.name"):
            print(record)
        session.close()


if __name__ == "__main__":
    main()
