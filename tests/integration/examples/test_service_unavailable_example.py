#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2020 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
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


import pytest

# tag::service-unavailable-import[]
from neo4j.exceptions import ServiceUnavailable
# end::service-unavailable-import[]


class ServiceUnavailableExample:

    def __init__(self, driver):
        self.driver = driver

    # tag::service-unavailable[]
    def add_item(self):
        try:
            with self.driver.session() as session:
                session.write_transaction(lambda tx: tx.run("CREATE (a:Item)"))
            return True
        except ServiceUnavailable:
            return False
    # end::service-unavailable[]


def test_example():
    # TODO: Better error messages for the user
    pytest.skip("Fix better error messages for the user. Be able to kill the server.")
