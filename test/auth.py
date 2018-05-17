#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2018 "Neo4j,"
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


from sys import argv

from neo4j.v1 import basic_auth
from neo4j.v1.api import GraphDatabase
from neo4j.util import Watcher


def update_password(user, password, new_password):
    """ Test utility for setting the initial password.

    :param user: user name
    :param password: current password
    :param new_password: new password
    """

    token = basic_auth(user, password)
    setattr(token, "new-credentials", new_password)  # TODO: hopefully switch hyphen to underscore on server
    GraphDatabase.driver("bolt://localhost:7687", auth=token).session().close()


if __name__ == "__main__":
    Watcher("neo4j.bolt").watch()
    update_password("neo4j", "neo4j", argv[1])
