#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2019 "Neo4j,"
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


from neo4j.conf import DeprecatedAlias
from neo4j.work import AsyncWorkspace, WorkspaceConfig


class AsyncSessionConfig(WorkspaceConfig):

    #:
    acquire_timeout = 30.0  # seconds

    #:
    bookmarks = ()

    #:
    default_access_mode = "WRITE"
    access_mode = DeprecatedAlias("default_access_mode")


class AsyncSession(AsyncWorkspace):

    # The set of bookmarks after which the next
    # :class:`.Transaction` should be carried out.
    _bookmarks_in = None

    # The bookmark returned from the last commit.
    _bookmark_out = None

    def __init__(self, pool, config):
        super().__init__(pool, config)
        assert isinstance(config, AsyncSessionConfig)
        self._bookmarks_in = tuple(config.bookmarks)

    async def run(self, cypher, parameters=None, **kwparameters):
        raise NotImplementedError
