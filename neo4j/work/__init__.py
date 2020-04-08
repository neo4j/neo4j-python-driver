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


from neo4j.conf import WorkspaceConfig
from neo4j.exceptions import ServiceUnavailable


class Workspace:

    def __init__(self, pool, config):
        assert isinstance(config, WorkspaceConfig)
        self._pool = pool
        self._config = config
        self._connection = None
        self._connection_access_mode = None

    def __del__(self):
        try:
            self.close()
        except OSError:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _connect(self, access_mode):
        if self._connection:
            if access_mode == self._connection_access_mode:
                return
            self._disconnect(sync=True)
        self._connection = self._pool.acquire(access_mode=access_mode, timeout=self._config.connection_acquisition_timeout, database=self._config.database)
        self._connection_access_mode = access_mode

    def _disconnect(self, sync):
        if self._connection:
            if sync:
                try:
                    self._connection.send_all()
                    self._connection.fetch_all()
                except (WorkspaceError, ServiceUnavailable):
                    pass
            if self._connection:
                self._connection.in_use = False
                self._connection = None
            self._connection_access_mode = None

    def close(self):
        self._disconnect(sync=True)


class WorkspaceError(Exception):

    pass
