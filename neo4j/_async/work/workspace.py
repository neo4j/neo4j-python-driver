# Copyright (c) "Neo4j"
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


import asyncio

from ...conf import WorkspaceConfig
from ...exceptions import ServiceUnavailable
from ..io import AsyncNeo4jPool


class AsyncWorkspace:

    def __init__(self, pool, config):
        assert isinstance(config, WorkspaceConfig)
        self._pool = pool
        self._config = config
        self._connection = None
        self._connection_access_mode = None
        # Sessions are supposed to cache the database on which to operate.
        self._cached_database = False
        self._bookmarks_in = None

    def __del__(self):
        if asyncio.iscoroutinefunction(self.close):
            return
        try:
            self.close()
        except OSError:
            pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    def _set_cached_database(self, database):
        self._cached_database = True
        self._config.database = database

    async def _connect(self, access_mode):
        if self._connection:
            # TODO: Investigate this
            # log.warning("FIXME: should always disconnect before connect")
            await self._connection.send_all()
            await self._connection.fetch_all()
            await self._disconnect()
        if not self._cached_database:
            if (self._config.database is not None
                    or not isinstance(self._pool, AsyncNeo4jPool)):
                self._set_cached_database(self._config.database)
            else:
                # This is the first time we open a connection to a server in a
                # cluster environment for this session without explicitly
                # configured database. Hence, we request a routing table update
                # to try to fetch the home database. If provided by the server,
                # we shall use this database explicitly for all subsequent
                # actions within this session.
                await self._pool.update_routing_table(
                    database=self._config.database,
                    imp_user=self._config.impersonated_user,
                    bookmarks=self._bookmarks_in,
                    database_callback=self._set_cached_database
                )
        self._connection = await self._pool.acquire(
            access_mode=access_mode,
            timeout=self._config.connection_acquisition_timeout,
            database=self._config.database,
            bookmarks=self._bookmarks_in
        )
        self._connection_access_mode = access_mode

    async def _disconnect(self, sync=False):
        if self._connection:
            if sync:
                try:
                    await self._connection.send_all()
                    await self._connection.fetch_all()
                except ServiceUnavailable:
                    pass
            if self._connection:
                await self._pool.release(self._connection)
                self._connection = None
            self._connection_access_mode = None

    async def close(self):
        await self._disconnect(sync=True)
