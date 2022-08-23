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


from __future__ import annotations

import asyncio

from ..._async_compat.util import AsyncUtil
from ..._conf import WorkspaceConfig
from ..._deadline import Deadline
from ..._meta import (
    deprecation_warn,
    unclosed_resource_warn,
)
from ...api import Bookmarks
from ...exceptions import (
    ServiceUnavailable,
    SessionError,
    SessionExpired,
)
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
        self._bookmarks = ()
        self._initial_bookmarks = ()
        self._bookmark_manager = None
        self._last_from_bookmark_manager = None
        # Workspace has been closed.
        self._closed = False

    def __del__(self):
        if self._closed:
            return
        unclosed_resource_warn(self)
        # TODO: 6.0 - remove this
        if asyncio.iscoroutinefunction(self.close):
            return
        try:
            deprecation_warn(
                "Relying on AsyncSession's destructor to close the session "
                "is deprecated. Please make sure to close the session. Use it "
                "as a context (`with` statement) or make sure to call "
                "`.close()` explicitly. Future versions of the driver will "
                "not close sessions automatically."
            )
            self.close()
        except (OSError, ServiceUnavailable, SessionExpired):
            pass

    async def __aenter__(self) -> AsyncWorkspace:
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    def _set_cached_database(self, database):
        self._cached_database = True
        self._config.database = database

    def _initialize_bookmarks(self, bookmarks):
        if isinstance(bookmarks, Bookmarks):
            prepared_bookmarks = tuple(bookmarks.raw_values)
        elif hasattr(bookmarks, "__iter__"):
            deprecation_warn(
                "Passing an iterable as `bookmarks` to `Session` is "
                "deprecated. Please use a `Bookmarks` instance.",
                stack_level=5
            )
            prepared_bookmarks = tuple(bookmarks)
        elif not bookmarks:
            prepared_bookmarks = ()
        else:
            raise TypeError("Bookmarks must be an instance of Bookmarks or an "
                            "iterable of raw bookmarks (deprecated).")
        self._initial_bookmarks = self._bookmarks = prepared_bookmarks

    async def _get_bookmarks(self, database):
        if self._bookmark_manager is None:
            return self._bookmarks

        # For 4.3- support: the server will not send the resolved home
        # database back. To avoid confusion between `None` as in "all
        # database" and `None` as in "home database" we re-write the
        # home database to `""`, which otherwise is an invalid database
        # name. It will not work properly either way, as the home database
        # can change (server config change or client side user change).
        if database is None:
            database = ""
        self._last_from_bookmark_manager = tuple({
            *await AsyncUtil.callback(
                self._bookmark_manager.get_bookmarks, database
            ),
            *self._initial_bookmarks
        })
        return self._last_from_bookmark_manager

    async def _get_all_bookmarks(self):
        if self._bookmark_manager is None:
            return self._bookmarks

        self._last_from_bookmark_manager = tuple({
            *await AsyncUtil.callback(
                self._bookmark_manager.get_all_bookmarks,
            ),
            *self._initial_bookmarks
        })
        return self._last_from_bookmark_manager

    async def _update_bookmarks(self, database, new_bookmarks):
        if not new_bookmarks:
            return
        self._initial_bookmarks = ()
        self._bookmarks = new_bookmarks
        if self._bookmark_manager is None:
            return
        previous_bookmarks = self._last_from_bookmark_manager
        # For 4.3- support: the server will not send the resolved home
        # database back. To avoid confusion between `None` as in "all
        # database" and `None` as in "home database" we re-write the home
        # database to `""`, which otherwise is an invalid database name.
        if database is None:
            database = ""
        await AsyncUtil.callback(
            self._bookmark_manager.update_bookmarks,
            database, previous_bookmarks, new_bookmarks
        )

    async def _update_bookmark(self, database, bookmark):
        if not bookmark:
            return
        if not database:
            database = self._config.database
        await self._update_bookmarks(database, (bookmark,))

    async def _connect(self, access_mode, **acquire_kwargs):
        acquisition_timeout = self._config.connection_acquisition_timeout
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
                    bookmarks=await self._get_bookmarks("system"),
                    acquisition_timeout=acquisition_timeout,
                    database_callback=self._set_cached_database
                )
        acquire_kwargs_ = {
            "access_mode": access_mode,
            "timeout": acquisition_timeout,
            "database": self._config.database,
            "bookmarks": await self._get_bookmarks("system"),
            "liveness_check_timeout": None,
        }
        acquire_kwargs_.update(acquire_kwargs)
        self._connection = await self._pool.acquire(**acquire_kwargs_)
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

    async def close(self) -> None:
        if self._closed:
            return
        await self._disconnect(sync=True)
        self._closed = True

    def closed(self) -> bool:
        """Indicate whether the session has been closed.

        :return: :const:`True` if closed, :const:`False` otherwise.
        """
        return self._closed

    def _check_state(self):
        if self._closed:
            raise SessionError(self, "Session closed")
