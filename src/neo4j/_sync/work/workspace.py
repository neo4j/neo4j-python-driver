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
import logging
import typing as t

from ..._async_compat.util import Util
from ..._auth_management import to_auth_dict
from ..._conf import WorkspaceConfig
from ..._meta import (
    deprecation_warn,
    unclosed_resource_warn,
)
from ...api import (
    _TAuth,
    Bookmarks,
)
from ...auth_management import AuthManager
from ...exceptions import (
    ServiceUnavailable,
    SessionError,
    SessionExpired,
)
from ..home_db_cache import (
    HomeDbCache,
    TKey,
)
from ..io import (
    AcquireAuth,
    Neo4jPool,
)


log = logging.getLogger("neo4j")


class Workspace:

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
                "Relying on Session's destructor to close the session "
                "is deprecated. Please make sure to close the session. Use it "
                "as a context (`with` statement) or make sure to call "
                "`.close()` explicitly. Future versions of the driver will "
                "not close sessions automatically."
            )
            self.close()
        except (OSError, ServiceUnavailable, SessionExpired):
            pass

    def __enter__(self) -> Workspace:
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _make_database_callback(
        self,
        cache_key: TKey,
    ) -> t.Callable[[str], t.Union[None]]:
        def _database_callback(database) -> None:
            db_cache: HomeDbCache = self._pool.home_db_cache
            if db_cache.enabled:
                db_cache.set(cache_key, database)
            self._set_cached_database(database)

        return _database_callback

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

    def _get_bookmarks(self,):
        if self._bookmark_manager is None:
            return self._bookmarks

        self._last_from_bookmark_manager = tuple({
            *Util.callback(self._bookmark_manager.get_bookmarks),
            *self._initial_bookmarks
        })
        return self._last_from_bookmark_manager

    def _update_bookmarks(self, new_bookmarks):
        if not new_bookmarks:
            return
        self._initial_bookmarks = ()
        self._bookmarks = new_bookmarks
        if self._bookmark_manager is None:
            return
        previous_bookmarks = self._last_from_bookmark_manager
        Util.callback(
            self._bookmark_manager.update_bookmarks,
            previous_bookmarks, new_bookmarks
        )

    def _update_bookmark(self, bookmark):
        if not bookmark:
            return
        self._update_bookmarks((bookmark,))

    def _connect(self, access_mode, auth=None, **acquire_kwargs):
        acquisition_timeout = self._config.connection_acquisition_timeout
        force_auth=acquire_kwargs.pop("force_auth", False)
        acquire_auth = AcquireAuth(auth, force_auth=force_auth)

        if self._connection:
            # TODO: Investigate this
            # log.warning("FIXME: should always disconnect before connect")
            self._connection.send_all()
            self._connection.fetch_all()
            self._disconnect()
        self._fill_cached_database(acquire_auth)
        acquire_kwargs_ = {
            "access_mode": access_mode,
            "timeout": acquisition_timeout,
            "database": self._config.database,
            "bookmarks": self._get_bookmarks(),
            "auth": acquire_auth,
            "liveness_check_timeout": None,
        }
        acquire_kwargs_.update(acquire_kwargs)
        self._connection = self._pool.acquire(**acquire_kwargs_)
        self._connection_access_mode = access_mode

    def _fill_cached_database(self, acquire_auth: AcquireAuth) -> None:
        auth = acquire_auth.auth
        acquisition_timeout = self._config.connection_acquisition_timeout
        if not self._cached_database:
            if (self._config.database is not None
                or not isinstance(self._pool, Neo4jPool)):
                self._set_cached_database(self._config.database)
            else:
                # This is the first time we open a connection to a server in a
                # cluster environment for this session without explicitly
                # configured database. Hence, we request a routing table update
                # to try to fetch the home database. If provided by the server,
                # we shall use this database explicitly for all subsequent
                # actions within this session.
                # Unless we have the resolved home db in out cache:

                db_cache: HomeDbCache = self._pool.home_db_cache
                cache_key = cached_db = None
                if db_cache.enabled:
                    cache_key = db_cache.compute_key(
                        self._config.impersonated_user,
                        self._resolve_session_auth(auth)
                    )
                    cached_db = db_cache.get(cache_key)
                if cached_db is not None:
                    log.debug("[#0000]  _: <WORKSPACE> resolved home database "
                              f"from cache: {cached_db}")
                    self._set_cached_database(cached_db)
                    return
                log.debug("[#0000]  _: <WORKSPACE> resolve home database")
                self._pool.update_routing_table(
                    database=self._config.database,
                    imp_user=self._config.impersonated_user,
                    bookmarks=self._get_bookmarks(),
                    auth=acquire_auth,
                    acquisition_timeout=acquisition_timeout,
                    database_callback=self._make_database_callback(cache_key),
                )

    @staticmethod
    def _resolve_session_auth(
        auth: t.Union[AuthManager, AuthManager, None]
    ) -> t.Optional[dict]:
        if auth is None:
            return None
        # resolved_auth = await AsyncUtil.callback(auth.get_auth)
        # The above line breaks mypy
        # https://github.com/python/mypy/issues/15295
        auth_getter: t.Callable[[], t.Union[_TAuth, t.Union[_TAuth]]] = \
            auth.get_auth
        # so we enforce the right type here
        # (explicit type annotation above added as it's a necessary assumption
        #  for this cast to be correct)
        resolved_auth = t.cast(_TAuth, Util.callback(auth_getter))
        return to_auth_dict(resolved_auth)

    def _disconnect(self, sync=False):
        if self._connection:
            if sync:
                try:
                    self._connection.send_all()
                    self._connection.fetch_all()
                except ServiceUnavailable:
                    pass
            if self._connection:
                self._pool.release(self._connection)
                self._connection = None
            self._connection_access_mode = None

    def close(self) -> None:
        if self._closed:
            return
        self._disconnect(sync=True)
        self._closed = True

    def closed(self) -> bool:
        """Indicate whether the session has been closed.

        :returns: :const:`True` if closed, :const:`False` otherwise.
        """
        return self._closed

    def _check_state(self):
        if self._closed:
            raise SessionError(self, "Session closed")
