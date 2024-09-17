# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
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

import logging

from ..._async_compat.util import Util
from ..._conf import WorkspaceConfig
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
from .._debug import NonConcurrentMethodChecker
from ..io import (
    AcquireAuth,
    Neo4jPool,
)


log = logging.getLogger("neo4j")


class Workspace(NonConcurrentMethodChecker):
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
        super().__init__()

    def __del__(
        self,
        _unclosed_resource_warn=unclosed_resource_warn,
        _is_async_code=Util.is_async_code,
        _deprecation_warn=deprecation_warn,
    ):
        if self._closed:
            return
        _unclosed_resource_warn(self)
        # TODO: 6.0 - remove this
        if _is_async_code:
            return
        try:
            _deprecation_warn(
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
                stack_level=5,
            )
            prepared_bookmarks = tuple(bookmarks)
        elif not bookmarks:
            prepared_bookmarks = ()
        else:
            raise TypeError(
                "Bookmarks must be an instance of Bookmarks or an "
                "iterable of raw bookmarks (deprecated)."
            )
        self._initial_bookmarks = self._bookmarks = prepared_bookmarks

    def _get_bookmarks(self):
        if self._bookmark_manager is None:
            return self._bookmarks

        bmm = Util.callback(self._bookmark_manager.get_bookmarks)
        initial = self._initial_bookmarks
        self._last_from_bookmark_manager = tuple({*bmm, *initial})
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
            previous_bookmarks,
            new_bookmarks,
        )

    def _update_bookmark(self, bookmark):
        if not bookmark:
            return
        self._update_bookmarks((bookmark,))

    def _connect(self, access_mode, auth=None, **acquire_kwargs):
        acquisition_timeout = self._config.connection_acquisition_timeout
        auth = AcquireAuth(
            auth,
            force_auth=acquire_kwargs.pop("force_auth", False),
        )

        if self._connection:
            # TODO: Investigate this
            # log.warning("FIXME: should always disconnect before connect")
            self._connection.send_all()
            self._connection.fetch_all()
            self._disconnect()
        if not self._cached_database:
            if self._config.database is not None or not isinstance(
                self._pool, Neo4jPool
            ):
                self._set_cached_database(self._config.database)
            else:
                # This is the first time we open a connection to a server in a
                # cluster environment for this session without explicitly
                # configured database. Hence, we request a routing table update
                # to try to fetch the home database. If provided by the server,
                # we shall use this database explicitly for all subsequent
                # actions within this session.
                log.debug("[#0000]  _: <WORKSPACE> resolve home database")
                self._pool.update_routing_table(
                    database=self._config.database,
                    imp_user=self._config.impersonated_user,
                    bookmarks=self._get_bookmarks(),
                    auth=auth,
                    acquisition_timeout=acquisition_timeout,
                    database_callback=self._set_cached_database,
                )
        acquire_kwargs_ = {
            "access_mode": access_mode,
            "timeout": acquisition_timeout,
            "database": self._config.database,
            "bookmarks": self._get_bookmarks(),
            "auth": auth,
            "liveness_check_timeout": None,
        }
        acquire_kwargs_.update(acquire_kwargs)
        self._connection = self._pool.acquire(**acquire_kwargs_)
        self._connection_access_mode = access_mode

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

    @NonConcurrentMethodChecker.non_concurrent_method
    def close(self) -> None:
        if self._closed:
            return
        self._disconnect(sync=True)
        self._closed = True

    def closed(self) -> bool:
        """
        Indicate whether the session has been closed.

        :returns: :data:`True` if closed, :data:`False` otherwise.
        """
        return self._closed

    def _check_state(self):
        if self._closed:
            raise SessionError(self, "Session closed")
