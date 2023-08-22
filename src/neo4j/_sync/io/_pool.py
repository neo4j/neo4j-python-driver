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

import abc
import asyncio
import logging
import typing as t
from collections import (
    defaultdict,
    deque,
)
from copy import copy
from dataclasses import dataclass
from logging import getLogger
from random import choice

from ..._async_compat.concurrency import (
    Condition,
    CooperativeRLock,
    RLock,
)
from ..._async_compat.network import NetworkUtil
from ..._async_compat.util import Util
from ..._conf import (
    PoolConfig,
    WorkspaceConfig,
)
from ..._deadline import (
    connection_deadline,
    Deadline,
)
from ..._exceptions import BoltError
from ..._routing import RoutingTable
from ...api import (
    READ_ACCESS,
    WRITE_ACCESS,
)
from ...auth_management import AuthManager
from ...exceptions import (
    ClientError,
    ConfigurationError,
    DriverError,
    Neo4jError,
    ReadServiceUnavailable,
    ServiceUnavailable,
    SessionExpired,
    WriteServiceUnavailable,
)
from ._bolt import Bolt


# Set up logger
log = getLogger("neo4j")


@dataclass
class AcquireAuth:
    auth: t.Union[AuthManager, AuthManager, None]
    force_auth: bool = False


class IOPool(abc.ABC):
    """ A collection of connections to one or more server addresses.
    """

    def __init__(self, opener, pool_config, workspace_config):
        assert callable(opener)
        assert isinstance(pool_config, PoolConfig)
        assert isinstance(workspace_config, WorkspaceConfig)

        self.opener = opener
        self.pool_config = pool_config
        self.workspace_config = workspace_config
        self.connections = defaultdict(deque)
        self.connections_reservations = defaultdict(lambda: 0)
        self.lock = CooperativeRLock()
        self.cond = Condition(self.lock)

    @property
    @abc.abstractmethod
    def is_direct_pool(self) -> bool:
        ...

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def get_auth(self):
        return Util.callback(self.pool_config.auth.get_auth)

    def _acquire_from_pool(self, address):
        with self.lock:
            for connection in list(self.connections.get(address, [])):
                if connection.in_use:
                    continue
                connection.pool = self
                connection.in_use = True
                return connection
        return None  # no free connection available

    def _remove_connection(self, connection):
        address = connection.unresolved_address
        with self.lock:
            log.debug(
                "[#%04X]  _: <POOL> remove connection from pool %r %s",
                connection.local_port, address, connection.connection_id
            )
            try:
                self.connections.get(address, []).remove(connection)
            except ValueError:
                # If closure fails (e.g. because the server went
                # down), all connections to the same address will
                # be removed. Therefore, we silently ignore if the
                # connection isn't in the pool anymore.
                pass

    def _acquire_from_pool_checked(
        self, address, health_check, deadline
    ):
        while not deadline.expired():
            connection = self._acquire_from_pool(address)
            if not connection:
                return None  # no free connection available
            if not health_check(connection, deadline):
                # `close` is a noop on already closed connections.
                # This is to make sure that the connection is
                # gracefully closed, e.g. if it's just marked as
                # `stale` but still alive.
                if log.isEnabledFor(logging.DEBUG):
                    log.debug(
                        "[#%04X]  _: <POOL> found unhealthy connection %s "
                        "(closed=%s, defunct=%s, stale=%s, in_use=%s)",
                        connection.local_port, connection.connection_id,
                        connection.closed(), connection.defunct(),
                        connection.stale(), connection.in_use
                    )
                connection.close()
                self._remove_connection(connection)
                continue  # try again with a new connection
            else:
                return connection

    def _acquire_new_later(self, address, auth, deadline):
        def connection_creator():
            released_reservation = False
            try:
                try:
                    connection = self.opener(
                        address, auth or self.pool_config.auth, deadline
                    )
                except ServiceUnavailable:
                    self.deactivate(address)
                    raise
                if auth:
                    # It's unfortunate that we have to create a connection
                    # first to determine if the session-level auth is supported
                    # by the protocol or not.
                    try:
                        connection.assert_re_auth_support()
                    except ConfigurationError:
                        log.debug("[#%04X]  _: <POOL> no re-auth support",
                                  connection.local_port)
                        connection.close()
                        raise
                connection.pool = self
                connection.in_use = True
                with self.lock:
                    self.connections_reservations[address] -= 1
                    released_reservation = True
                    self.connections[address].append(connection)
                return connection
            finally:
                if not released_reservation:
                    with self.lock:
                        self.connections_reservations[address] -= 1

        max_pool_size = self.pool_config.max_connection_pool_size
        infinite_pool_size = (max_pool_size < 0
                              or max_pool_size == float("inf"))
        with self.lock:
            connections = self.connections[address]
            pool_size = (len(connections)
                         + self.connections_reservations[address])
            if infinite_pool_size or pool_size < max_pool_size:
                # there's room for a new connection
                self.connections_reservations[address] += 1
                return connection_creator
        return None

    def _re_auth_connection(self, connection, auth, force):
        if auth:
            # Assert session auth is supported by the protocol.
            # The Bolt implementation will try as hard as it can to make the
            # re-auth work. So if the session auth token is identical to the
            # driver auth token, the connection doesn't have to do anything so
            # it won't fail regardless of the protocol version.
            connection.assert_re_auth_support()
        new_auth_manager = auth or self.pool_config.auth
        log_auth = "******" if auth else "None"
        new_auth = Util.callback(new_auth_manager.get_auth)
        try:
            updated = connection.re_auth(new_auth, new_auth_manager,
                                         force=force)
            log.debug("[#%04X]  _: <POOL> checked re_auth auth=%s updated=%s "
                      "force=%s",
                      connection.local_port, log_auth, updated, force)
        except Exception as exc:
            log.debug("[#%04X]  _: <POOL> check re_auth failed %r auth=%s "
                      "force=%s",
                      connection.local_port, exc, log_auth, force)
            raise
        assert not force or updated  # force=True implies updated=True
        if force:
            connection.send_all()
            connection.fetch_all()

    def _acquire(
        self, address, auth, deadline, liveness_check_timeout
    ):
        """ Acquire a connection to a given address from the pool.
        The address supplied should always be an IP address, not
        a host name.

        This method is thread safe.
        """
        if auth is None:
            auth = AcquireAuth(None)
        force_auth = auth.force_auth
        auth = auth.auth

        def health_check(connection_, deadline_):
            if (connection_.closed()
                    or connection_.defunct()
                    or connection_.stale()):
                return False
            if liveness_check_timeout is not None:
                if connection_.is_idle_for(liveness_check_timeout):
                    with connection_deadline(connection_, deadline_):
                        try:
                            log.debug("[#%04X]  _: <POOL> liveness check",
                                      connection_.local_port)
                            connection_.reset()
                        except (OSError, ServiceUnavailable, SessionExpired):
                            return False
            return True

        while True:
            # try to find a free connection in the pool
            connection = self._acquire_from_pool_checked(
                address, health_check, deadline
            )
            if connection:
                log.debug("[#%04X]  _: <POOL> picked existing connection %s",
                          connection.local_port, connection.connection_id)
                try:
                    self._re_auth_connection(
                        connection, auth, force_auth
                    )
                except ConfigurationError:
                    if auth:
                        # protocol version lacks support for re-auth
                        # => session auth token is not supported
                        raise
                    # expiring tokens supported by flushing the pool
                    # => give up this connection
                    log.debug("[#%04X]  _: <POOL> backwards compatible "
                              "auth token refresh: purge connection",
                              connection.local_port)
                    connection.close()
                    self.release(connection)
                    continue
                log.debug("[#%04X]  _: <POOL> handing out existing connection",
                          connection.local_port)
                return connection
            # all connections in pool are in-use
            with self.lock:
                connection_creator = self._acquire_new_later(
                    address, auth, deadline,
                )
                if connection_creator:
                    break

                # failed to obtain a connection from pool because the
                # pool is full and no free connection in the pool
                timeout = deadline.to_timeout()
                if (
                    timeout == 0  # deadline expired
                    or not self.cond.wait(timeout)
                ):
                    log.debug("[#0000]  _: <POOL> acquisition timed out")
                    raise ClientError(
                        "failed to obtain a connection from the pool within "
                        "{!r}s (timeout)".format(deadline.original_timeout)
                    )
        log.debug("[#0000]  _: <POOL> trying to hand out new connection")
        return connection_creator()

    @abc.abstractmethod
    def acquire(
        self, access_mode, timeout, database, bookmarks, auth: AcquireAuth,
        liveness_check_timeout
    ):
        """ Acquire a connection to a server that can satisfy a set of parameters.

        :param access_mode:
        :param timeout: timeout for the core acquisition
            (excluding potential preparation like fetching routing tables).
        :param database:
        :param bookmarks:
        :param auth:
        :param liveness_check_timeout:
        """
        ...

    def kill_and_release(self, *connections):
        """ Release connections back into the pool after closing them.

        This method is thread safe.
        """
        for connection in connections:
            if not (connection.defunct()
                    or connection.closed()):
                log.debug(
                    "[#%04X]  _: <POOL> killing connection on release %s",
                    connection.local_port, connection.connection_id
                )
                connection.kill()
        with self.lock:
            for connection in connections:
                connection.in_use = False
            self.cond.notify_all()

    def release(self, *connections):
        """ Release connections back into the pool.

        This method is thread safe.
        """
        cancelled = None
        for connection in connections:
            if not (connection.defunct()
                    or connection.closed()
                    or connection.is_reset):
                if cancelled is not None:
                    log.debug(
                        "[#%04X]  _: <POOL> kill unclean connection %s",
                        connection.local_port, connection.connection_id
                    )
                    connection.kill()
                    continue
                try:
                    log.debug(
                        "[#%04X]  _: <POOL> release unclean connection %s",
                        connection.local_port, connection.connection_id
                    )
                    connection.reset()
                except (Neo4jError, DriverError, BoltError) as exc:
                    log.debug("[#%04X]  _: <POOL> failed to reset connection "
                              "on release: %r", connection.local_port, exc)
                except asyncio.CancelledError as exc:
                    log.debug("[#%04X]  _: <POOL> cancelled reset connection "
                              "on release: %r", connection.local_port, exc)
                    cancelled = exc
                    connection.kill()
        with self.lock:
            for connection in connections:
                connection.in_use = False
                log.debug(
                    "[#%04X]  _: <POOL> released %s",
                    connection.local_port, connection.connection_id
                )
            self.cond.notify_all()
        if cancelled is not None:
            raise cancelled

    def in_use_connection_count(self, address):
        """ Count the number of connections currently in use to a given
        address.
        """
        with self.lock:
            connections = self.connections.get(address, ())
            return sum(connection.in_use for connection in connections)

    def mark_all_stale(self):
        with self.lock:
            for address in self.connections:
                for connection in self.connections[address]:
                    connection.set_stale()

    @classmethod
    def _close_connections(cls, connections):
        cancelled = None
        for connection in connections:
            if cancelled is not None:
                connection.kill()
                continue
            try:
                connection.close()
            except asyncio.CancelledError as e:
                # We've got cancelled: no more time to gracefully close these
                # connections. Time to burn down the place.
                cancelled = e
                connection.kill()
        if cancelled is not None:
            raise cancelled

    def deactivate(self, address):
        """ Deactivate an address from the connection pool, if present, closing
        all idle connection to that address
        """
        with self.lock:
            try:
                connections = self.connections[address]
            except KeyError:  # already removed from the connection pool
                return
            closable_connections = [
                conn for conn in connections if not conn.in_use
            ]
            # First remove all connections in question, then try to close them.
            # If closing of a connection fails, we will end up in this method
            # again.
            for conn in closable_connections:
                connections.remove(conn)
            if not self.connections[address]:
                del self.connections[address]

        self._close_connections(closable_connections)

    def on_write_failure(self, address, database):
        raise WriteServiceUnavailable(
            "No write service available for pool {}".format(self)
        )

    def on_neo4j_error(self, error, connection):
        assert isinstance(error, Neo4jError)
        if error._unauthenticates_all_connections():
            address = connection.unresolved_address
            log.debug(
                "[#0000]  _: <POOL> mark all connections to %r as "
                "unauthenticated", address
            )
            with self.lock:
                for connection in self.connections.get(address, ()):
                    connection.mark_unauthenticated()
        if error._has_security_code():
            handled = Util.callback(
                connection.auth_manager.handle_security_exception,
                connection.auth, error
            )
            if handled:
                error._retryable = True

    def close(self):
        """ Close all connections and empty the pool.
        This method is thread safe.
        """
        log.debug("[#0000]  _: <POOL> close")
        try:
            connections = []
            with self.lock:
                for address in list(self.connections):
                    for connection in self.connections.pop(address, ()):
                        connections.append(connection)
            self._close_connections(connections)
        except TypeError:
            pass


class BoltPool(IOPool):

    is_direct_pool = True

    @classmethod
    def open(cls, address, *, pool_config, workspace_config):
        """Create a new BoltPool

        :param address:
        :param pool_config:
        :param workspace_config:
        :returns: BoltPool
        """

        def opener(addr, auth_manager, deadline):
            return Bolt.open(
                addr, auth_manager=auth_manager, deadline=deadline,
                routing_context=None, pool_config=pool_config
            )

        pool = cls(opener, pool_config, workspace_config, address)
        log.debug("[#0000]  _: <POOL> created, direct address %r", address)
        return pool

    def __init__(self, opener, pool_config, workspace_config, address):
        super().__init__(opener, pool_config, workspace_config)
        self.address = address

    def __repr__(self):
        return "<{} address={!r}>".format(self.__class__.__name__,
                                          self.address)

    def acquire(
        self, access_mode, timeout, database, bookmarks, auth: AcquireAuth,
        liveness_check_timeout
    ):
        # The access_mode and database is not needed for a direct connection,
        # it's just there for consistency.
        log.debug("[#0000]  _: <POOL> acquire direct connection, "
                  "access_mode=%r, database=%r", access_mode, database)
        deadline = Deadline.from_timeout_or_deadline(timeout)
        return self._acquire(
            self.address, auth, deadline, liveness_check_timeout
        )


class Neo4jPool(IOPool):
    """ Connection pool with routing table.
    """

    is_direct_pool = False

    @classmethod
    def open(cls, *addresses, pool_config, workspace_config,
             routing_context=None):
        """Create a new Neo4jPool

        :param addresses: one or more address as positional argument
        :param pool_config:
        :param workspace_config:
        :param routing_context:
        :returns: Neo4jPool
        """

        address = addresses[0]
        if routing_context is None:
            routing_context = {}
        elif "address" in routing_context:
            raise ConfigurationError("The key 'address' is reserved for routing context.")
        routing_context["address"] = str(address)

        def opener(addr, auth_manager, deadline):
            return Bolt.open(
                addr, auth_manager=auth_manager, deadline=deadline,
                routing_context=routing_context, pool_config=pool_config
            )

        pool = cls(opener, pool_config, workspace_config, address)
        log.debug("[#0000]  _: <POOL> created, routing address %r", address)
        return pool

    def __init__(self, opener, pool_config, workspace_config, address):
        """

        :param opener:
        :param pool_config:
        :param workspace_config:
        :param address:
        """
        super().__init__(opener, pool_config, workspace_config)
        # Each database have a routing table, the default database is a special case.
        self.address = address
        self.routing_tables = {}
        self.refresh_lock = RLock()
        self.is_direct_pool = False

    def __repr__(self):
        """ The representation shows the initial routing addresses.

        :returns: The representation
        :rtype: str
        """
        return "<{} address={!r}>".format(self.__class__.__name__,
                                          self.address)

    def get_or_create_routing_table(self, database):
        with self.refresh_lock:
            if database not in self.routing_tables:
                self.routing_tables[database] = RoutingTable(
                    database=database,
                    routers=[self.address]
                )
            return self.routing_tables[database]

    def fetch_routing_info(
        self, address, database, imp_user, bookmarks, auth, acquisition_timeout
    ):
        """ Fetch raw routing info from a given router address.

        :param address: router address
        :param database: the database name to get routing table for
        :param imp_user: the user to impersonate while fetching the routing
                         table
        :type imp_user: str or None
        :param bookmarks: iterable of bookmark values after which the routing
                          info should be fetched
        :param auth: auth
        :param acquisition_timeout: connection acquisition timeout

        :returns: list of routing records, or None if no connection
            could be established or if no readers or writers are present
        :raise ServiceUnavailable: if the server does not support
            routing, or if routing support is broken or outdated
        """
        deadline = Deadline.from_timeout_or_deadline(acquisition_timeout)
        log.debug("[#0000]  _: <POOL> _acquire router connection, "
                  "database=%r, address=%r", database, address)
        if auth:
            auth = copy(auth)
            auth.force_auth = False
        cx = self._acquire(address, auth, deadline, None)
        try:
            routing_table = cx.route(
                database=database or self.workspace_config.database,
                imp_user=imp_user or self.workspace_config.impersonated_user,
                bookmarks=bookmarks
            )
        finally:
            self.release(cx)
        return routing_table

    def fetch_routing_table(
        self, *, address, acquisition_timeout, database, imp_user,
        bookmarks, auth
    ):
        """ Fetch a routing table from a given router address.

        :param address: router address
        :param acquisition_timeout: connection acquisition timeout
        :param database: the database name
        :type: str
        :param imp_user: the user to impersonate while fetching the routing
                         table
        :type imp_user: str or None
        :param bookmarks: bookmarks used when fetching routing table
        :param auth: auth

        :returns: a new RoutingTable instance or None if the given router is
                 currently unable to provide routing information
        """
        new_routing_info = None
        try:
            new_routing_info = self.fetch_routing_info(
                address, database, imp_user, bookmarks, auth,
                acquisition_timeout
            )
        except Neo4jError as e:
            # checks if the code is an error that is caused by the client. In
            # this case there is no sense in trying to fetch a RT from another
            # router. Hence, the driver should fail fast during discovery.
            if e._is_fatal_during_discovery():
                raise
        except (ServiceUnavailable, SessionExpired):
            pass
        if not new_routing_info:
            log.debug("[#0000]  _: <POOL> failed to fetch routing info "
                      "from %r", address)
            return None
        else:
            servers = new_routing_info[0]["servers"]
            ttl = new_routing_info[0]["ttl"]
            database = new_routing_info[0].get("db", database)
            new_routing_table = RoutingTable.parse_routing_info(
                database=database, servers=servers, ttl=ttl
            )

        # Parse routing info and count the number of each type of server
        num_routers = len(new_routing_table.routers)
        num_readers = len(new_routing_table.readers)

        # num_writers = len(new_routing_table.writers)
        # If no writers are available. This likely indicates a temporary state,
        # such as leader switching, so we should not signal an error.

        # No routers
        if num_routers == 0:
            log.debug("[#0000]  _: <POOL> no routing servers returned from "
                      "server %s", address)
            return None

        # No readers
        if num_readers == 0:
            log.debug("[#0000]  _: <POOL> no read servers returned from "
                      "server %s", address)
            return None

        # At least one of each is fine, so return this table
        return new_routing_table

    def _update_routing_table_from(
        self, *routers, database, imp_user, bookmarks, auth,
        acquisition_timeout, database_callback
    ):
        """ Try to update routing tables with the given routers.

        :returns: True if the routing table is successfully updated,
        otherwise False
        """
        if routers:
            log.debug("[#0000]  _: <POOL> attempting to update routing "
                      "table from {}".format(", ".join(map(repr, routers))))
        for router in routers:
            for address in NetworkUtil.resolve_address(
                router, resolver=self.pool_config.resolver
            ):
                new_routing_table = self.fetch_routing_table(
                    address=address, acquisition_timeout=acquisition_timeout,
                    database=database, imp_user=imp_user, bookmarks=bookmarks,
                    auth=auth
                )
                if new_routing_table is not None:
                    new_database = new_routing_table.database
                    old_routing_table = self.get_or_create_routing_table(
                        new_database
                    )
                    old_routing_table.update(new_routing_table)
                    log.debug(
                        "[#0000]  _: <POOL> update routing table from "
                        "address=%r (%r)",
                        address, self.routing_tables[new_database]
                    )
                    if callable(database_callback):
                        database_callback(new_database)
                    return True
            self.deactivate(router)
        return False

    def update_routing_table(
        self, *, database, imp_user, bookmarks, auth=None,
        acquisition_timeout=None, database_callback=None
    ):
        """ Update the routing table from the first router able to provide
        valid routing information.

        :param database: The database name
        :param imp_user: the user to impersonate while fetching the routing
                         table
        :type imp_user: str or None
        :param bookmarks: bookmarks used when fetching routing table
        :param auth: auth
        :param acquisition_timeout: connection acquisition timeout
        :param database_callback: A callback function that will be called with
            the database name as only argument when a new routing table has been
            acquired. This database name might different from `database` if that
            was None and the underlying protocol supports reporting back the
            actual database.

        :raise neo4j.exceptions.ServiceUnavailable:
        """
        with self.refresh_lock:
            routing_table = self.get_or_create_routing_table(database)
            # copied because it can be modified
            existing_routers = set(routing_table.routers)

            prefer_initial_routing_address = \
                self.routing_tables[database].initialized_without_writers

            if prefer_initial_routing_address:
                # TODO: Test this state
                if self._update_routing_table_from(
                    self.address, database=database,
                    imp_user=imp_user, bookmarks=bookmarks, auth=auth,
                    acquisition_timeout=acquisition_timeout,
                    database_callback=database_callback
                ):
                    # Why is only the first initial routing address used?
                    return
            if self._update_routing_table_from(
                *(existing_routers - {self.address}), database=database,
                imp_user=imp_user, bookmarks=bookmarks, auth=auth,
                acquisition_timeout=acquisition_timeout,
                database_callback=database_callback
            ):
                return

            if not prefer_initial_routing_address:
                if self._update_routing_table_from(
                    self.address, database=database,
                    imp_user=imp_user, bookmarks=bookmarks, auth=auth,
                    acquisition_timeout=acquisition_timeout,
                    database_callback=database_callback
                ):
                    # Why is only the first initial routing address used?
                    return

            # None of the routers have been successful, so just fail
            log.error("Unable to retrieve routing information")
            raise ServiceUnavailable("Unable to retrieve routing information")

    def update_connection_pool(self, *, database):
        with self.refresh_lock:
            routing_tables = [self.get_or_create_routing_table(database)]
            for db in self.routing_tables.keys():
                if db == database:
                    continue
                routing_tables.append(self.routing_tables[db])
        servers = set.union(*(rt.servers() for rt in routing_tables))
        for address in list(self.connections):
            if address._unresolved not in servers:
                super(Neo4jPool, self).deactivate(address)

    def ensure_routing_table_is_fresh(
        self, *, access_mode, database, imp_user, bookmarks, auth=None,
        acquisition_timeout=None, database_callback=None
    ):
        """ Update the routing table if stale.

        This method performs two freshness checks, before and after acquiring
        the refresh lock. If the routing table is already fresh on entry, the
        method exits immediately; otherwise, the refresh lock is acquired and
        the second freshness check that follows determines whether an update
        is still required.

        This method is thread-safe.

        :returns: `True` if an update was required, `False` otherwise.
        """
        from ...api import READ_ACCESS
        with self.refresh_lock:
            for database_ in list(self.routing_tables.keys()):
                # Remove unused databases in the routing table
                # Remove the routing table after a timeout = TTL + 30s
                log.debug("[#0000]  _: <POOL> routing aged?, database=%s",
                          database_)
                routing_table = self.routing_tables[database_]
                if routing_table.should_be_purged_from_memory():
                    log.debug("[#0000]  _: <POOL> dropping routing table for "
                              "database=%s", database_)
                    del self.routing_tables[database_]

            routing_table = self.get_or_create_routing_table(database)
            if routing_table.is_fresh(readonly=(access_mode == READ_ACCESS)):
                # table is still valid
                log.debug("[#0000]  _: <POOL> using existing routing table %r",
                          routing_table)
                return False

            self.update_routing_table(
                database=database, imp_user=imp_user, bookmarks=bookmarks,
                auth=auth, acquisition_timeout=acquisition_timeout,
                database_callback=database_callback
            )
            self.update_connection_pool(database=database)

            return True

    def _select_address(self, *, access_mode, database):
        from ...api import READ_ACCESS
        """ Selects the address with the fewest in-use connections.
        """
        with self.refresh_lock:
            routing_table = self.routing_tables.get(database)
            if routing_table:
                if access_mode == READ_ACCESS:
                    addresses = routing_table.readers
                else:
                    addresses = routing_table.writers
            else:
                addresses = ()
            addresses_by_usage = {}
            for address in addresses:
                addresses_by_usage.setdefault(
                    self.in_use_connection_count(address), []
                ).append(address)
        if not addresses_by_usage:
            if access_mode == READ_ACCESS:
                raise ReadServiceUnavailable(
                    "No read service currently available"
                )
            else:
                raise WriteServiceUnavailable(
                    "No write service currently available"
                )
        return choice(addresses_by_usage[min(addresses_by_usage)])

    def acquire(
        self, access_mode, timeout, database, bookmarks,
        auth: t.Optional[AcquireAuth], liveness_check_timeout
    ):
        if access_mode not in (WRITE_ACCESS, READ_ACCESS):
            raise ClientError("Non valid 'access_mode'; {}".format(access_mode))
        if not timeout:
            raise ClientError("'timeout' must be a float larger than 0; {}"
                              .format(timeout))


        from ...api import check_access_mode
        access_mode = check_access_mode(access_mode)
        #     await self.ensure_routing_table_is_fresh(
        #         access_mode=access_mode, database=database, imp_user=None,
        #         bookmarks=bookmarks, acquisition_timeout=timeout
        #     )

        log.debug("[#0000]  _: <POOL> acquire routing connection, "
                  "access_mode=%r, database=%r", access_mode, database)
        self.ensure_routing_table_is_fresh(
            access_mode=access_mode, database=database,
            imp_user=None, bookmarks=bookmarks, auth=auth,
            acquisition_timeout=timeout
        )

        while True:
            try:
                # Get an address for a connection that have the fewest in-use
                # connections.
                address = self._select_address(
                    access_mode=access_mode, database=database
                )
            except (ReadServiceUnavailable, WriteServiceUnavailable) as err:
                raise SessionExpired("Failed to obtain connection towards '%s' server." % access_mode) from err
            try:
                log.debug("[#0000]  _: <POOL> acquire address, database=%r "
                          "address=%r", database, address)
                deadline = Deadline.from_timeout_or_deadline(timeout)
                # should always be a resolved address
                connection = self._acquire(
                    address, auth, deadline, liveness_check_timeout,
                )
            except (ServiceUnavailable, SessionExpired):
                self.deactivate(address=address)
            else:
                return connection

    def deactivate(self, address):
        """ Deactivate an address from the connection pool,
        if present, remove from the routing table and also closing
        all idle connections to that address.
        """
        log.debug("[#0000]  _: <POOL> deactivating address %r", address)
        # We use `discard` instead of `remove` here since the former
        # will not fail if the address has already been removed.
        with self.refresh_lock:
            for database in self.routing_tables.keys():
                self.routing_tables[database].routers.discard(address)
                self.routing_tables[database].readers.discard(address)
                self.routing_tables[database].writers.discard(address)
        log.debug("[#0000]  _: <POOL> table=%r", self.routing_tables)
        super(Neo4jPool, self).deactivate(address)

    def on_write_failure(self, address, database):
        """ Remove a writer address from the routing table, if present.
        """
        log.debug("[#0000]  _: <POOL> removing writer %r for database %r",
                  address, database)
        with self.refresh_lock:
            table = self.routing_tables.get(database)
            if table is not None:
                self.routing_tables[database].writers.discard(address)
        log.debug("[#0000]  _: <POOL> table=%r", self.routing_tables)
