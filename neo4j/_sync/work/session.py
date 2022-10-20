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
import typing as t
from logging import getLogger
from random import random
from time import perf_counter


if t.TYPE_CHECKING:
    import typing_extensions as te

    from ..io import Bolt

    _R = t.TypeVar("_R")
    _P = te.ParamSpec("_P")

from ..._async_compat import sleep
from ..._async_compat.util import Util
from ..._conf import SessionConfig
from ..._meta import deprecated
from ...api import (
    Bookmarks,
    READ_ACCESS,
    WRITE_ACCESS,
)
from ...exceptions import (
    ClientError,
    DriverError,
    Neo4jError,
    ServiceUnavailable,
    SessionExpired,
    TransactionError,
)
from ...work import Query
from .result import Result
from .transaction import (
    ManagedTransaction,
    Transaction,
)
from .workspace import Workspace


log = getLogger("neo4j")


class Session(Workspace):
    """A :class:`.Session` is a logical context for transactional units
    of work. Connections are drawn from the :class:`.Driver` connection
    pool as required.

    Session creation is a lightweight operation and sessions are not safe to
    be used in concurrent contexts (multiple threads/coroutines).
    Therefore, a session should generally be short-lived, and must not
    span multiple threads/asynchronous Tasks.

    In general, sessions will be created and destroyed within a `with`
    context. For example::

        with driver.session() as session:
            result = session.run("MATCH (n:Person) RETURN n.name AS name")
            # do something with the result...

    :param pool: connection pool instance
    :param config: session config instance
    """

    # The current connection.
    _connection: t.Optional[Bolt] = None

    # The current transaction instance, if any.
    _transaction: t.Union[Transaction, ManagedTransaction, None] = \
        None

    # The current auto-transaction result, if any.
    _auto_result = None

    # The state this session is in.
    _state_failed = False

    def __init__(self, pool, session_config):
        assert isinstance(session_config, SessionConfig)
        super().__init__(pool, session_config)
        self._initialize_bookmarks(session_config.bookmarks)
        self._bookmark_manager = session_config.bookmark_manager

    def __enter__(self) -> Session:
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        if exception_type:
            if issubclass(exception_type, asyncio.CancelledError):
                self._handle_cancellation(message="__exit__")
                self._closed = True
                return
            self._state_failed = True
        self.close()

    def _connect(self, access_mode, **access_kwargs):
        if access_mode is None:
            access_mode = self._config.default_access_mode
        try:
            super()._connect(access_mode, **access_kwargs)
        except asyncio.CancelledError:
            self._handle_cancellation(message="_connect")
            raise

    def _disconnect(self, sync=False):
        try:
            return super()._disconnect(sync=sync)
        except asyncio.CancelledError:
            self._handle_cancellation(message="_disconnect")
            raise

    def _handle_cancellation(self, message="General"):
        self._transaction = None
        self._auto_result = None
        connection = self._connection
        self._connection = None
        if connection:
            log.debug("[#%04X]  %s cancellation clean-up",
                      connection.local_port, message)
            self._pool.kill_and_release(connection)
        else:
            log.debug("[#0000]  %s cancellation clean-up", message)

    def _result_closed(self):
        if self._auto_result:
            self._update_bookmark(self._auto_result._database,
                                        self._auto_result._bookmark)
            self._auto_result = None
            self._disconnect()

    def _result_error(self, error):
        if isinstance(error, asyncio.CancelledError):
            return self._handle_cancellation(message="_result_error")
        if self._auto_result:
            self._auto_result = None
            self._disconnect()

    def _get_server_info(self):
        assert not self._connection
        self._connect(READ_ACCESS, liveness_check_timeout=0)
        server_info = self._connection.server_info
        self._disconnect()
        return server_info

    def close(self) -> None:
        """Close the session.

        This will release any borrowed resources, such as connections, and will
        roll back any outstanding transactions.
        """
        if self._closed:
            return
        if self._connection:
            if self._auto_result:
                if self._state_failed is False:
                    try:
                        self._auto_result.consume()
                        self._update_bookmark(
                            self._auto_result._database,
                            self._auto_result._bookmark
                        )
                    except Exception as error:
                        # TODO: Investigate potential non graceful close states
                        self._auto_result = None
                        self._state_failed = True

            if self._transaction:
                if self._transaction._closed() is False:
                    # roll back the transaction if it is not closed
                    self._transaction._rollback()
                self._transaction = None

            try:
                if self._connection:
                    self._connection.send_all()
                    self._connection.fetch_all()
                    # TODO: Investigate potential non graceful close states
            except Neo4jError:
                pass
            except TransactionError:
                pass
            except ServiceUnavailable:
                pass
            except SessionExpired:
                pass
            finally:
                self._disconnect()

            self._state_failed = False
        self._closed = True

    if Util.is_async_code:
        def cancel(self) -> None:
            """Cancel this session.

            If the session is already closed, this method does nothing.
            Else, it will if present, forcefully close the connection the
            session holds. This will violently kill all work in flight.

            The primary purpose of this function is to handle
            :class:`asyncio.CancelledError`.

            ::

                session = driver.session()
                try:
                    ...  # do some work
                except asyncio.CancelledError:
                    session.cancel()
                    raise

            """
            self._handle_cancellation(message="manual cancel")

    def run(
        self,
        query: t.Union[str, Query],
        parameters: t.Dict[str, t.Any] = None,
        **kwargs: t.Any
    ) -> Result:
        """Run a Cypher query within an auto-commit transaction.

        The query is sent and the result header received
        immediately but the :class:`neo4j.Result` content is
        fetched lazily as consumed by the client application.

        If a query is executed before a previous
        :class:`neo4j.Result` in the same :class:`.Session` has
        been fully consumed, the first result will be fully fetched
        and buffered. Note therefore that the generally recommended
        pattern of usage is to fully consume one result before
        executing a subsequent query. If two results need to be
        consumed in parallel, multiple :class:`.Session` objects
        can be used as an alternative to result buffering.

        For more usage details, see :meth:`.Transaction.run`.

        :param query: cypher query
        :param parameters: dictionary of parameters
        :param kwargs: additional keyword parameters.
            These take precedence over parameters passed as ``parameters``.

        :raises SessionError: if the session has been closed.

        :returns: a new :class:`neo4j.Result` object
        """
        self._check_state()
        if not query:
            raise ValueError("Cannot run an empty query")
        if not isinstance(query, (str, Query)):
            raise TypeError("query must be a string or a Query instance")

        if self._transaction:
            raise ClientError("Explicit Transaction must be handled explicitly")

        if self._auto_result:
            # This will buffer upp all records for the previous auto-transaction
            self._auto_result._buffer_all()

        if not self._connection:
            self._connect(self._config.default_access_mode)
        cx = self._connection

        self._auto_result = Result(
            cx, self._config.fetch_size, self._result_closed,
            self._result_error
        )
        bookmarks = self._get_all_bookmarks()
        parameters = dict(parameters or {}, **kwargs)
        self._auto_result._run(
            query, parameters, self._config.database,
            self._config.impersonated_user, self._config.default_access_mode,
            bookmarks
        )

        return self._auto_result

    @deprecated(
        "`last_bookmark` has been deprecated in favor of `last_bookmarks`. "
        "This method can lead to unexpected behaviour."
    )
    def last_bookmark(self) -> t.Optional[str]:
        """Return the bookmark received following the last completed transaction.

        Note: For auto-transactions (:meth:`Session.run`), this will trigger
        :meth:`Result.consume` for the current result.

        .. warning::
            This method can lead to unexpected behaviour if the session has not
            yet successfully completed a transaction.

        .. deprecated:: 5.0
            :meth:`last_bookmark` will be removed in version 6.0.
            Use :meth:`last_bookmarks` instead.

        :returns: last bookmark
        """
        # The set of bookmarks to be passed into the next transaction.

        if self._auto_result:
            self._auto_result.consume()

        if self._transaction and self._transaction._closed:
            self._update_bookmark(self._transaction._database,
                                        self._transaction._bookmark)
            self._transaction = None

        if self._bookmarks:
            return self._bookmarks[-1]
        return None

    def last_bookmarks(self) -> Bookmarks:
        """Return most recent bookmarks of the session.

        Bookmarks can be used to causally chain sessions. For example,
        if a session (``session1``) wrote something, that another session
        (``session2``) needs to read, use
        ``session2 = driver.session(bookmarks=session1.last_bookmarks())`` to
        achieve this.

        Combine the bookmarks of multiple sessions like so::

            bookmarks1 = session1.last_bookmarks()
            bookmarks2 = session2.last_bookmarks()
            session3 = driver.session(bookmarks=bookmarks1 + bookmarks2)

        A session automatically manages bookmarks, so this method is rarely
        needed. If you need causal consistency, try to run the relevant queries
        in the same session.

        "Most recent bookmarks" are either the bookmarks passed to the session
        or creation, or the last bookmark the session received after committing
        a transaction to the server.

        Note: For auto-transactions (:meth:`Session.run`), this will trigger
        :meth:`Result.consume` for the current result.

        :returns: the session's last known bookmarks
        """
        # The set of bookmarks to be passed into the next transaction.

        if self._auto_result:
            self._auto_result.consume()

        if self._transaction and self._transaction._closed():
            self._update_bookmark(self._transaction._database,
                                        self._transaction._bookmark)
            self._transaction = None

        return Bookmarks.from_raw_values(self._bookmarks)

    def _transaction_closed_handler(self):
        if self._transaction:
            self._update_bookmark(self._transaction._database,
                                        self._transaction._bookmark)
            self._transaction = None
            self._disconnect()

    def _transaction_error_handler(self, error):
        if self._transaction:
            self._transaction = None
            self._disconnect()

    def _transaction_cancel_handler(self):
        return self._handle_cancellation(
            message="_transaction_cancel_handler"
        )

    def _open_transaction(
        self, *, tx_cls, access_mode, metadata=None, timeout=None
    ):
        self._connect(access_mode=access_mode)
        self._transaction = tx_cls(
            self._connection, self._config.fetch_size,
            self._transaction_closed_handler,
            self._transaction_error_handler,
            self._transaction_cancel_handler
        )
        bookmarks = self._get_all_bookmarks()
        self._transaction._begin(
            self._config.database, self._config.impersonated_user,
            bookmarks, access_mode, metadata, timeout
        )

    def begin_transaction(
        self,
        metadata: t.Dict[str, t.Any] = None,
        timeout: float = None
    ) -> Transaction:
        """ Begin a new unmanaged transaction. Creates a new :class:`.Transaction` within this session.
            At most one transaction may exist in a session at any point in time.
            To maintain multiple concurrent transactions, use multiple concurrent sessions.

            Note: For auto-transaction (Session.run) this will trigger a consume for the current result.

        :param metadata:
            a dictionary with metadata.
            Specified metadata will be attached to the executing transaction and visible in the output of ``dbms.listQueries`` and ``dbms.listTransactions`` procedures.
            It will also get logged to the ``query.log``.
            This functionality makes it easier to tag transactions and is equivalent to ``dbms.setTXMetaData`` procedure, see https://neo4j.com/docs/operations-manual/current/reference/procedures/ for procedure reference.

        :param timeout:
            the transaction timeout in seconds.
            Transactions that execute longer than the configured timeout will be terminated by the database.
            This functionality allows to limit query/transaction execution time.
            Specified timeout overrides the default timeout configured in the database using ``dbms.transaction.timeout`` setting.
            Value should not represent a duration of zero or negative duration.

        :raises TransactionError: if a transaction is already open.
        :raises SessionError: if the session has been closed.

        :returns: A new transaction instance.
        """
        self._check_state()
        # TODO: Implement TransactionConfig consumption

        if self._auto_result:
            self._auto_result.consume()

        if self._transaction:
            raise TransactionError(
                self._transaction, "Explicit transaction already open"
            )

        self._open_transaction(
            tx_cls=Transaction,
            access_mode=self._config.default_access_mode, metadata=metadata,
            timeout=timeout
        )

        return t.cast(Transaction, self._transaction)

    def _run_transaction(
        self, access_mode, transaction_function, *args, **kwargs
    ):
        self._check_state()
        if not callable(transaction_function):
            raise TypeError("Unit of work is not callable")

        metadata = getattr(transaction_function, "metadata", None)
        timeout = getattr(transaction_function, "timeout", None)

        retry_delay = retry_delay_generator(
            self._config.initial_retry_delay,
            self._config.retry_delay_multiplier,
            self._config.retry_delay_jitter_factor
        )

        errors = []

        t0 = -1  # Timer

        while True:
            try:
                self._open_transaction(
                    tx_cls=ManagedTransaction,
                    access_mode=access_mode, metadata=metadata,
                    timeout=timeout
                )
                tx = self._transaction
                try:
                    result = transaction_function(tx, *args, **kwargs)
                except asyncio.CancelledError:
                    # if cancellation callback has not been called yet:
                    if self._transaction is not None:
                        self._handle_cancellation(
                            message="transaction function"
                        )
                    raise
                except Exception:
                    tx._close()
                    raise
                else:
                    tx._commit()
            except (DriverError, Neo4jError) as error:
                self._disconnect()
                if not error.is_retryable():
                    raise
                errors.append(error)
            else:
                return result
            if t0 == -1:
                # The timer should be started after the first attempt
                t0 = perf_counter()
            t1 = perf_counter()
            if t1 - t0 > self._config.max_transaction_retry_time:
                break
            delay = next(retry_delay)
            log.warning("Transaction failed and will be retried in {}s ({})"
                        "".format(delay, "; ".join(errors[-1].args)))
            try:
                sleep(delay)
            except asyncio.CancelledError:
                log.debug("[#0000]  Retry cancelled")
                raise

        if errors:
            raise errors[-1]
        else:
            raise ServiceUnavailable("Transaction failed")

    def execute_read(
        self,
        transaction_function: t.Callable[
            te.Concatenate[ManagedTransaction, _P], t.Union[_R]
        ],
        *args: _P.args, **kwargs: _P.kwargs
    ) -> _R:
        """Execute a unit of work in a managed read transaction.

        .. note::
            This does not necessarily imply access control, see the session
            configuration option :ref:`default-access-mode-ref`.

        This transaction will automatically be committed when the function
        returns, unless an exception is thrown during query execution or by
        the user code. Note, that this function performs retries and that the
        supplied `transaction_function` might get invoked more than once.
        Therefore, it needs to be idempotent (i.e., have the same effect,
        regardless if called once or many times).

        Example::

            def do_cypher_tx(tx, cypher):
                result = tx.run(cypher)
                values = [record.values() for record in result]
                return values

            with driver.session() as session:
                values = session.execute_read(do_cypher_tx, "RETURN 1 AS x")

        Example::

            def get_two_tx(tx):
                result = tx.run("UNWIND [1,2,3,4] AS x RETURN x")
                values = []
                for record in result:
                    if len(values) >= 2:
                        break
                    values.append(record.values())
                # or shorter: values = [record.values()
                #                       for record in result.fetch(2)]

                # discard the remaining records if there are any
                summary = result.consume()
                # use the summary for logging etc.
                return values

            with driver.session() as session:
                values = session.execute_read(get_two_tx)

        :param transaction_function: a function that takes a transaction as an
            argument and does work with the transaction.
            `transaction_function(tx, *args, **kwargs)` where `tx` is a
            :class:`.ManagedTransaction`.
        :param args: additional arguments for the `transaction_function`
        :param kwargs: key word arguments for the `transaction_function`

        :raises SessionError: if the session has been closed.

        :return: a result as returned by the given unit of work

        .. versionadded:: 5.0
        """
        return self._run_transaction(
            READ_ACCESS, transaction_function, *args, **kwargs
        )

    # TODO 6.0: Remove this method
    @deprecated("read_transaction has been renamed to execute_read")
    def read_transaction(
        self,
        transaction_function: t.Callable[
            te.Concatenate[ManagedTransaction, _P], t.Union[_R]
        ],
        *args: _P.args, **kwargs: _P.kwargs
    ) -> _R:
        """Execute a unit of work in a managed read transaction.

        .. note::
            This does not necessarily imply access control, see the session
            configuration option :ref:`default-access-mode-ref`.

        This transaction will automatically be committed when the function
        returns, unless an exception is thrown during query execution or by
        the user code. Note, that this function performs retries and that the
        supplied `transaction_function` might get invoked more than once.
        Therefore, it needs to be idempotent (i.e., have the same effect,
        regardless if called once or many times).

        Example::

            def do_cypher_tx(tx, cypher):
                result = tx.run(cypher)
                values = [record.values() for record in result]
                return values

            with driver.session() as session:
                values = session.read_transaction(do_cypher_tx, "RETURN 1 AS x")

        Example::

            def get_two_tx(tx):
                result = tx.run("UNWIND [1,2,3,4] AS x RETURN x")
                values = []
                for record in result:
                    if len(values) >= 2:
                        break
                    values.append(record.values())
                # or shorter: values = [record.values()
                #                       for record in result.fetch(2)]

                # discard the remaining records if there are any
                summary = result.consume()
                # use the summary for logging etc.
                return values

            with driver.session() as session:
                values = session.read_transaction(get_two_tx)

        :param transaction_function: a function that takes a transaction as an
            argument and does work with the transaction.
            `transaction_function(tx, *args, **kwargs)` where `tx` is a
            :class:`.ManagedTransaction`.
        :param args: additional arguments for the `transaction_function`
        :param kwargs: key word arguments for the `transaction_function`

        :raises SessionError: if the session has been closed.

        :return: a result as returned by the given unit of work

        .. deprecated:: 5.0
            Method was renamed to :meth:`.execute_read`.
        """
        return self._run_transaction(
            READ_ACCESS, transaction_function, *args, **kwargs
        )

    def execute_write(
        self,
        transaction_function: t.Callable[
            te.Concatenate[ManagedTransaction, _P], t.Union[_R]
        ],
        *args: _P.args,  **kwargs: _P.kwargs
    ) -> _R:
        """Execute a unit of work in a managed write transaction.

        .. note::
            This does not necessarily imply access control, see the session
            configuration option :ref:`default-access-mode-ref`.

        This transaction will automatically be committed when the function
        returns unless, an exception is thrown during query execution or by
        the user code. Note, that this function performs retries and that the
        supplied `transaction_function` might get invoked more than once.
        Therefore, it needs to be idempotent (i.e., have the same effect,
        regardless if called once or many times).

        Example::

            def create_node_tx(tx, name):
                query = "CREATE (n:NodeExample { name: $name }) RETURN id(n) AS node_id"
                result = tx.run(query, name=name)
                record = result.single()
                return record["node_id"]

            with driver.session() as session:
                node_id = session.execute_write(create_node_tx, "example")

        :param transaction_function: a function that takes a transaction as an
            argument and does work with the transaction.
            `transaction_function(tx, *args, **kwargs)` where `tx` is a
            :class:`.ManagedTransaction`.
        :param args: additional arguments for the `transaction_function`
        :param kwargs: key word arguments for the `transaction_function`

        :raises SessionError: if the session has been closed.

        :return: a result as returned by the given unit of work

        .. versionadded:: 5.0
        """
        return self._run_transaction(
            WRITE_ACCESS, transaction_function, *args, **kwargs
        )

    # TODO 6.0: Remove this method
    @deprecated("write_transaction has been renamed to execute_write")
    def write_transaction(
        self,
        transaction_function: t.Callable[
            te.Concatenate[ManagedTransaction, _P], t.Union[_R]
        ],
        *args: _P.args,  **kwargs: _P.kwargs
    ) -> _R:
        """Execute a unit of work in a managed write transaction.

        .. note::
            This does not necessarily imply access control, see the session
            configuration option :ref:`default-access-mode-ref`.

        This transaction will automatically be committed when the function
        returns unless, an exception is thrown during query execution or by
        the user code. Note, that this function performs retries and that the
        supplied `transaction_function` might get invoked more than once.
        Therefore, it needs to be idempotent (i.e., have the same effect,
        regardless if called once or many times).

        Example::

            def create_node_tx(tx, name):
                query = "CREATE (n:NodeExample { name: $name }) RETURN id(n) AS node_id"
                result = tx.run(query, name=name)
                record = result.single()
                return record["node_id"]

            with driver.session() as session:
                node_id = session.write_transaction(create_node_tx, "example")

        :param transaction_function: a function that takes a transaction as an
            argument and does work with the transaction.
            `transaction_function(tx, *args, **kwargs)` where `tx` is a
            :class:`.ManagedTransaction`.
        :param args: additional arguments for the `transaction_function`
        :param kwargs: key word arguments for the `transaction_function`

        :raises SessionError: if the session has been closed.

        :return: a result as returned by the given unit of work

        .. deprecated:: 5.0
            Method was renamed to :meth:`.execute_write`.
        """
        return self._run_transaction(
            WRITE_ACCESS, transaction_function, *args, **kwargs
        )


def retry_delay_generator(initial_delay, multiplier, jitter_factor):
    delay = initial_delay
    while True:
        jitter = jitter_factor * delay
        yield delay - jitter + (2 * jitter * random())
        delay *= multiplier
