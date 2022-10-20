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
from functools import wraps

from ..._async_compat.util import AsyncUtil
from ...exceptions import TransactionError
from ...work import Query
from ..io import ConnectionErrorHandler
from .result import AsyncResult


__all__ = (
    "AsyncManagedTransaction",
    "AsyncTransaction",
    "AsyncTransactionBase",
)


class AsyncTransactionBase:
    def __init__(self, connection, fetch_size, on_closed, on_error,
                 on_cancel):
        self._connection = connection
        self._error_handling_connection = ConnectionErrorHandler(
            connection, self._error_handler
        )
        self._bookmark = None
        self._database = None
        self._results = []
        self._closed_flag = False
        self._last_error = None
        self._fetch_size = fetch_size
        self._on_closed = on_closed
        self._on_error = on_error
        self._on_cancel = on_cancel

    async def _enter(self):
        return self

    async def _exit(self, exception_type, exception_value, traceback):
        if self._closed_flag:
            return
        success = not bool(exception_type)
        if success:
            await self._commit()
        elif issubclass(exception_type, asyncio.CancelledError):
            self._cancel()
            return
        await self._close()

    async def _begin(
        self, database, imp_user, bookmarks, access_mode, metadata, timeout
    ):
        self._database = database
        self._connection.begin(
            bookmarks=bookmarks, metadata=metadata, timeout=timeout,
            mode=access_mode, db=database, imp_user=imp_user
        )
        await self._error_handling_connection.send_all()
        await self._error_handling_connection.fetch_all()

    async def _result_on_closed_handler(self):
        pass

    async def _error_handler(self, exc):
        self._last_error = exc
        if isinstance(exc, asyncio.CancelledError):
            self._cancel()
            return
        await AsyncUtil.callback(self._on_error, exc)

    async def _consume_results(self):
        for result in self._results:
            await result._tx_end()
        self._results = []

    async def run(
        self,
        query: str,
        parameters: t.Dict[str, t.Any] = None,
        **kwparameters: t.Any
    ) -> AsyncResult:
        """ Run a Cypher query within the context of this transaction.

        Cypher is typically expressed as a query template plus a
        set of named parameters. In Python, parameters may be expressed
        through a dictionary of parameters, through individual parameter
        arguments, or as a mixture of both. For example, the `run`
        queries below are all equivalent::

            >>> query = "CREATE (a:Person { name: $name, age: $age })"
            >>> result = await tx.run(query, {"name": "Alice", "age": 33})
            >>> result = await tx.run(query, {"name": "Alice"}, age=33)
            >>> result = await tx.run(query, name="Alice", age=33)

        Parameter values can be of any type supported by the Neo4j type
        system. In Python, this includes :class:`bool`, :class:`int`,
        :class:`str`, :class:`list` and :class:`dict`. Note however that
        :class:`list` properties must be homogenous.

        :param query: cypher query
        :param parameters: dictionary of parameters
        :param kwparameters: additional keyword parameters.
            These take precedence over parameters passed as ``parameters``.

        :raise TransactionError: if the transaction is already closed

        :returns: a new :class:`neo4j.AsyncResult` object
        """
        if isinstance(query, Query):
            raise ValueError("Query object is only supported for session.run")

        if self._closed_flag:
            raise TransactionError(self, "Transaction closed")
        if self._last_error:
            raise TransactionError(self,
                                   "Transaction failed") from self._last_error

        if (self._results
                and self._connection.supports_multiple_results is False):
            # Bolt 3 Support
            # Buffer up all records for the previous Result because it does not
            # have any qid to fetch in batches.
            await self._results[-1]._buffer_all()

        result = AsyncResult(
            self._connection, self._fetch_size, self._result_on_closed_handler,
            self._error_handler
        )
        self._results.append(result)

        parameters = dict(parameters or {}, **kwparameters)
        await result._tx_ready_run(query, parameters)

        return result

    async def _commit(self):
        """Mark this transaction as successful and close in order to trigger a COMMIT.

        :raise TransactionError: if the transaction is already closed
        """
        if self._closed_flag:
            raise TransactionError(self, "Transaction closed")
        if self._last_error:
            raise TransactionError(self,
                                   "Transaction failed") from self._last_error

        metadata = {}
        try:
            # DISCARD pending records then do a commit.
            await self._consume_results()
            self._connection.commit(on_success=metadata.update)
            await self._connection.send_all()
            await self._connection.fetch_all()
            self._bookmark = metadata.get("bookmark")
            self._database = metadata.get("db", self._database)
        except asyncio.CancelledError:
            self._on_cancel()
            raise
        finally:
            self._closed_flag = True
            await AsyncUtil.callback(self._on_closed)

        return self._bookmark

    async def _rollback(self):
        """Mark this transaction as unsuccessful and close in order to trigger a ROLLBACK.

        :raise TransactionError: if the transaction is already closed
        """
        if self._closed_flag:
            raise TransactionError(self, "Transaction closed")

        metadata = {}
        try:
            if not (self._connection.defunct()
                    or self._connection.closed()
                    or self._connection.is_reset):
                # DISCARD pending records then do a rollback.
                await self._consume_results()
                self._connection.rollback(on_success=metadata.update)
                await self._connection.send_all()
                await self._connection.fetch_all()
        except asyncio.CancelledError:
            self._on_cancel()
            raise
        finally:
            self._closed_flag = True
            await AsyncUtil.callback(self._on_closed)

    async def _close(self):
        """Close this transaction, triggering a ROLLBACK if not closed.
        """
        if self._closed_flag:
            return
        await self._rollback()

    if AsyncUtil.is_async_code:
        def _cancel(self) -> None:
            """Cancel this transaction.

            If the transaction is already closed, this method does nothing.
            Else, it will close the connection without ROLLBACK or COMMIT in
            a non-blocking manner.

            The primary purpose of this function is to handle
            :class:`asyncio.CancelledError`.

            ::

                tx = await session.begin_transaction()
                try:
                    ...  # do some work
                except asyncio.CancelledError:
                    tx.cancel()
                    raise

            """
            if self._closed_flag:
                return
            try:
                self._on_cancel()
            finally:
                self._closed_flag = True

    def _closed(self):
        """Indicate whether the transaction has been closed or cancelled.

        :return:
            :const:`True` if closed or cancelled, :const:`False` otherwise.
        :rtype: bool
        """
        return self._closed_flag


class AsyncTransaction(AsyncTransactionBase):
    """ Container for multiple Cypher queries to be executed within a single
    context. :class:`AsyncTransaction` objects can be used as a context
    managers (:py:const:`async with` block) where the transaction is committed
    or rolled back on based on whether an exception is raised::

        async with await session.begin_transaction() as tx:
            ...

    """

    @wraps(AsyncTransactionBase._enter)
    async def __aenter__(self) -> AsyncTransaction:
        return await self._enter()

    @wraps(AsyncTransactionBase._exit)
    async def __aexit__(self, exception_type, exception_value, traceback):
        await self._exit(exception_type, exception_value, traceback)

    @wraps(AsyncTransactionBase._commit)
    async def commit(self) -> None:
        return await self._commit()

    @wraps(AsyncTransactionBase._rollback)
    async def rollback(self) -> None:
        return await self._rollback()

    @wraps(AsyncTransactionBase._close)
    async def close(self) -> None:
        return await self._close()

    @wraps(AsyncTransactionBase._closed)
    def closed(self) -> bool:
        return self._closed()

    if AsyncUtil.is_async_code:
        @wraps(AsyncTransactionBase._cancel)
        def cancel(self) -> None:
            return self._cancel()


class AsyncManagedTransaction(AsyncTransactionBase):
    """Transaction object provided to transaction functions.

    Inside a transaction function, the driver is responsible for managing
    (committing / rolling back) the transaction. Therefore,
    AsyncManagedTransactions don't offer such methods.
    Otherwise, they behave like :class:`.AsyncTransaction`.

    * To commit the transaction,
      return anything from the transaction function.
    * To rollback the transaction, raise any exception.

    Note that transaction functions have to be idempotent (i.e., the result
    of running the function once has to be the same as running it any number
    of times). This is, because the driver will retry the transaction function
    if the error is classified as retryable.

    .. versionadded:: 5.0

        Prior, transaction functions used :class:`AsyncTransaction` objects,
        but would cause hard to interpret errors when managed explicitly
        (committed or rolled back by user code).
    """
    pass
