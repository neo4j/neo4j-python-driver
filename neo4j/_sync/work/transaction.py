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

from ..._async_compat.util import Util
from ...exceptions import TransactionError
from ...work import Query
from ..io import ConnectionErrorHandler
from .result import Result


if t.TYPE_CHECKING:
    import typing_extensions as te


__all__ = (
    "ManagedTransaction",
    "Transaction",
    "TransactionBase",
)


class TransactionBase:
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

    def _enter(self):
        return self

    def _exit(self, exception_type, exception_value, traceback):
        if self._closed_flag:
            return
        success = not bool(exception_type)
        if success:
            self._commit()
        elif issubclass(exception_type, asyncio.CancelledError):
            self._cancel()
            return
        self._close()

    def _begin(
        self, database, imp_user, bookmarks, access_mode, metadata, timeout
    ):
        self._database = database
        self._connection.begin(
            bookmarks=bookmarks, metadata=metadata, timeout=timeout,
            mode=access_mode, db=database, imp_user=imp_user
        )
        self._error_handling_connection.send_all()
        self._error_handling_connection.fetch_all()

    def _result_on_closed_handler(self):
        pass

    def _error_handler(self, exc):
        self._last_error = exc
        if isinstance(exc, asyncio.CancelledError):
            self._cancel()
            return
        Util.callback(self._on_error, exc)

    def _consume_results(self):
        for result in self._results:
            result._tx_end()
        self._results = []

    def run(
        self,
        query: te.LiteralString,
        parameters: t.Optional[t.Dict[str, t.Any]] = None,
        **kwparameters: t.Any
    ) -> Result:
        """ Run a Cypher query within the context of this transaction.

        Cypher is typically expressed as a query template plus a
        set of named parameters. In Python, parameters may be expressed
        through a dictionary of parameters, through individual parameter
        arguments, or as a mixture of both. For example, the `run`
        queries below are all equivalent::

            >>> query = "CREATE (a:Person { name: $name, age: $age })"
            >>> result = tx.run(query, {"name": "Alice", "age": 33})
            >>> result = tx.run(query, {"name": "Alice"}, age=33)
            >>> result = tx.run(query, name="Alice", age=33)

        Parameter values can be of any type supported by the Neo4j type
        system. In Python, this includes :class:`bool`, :class:`int`,
        :class:`str`, :class:`list` and :class:`dict`. Note however that
        :class:`list` properties must be homogenous.

        :param query: cypher query
        :param parameters: dictionary of parameters
        :param kwparameters: additional keyword parameters.
            These take precedence over parameters passed as ``parameters``.

        :raise TransactionError: if the transaction is already closed

        :returns: a new :class:`neo4j.Result` object
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
            self._results[-1]._buffer_all()

        result = Result(
            self._connection, self._fetch_size, self._result_on_closed_handler,
            self._error_handler
        )
        self._results.append(result)

        parameters = dict(parameters or {}, **kwparameters)
        result._tx_ready_run(query, parameters)

        return result

    def _commit(self):
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
            self._consume_results()
            self._connection.commit(on_success=metadata.update)
            self._connection.send_all()
            self._connection.fetch_all()
            self._bookmark = metadata.get("bookmark")
            self._database = metadata.get("db", self._database)
        except asyncio.CancelledError:
            self._on_cancel()
            raise
        finally:
            self._closed_flag = True
            Util.callback(self._on_closed)

        return self._bookmark

    def _rollback(self):
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
                self._consume_results()
                self._connection.rollback(on_success=metadata.update)
                self._connection.send_all()
                self._connection.fetch_all()
        except asyncio.CancelledError:
            self._on_cancel()
            raise
        finally:
            self._closed_flag = True
            Util.callback(self._on_closed)

    def _close(self):
        """Close this transaction, triggering a ROLLBACK if not closed.
        """
        if self._closed_flag:
            return
        self._rollback()

    if Util.is_async_code:
        def _cancel(self) -> None:
            """Cancel this transaction.

            If the transaction is already closed, this method does nothing.
            Else, it will close the connection without ROLLBACK or COMMIT in
            a non-blocking manner.

            The primary purpose of this function is to handle
            :class:`asyncio.CancelledError`.

            ::

                tx = session.begin_transaction()
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

        :returns:
            :const:`True` if closed or cancelled, :const:`False` otherwise.
        :rtype: bool
        """
        return self._closed_flag


class Transaction(TransactionBase):
    """ Container for multiple Cypher queries to be executed within a single
    context. :class:`Transaction` objects can be used as a context
    managers (:py:const:`with` block) where the transaction is committed
    or rolled back on based on whether an exception is raised::

        with session.begin_transaction() as tx:
            ...

    """

    @wraps(TransactionBase._enter)
    def __enter__(self) -> Transaction:
        return self._enter()

    @wraps(TransactionBase._exit)
    def __exit__(self, exception_type, exception_value, traceback):
        self._exit(exception_type, exception_value, traceback)

    @wraps(TransactionBase._commit)
    def commit(self) -> None:
        return self._commit()

    @wraps(TransactionBase._rollback)
    def rollback(self) -> None:
        return self._rollback()

    @wraps(TransactionBase._close)
    def close(self) -> None:
        return self._close()

    @wraps(TransactionBase._closed)
    def closed(self) -> bool:
        return self._closed()

    if Util.is_async_code:
        @wraps(TransactionBase._cancel)
        def cancel(self) -> None:
            return self._cancel()


class ManagedTransaction(TransactionBase):
    """Transaction object provided to transaction functions.

    Inside a transaction function, the driver is responsible for managing
    (committing / rolling back) the transaction. Therefore,
    ManagedTransactions don't offer such methods.
    Otherwise, they behave like :class:`.Transaction`.

    * To commit the transaction,
      return anything from the transaction function.
    * To rollback the transaction, raise any exception.

    Note that transaction functions have to be idempotent (i.e., the result
    of running the function once has to be the same as running it any number
    of times). This is, because the driver will retry the transaction function
    if the error is classified as retryable.

    .. versionadded:: 5.0

        Prior, transaction functions used :class:`Transaction` objects,
        but would cause hard to interpret errors when managed explicitly
        (committed or rolled back by user code).
    """
    pass
