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


from ..._async_compat.util import AsyncUtil
from ...data import DataHydrator
from ...exceptions import TransactionError
from ...work import Query
from ..io import ConnectionErrorHandler
from .result import AsyncResult


class AsyncTransaction:
    """ Container for multiple Cypher queries to be executed within a single
    context. asynctransactions can be used within a :py:const:`async with`
    block where the transaction is committed or rolled back on based on
    whether an exception is raised::

        async with session.begin_transaction() as tx:
            ...

    """

    def __init__(self, connection, fetch_size, on_closed, on_error):
        self._connection = connection
        self._error_handling_connection = ConnectionErrorHandler(
            connection, self._error_handler
        )
        self._bookmark = None
        self._results = []
        self._closed = False
        self._last_error = None
        self._fetch_size = fetch_size
        self._on_closed = on_closed
        self._on_error = on_error

    async def __aenter__(self):
        return self

    async def __aexit__(self, exception_type, exception_value, traceback):
        if self._closed:
            return
        success = not bool(exception_type)
        if success:
            await self.commit()
        await self.close()

    async def _begin(
        self, database, imp_user, bookmarks, access_mode, metadata, timeout
    ):
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
        await AsyncUtil.callback(self._on_error, exc)

    async def _consume_results(self):
        for result in self._results:
            await result.consume()
        self._results = []

    async def run(self, query, parameters=None, **kwparameters):
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
        :type query: str
        :param parameters: dictionary of parameters
        :type parameters: dict
        :param kwparameters: additional keyword parameters
        :returns: a new :class:`neo4j.Result` object
        :rtype: :class:`neo4j.Result`
        :raise TransactionError: if the transaction is already closed
        """
        if isinstance(query, Query):
            raise ValueError("Query object is only supported for session.run")

        if self._closed:
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
            self._connection, DataHydrator(), self._fetch_size,
            self._result_on_closed_handler,
            self._error_handler
        )
        self._results.append(result)

        await result._tx_ready_run(query, parameters, **kwparameters)

        return result

    async def commit(self):
        """Mark this transaction as successful and close in order to trigger a COMMIT.

        :raise TransactionError: if the transaction is already closed
        """
        if self._closed:
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
        finally:
            self._closed = True
            await AsyncUtil.callback(self._on_closed)

        return self._bookmark

    async def rollback(self):
        """Mark this transaction as unsuccessful and close in order to trigger a ROLLBACK.

        :raise TransactionError: if the transaction is already closed
        """
        if self._closed:
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
        finally:
            self._closed = True
            await AsyncUtil.callback(self._on_closed)

    async def close(self):
        """Close this transaction, triggering a ROLLBACK if not closed.
        """
        if self._closed:
            return
        await self.rollback()

    def closed(self):
        """Indicator to show whether the transaction has been closed.

        :return: :const:`True` if closed, :const:`False` otherwise.
        :rtype: bool
        """
        return self._closed
