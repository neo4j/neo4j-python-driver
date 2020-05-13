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


from neo4j.work.result import Result
from neo4j.data import DataHydrator
from neo4j._exceptions import BoltIncompleteCommitError
from neo4j.exceptions import (
    ServiceUnavailable,
    TransactionError,
)

from logging import getLogger
log = getLogger("neo4j")


class Transaction:
    """ Container for multiple Cypher queries to be executed within
    a single context. Transactions can be used within a :py:const:`with`
    block where the transaction is committed or rolled back on based on
    whether or not an exception is raised::

        with session.begin_transaction() as tx:
            pass

    """

    def __init__(self, connection, fetch_size, on_closed):
        self._connection = connection
        self._bookmark = None
        # self._result = None
        self._results = []
        self._closed = False
        self._fetch_size = fetch_size
        self._on_closed = on_closed

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        # log.debug("Transaction.__exit__")
        if self._closed:
            return
        success = not bool(exception_type)
        if success:
            self.commit()
        self._close()

    def _begin(self, database, bookmarks, access_mode, metadata, timeout):
        # log.debug("Transaction._begin")
        self._connection.begin(bookmarks=bookmarks, metadata=metadata, timeout=timeout, mode=access_mode, db=database)

    def run(self, query, parameters=None, **kwparameters):
        """ Run a Cypher query within the context of this transaction.

        The query is sent to the server lazily, when its result is
        consumed. To force the query to be sent to the server, use
        the :meth:`.Transaction.sync` method.

        Cypher is typically expressed as a query template plus a
        set of named parameters. In Python, parameters may be expressed
        through a dictionary of parameters, through individual parameter
        arguments, or as a mixture of both. For example, the `run`
        queries below are all equivalent::

            >>> query = "CREATE (a:Person {name:{name}, age:{age}})"
            >>> tx.run(query, {"name": "Alice", "age": 33})
            >>> tx.run(query, {"name": "Alice"}, age=33)
            >>> tx.run(query, name="Alice", age=33)

        Parameter values can be of any type supported by the Neo4j type
        system. In Python, this includes :class:`bool`, :class:`int`,
        :class:`str`, :class:`list` and :class:`dict`. Note however that
        :class:`list` properties must be homogenous.

        :param query: template Cypher query
        :param parameters: dictionary of parameters
        :param kwparameters: additional keyword parameters
        :returns: :class:`neo4j.Result` object
        :raise TransactionError: if the transaction is closed
        """
        # log.debug("Transaction.run")
        self._assert_open()
        if self._results and self._connection.supports_multiple_results is False:
            # Autocommit
            self._results[0]._detach()  # Buffer up the records for the result object
            self._results.pop(0)
            assert len(self._results) == 0

        result = Result(self._connection, DataHydrator(), self._fetch_size, self._result_closed)
        self._results.append(result)

        result._tx_ready_run(query, parameters, **kwparameters)

        return result

    def _result_closed(self):
        pass

    def sync(self):
        """ Force any queued queries to be sent to the server and
        all related results to be fetched and buffered.

        :raise TransactionError: if the transaction is closed
        """
        self._assert_open()
        self._connection.send_all()

    def commit(self):
        """ Mark this transaction as successful and close in order to
        trigger a COMMIT. This is functionally equivalent to::

        :raise TransactionError: if already closed
        """
        metadata = {}
        self._consume_results()  # DISCARD pending records then do a commit.
        try:
            self._connection.commit(on_success=metadata.update)
            self._connection.send_all()
            self._connection.fetch_all()
        except BoltIncompleteCommitError:
            raise ServiceUnavailable("Connection closed during commit")
        self._bookmark = metadata.get("bookmark")
        self._closed = True
        self._on_closed()

        return self._bookmark

    def rollback(self):
        """ Mark this transaction as unsuccessful and close in order to
        trigger a ROLLBACK. This is functionally equivalent to::

        :raise TransactionError: if already closed
        """
        metadata = {}
        self._consume_results()  # DISCARD pending records then do a rollback.
        self._connection.rollback(on_success=metadata.update)
        self._connection.send_all()
        self._connection.fetch_all()
        self._closed = True
        # self._consume_results()
        self._on_closed()

    def _consume_results(self):
        for result in self._results:
            result.consume()
        self._results = []

    def _close(self):
        """ Close this transaction, triggering either a ROLLBACK if not committed.

        :raise TransactionError: if already closed
        """
        if self._closed:
            return
        self.rollback()

    def closed(self):
        """ Indicator to show whether the transaction has been closed.
        :returns: :const:`True` if closed, :const:`False` otherwise.
        """
        return self._closed

    def _assert_open(self):
        if self._closed:
            raise TransactionError("Transaction closed")

