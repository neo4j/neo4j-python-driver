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


from collections import deque
from warnings import warn

from ..._async_compat.util import Util
from ...data import DataDehydrator
from ...exceptions import (
    ResultConsumedError,
    ResultNotSingleError,
)
from ...work import ResultSummary
from ..io import ConnectionErrorHandler


_RESULT_OUT_OF_SCOPE_ERROR = (
    "The result is out of scope. The associated transaction "
    "has been closed. Results can only be used while the "
    "transaction is open."
)
_RESULT_CONSUMED_ERROR = (
    "The result has been consumed. Fetch all needed records before calling "
    "Result.consume()."
)


class Result:
    """A handler for the result of Cypher query execution. Instances
    of this class are typically constructed and returned by
    :meth:`.AyncSession.run` and :meth:`.Transaction.run`.
    """

    def __init__(self, connection, hydrant, fetch_size, on_closed,
                 on_error):
        self._connection = ConnectionErrorHandler(connection, on_error)
        self._hydrant = hydrant
        self._on_closed = on_closed
        self._metadata = None
        self._keys = None
        self._record_buffer = deque()
        self._summary = None
        self._bookmark = None
        self._raw_qid = -1
        self._fetch_size = fetch_size

        # states
        self._discarding = False    # discard the remainder of records
        self._attached = False      # attached to a connection
        # there are still more response messages we wait for
        self._streaming = False
        # there ar more records available to pull from the server
        self._has_more = False
        # the result has been fully iterated or consumed
        self._exhausted = False
        # the result has been consumed
        self._consumed = False
        # the result has been closed as a result of closing the transaction
        self._out_of_scope = False

    @property
    def _qid(self):
        if self._raw_qid == self._connection.most_recent_qid:
            return -1
        else:
            return self._raw_qid

    def _tx_ready_run(self, query, parameters, **kwargs):
        # BEGIN+RUN does not carry any extra on the RUN message.
        # BEGIN {extra}
        # RUN "query" {parameters} {extra}
        self._run(
            query, parameters, None, None, None, None, **kwargs
        )

    def _run(
        self, query, parameters, db, imp_user, access_mode, bookmarks,
        **kwargs
    ):
        query_text = str(query)  # Query or string object
        query_metadata = getattr(query, "metadata", None)
        query_timeout = getattr(query, "timeout", None)

        parameters = DataDehydrator.fix_parameters(dict(parameters or {}, **kwargs))

        self._metadata = {
            "query": query_text,
            "parameters": parameters,
            "server": self._connection.server_info,
        }

        def on_attached(metadata):
            self._metadata.update(metadata)
            # For auto-commit there is no qid and Bolt 3 does not support qid
            self._raw_qid = metadata.get("qid", -1)
            if self._raw_qid != -1:
                self._connection.most_recent_qid = self._raw_qid
            self._keys = metadata.get("fields")
            self._attached = True

        def on_failed_attach(metadata):
            self._metadata.update(metadata)
            self._attached = False
            Util.callback(self._on_closed)

        self._connection.run(
            query_text,
            parameters=parameters,
            mode=access_mode,
            bookmarks=bookmarks,
            metadata=query_metadata,
            timeout=query_timeout,
            db=db,
            imp_user=imp_user,
            on_success=on_attached,
            on_failure=on_failed_attach,
        )
        self._pull()
        self._connection.send_all()
        self._attach()

    def _pull(self):
        def on_records(records):
            if not self._discarding:
                self._record_buffer.extend(self._hydrant.hydrate_records(self._keys, records))

        def on_summary():
            self._attached = False
            Util.callback(self._on_closed)

        def on_failure(metadata):
            self._attached = False
            Util.callback(self._on_closed)

        def on_success(summary_metadata):
            self._streaming = False
            has_more = summary_metadata.get("has_more")
            self._has_more = bool(has_more)
            if has_more:
                return
            self._metadata.update(summary_metadata)
            self._bookmark = summary_metadata.get("bookmark")

        self._connection.pull(
            n=self._fetch_size,
            qid=self._qid,
            on_records=on_records,
            on_success=on_success,
            on_failure=on_failure,
            on_summary=on_summary,
        )
        self._streaming = True

    def _discard(self):
        def on_summary():
            self._attached = False
            Util.callback(self._on_closed)

        def on_failure(metadata):
            self._metadata.update(metadata)
            self._attached = False
            Util.callback(self._on_closed)

        def on_success(summary_metadata):
            self._streaming = False
            has_more = summary_metadata.get("has_more")
            self._has_more = bool(has_more)
            if has_more:
                return
            self._discarding = False
            self._metadata.update(summary_metadata)
            self._bookmark = summary_metadata.get("bookmark")

        # This was the last page received, discard the rest
        self._connection.discard(
            n=-1,
            qid=self._qid,
            on_success=on_success,
            on_failure=on_failure,
            on_summary=on_summary,
        )
        self._streaming = True

    def __iter__(self):
        """Iterator returning Records.
        :returns: Record, it is an immutable ordered collection of key-value pairs.
        :rtype: :class:`neo4j.Record`
        """
        while self._record_buffer or self._attached:
            if self._record_buffer:
                yield self._record_buffer.popleft()
            elif self._streaming:
                self._connection.fetch_message()
            elif self._discarding:
                self._discard()
                self._connection.send_all()
            elif self._has_more:
                self._pull()
                self._connection.send_all()

        self._exhausted = True
        if self._out_of_scope:
            raise ResultConsumedError(self, _RESULT_OUT_OF_SCOPE_ERROR)
        if self._consumed:
            raise ResultConsumedError(self, _RESULT_CONSUMED_ERROR)

    def __next__(self):
        return self.__iter__().__next__()

    def _attach(self):
        """Sets the Result object in an attached state by fetching messages from
        the connection to the buffer.
        """
        if self._exhausted is False:
            while self._attached is False:
                self._connection.fetch_message()

    def _buffer(self, n=None):
        """Try to fill `self._record_buffer` with n records.

        Might end up with more records in the buffer if the fetch size makes it
        overshoot.
        Might ent up with fewer records in the buffer if there are not enough
        records available.
        """
        if self._out_of_scope:
            raise ResultConsumedError(self, _RESULT_OUT_OF_SCOPE_ERROR)
        if self._consumed:
            raise ResultConsumedError(self, _RESULT_CONSUMED_ERROR)
        if n is not None and len(self._record_buffer) >= n:
            return
        record_buffer = deque()
        for record in self:
            record_buffer.append(record)
            if n is not None and len(record_buffer) >= n:
                break
        if n is None:
            self._record_buffer = record_buffer
        else:
            self._record_buffer.extend(record_buffer)
        self._exhausted = not self._record_buffer

    def _buffer_all(self):
        """Sets the Result object in an detached state by fetching all records
        from the connection to the buffer.
        """
        self._buffer()

    def _obtain_summary(self):
        """Obtain the summary of this result, buffering any remaining records.

        :returns: The :class:`neo4j.ResultSummary` for this result
        """
        if self._summary is None:
            if self._metadata:
                self._summary = ResultSummary(
                    self._connection.unresolved_address, **self._metadata
                )
            elif self._connection:
                self._summary = ResultSummary(
                    self._connection.unresolved_address,
                    server=self._connection.server_info
                )

        return self._summary

    def keys(self):
        """The keys for the records in this result.

        :returns: tuple of key names
        :rtype: tuple
        """
        return self._keys

    def _exhaust(self):
        # Exhaust the result, ditching all remaining records.
        if not self._exhausted:
            self._discarding = True
            self._record_buffer.clear()
            for _ in self:
                pass

    def _tx_end(self):
        # Handle closure of the associated transaction.
        #
        # This will consume the result and mark it at out of scope.
        # Subsequent calls to `next` will raise a ResultConsumedError.
        self._exhaust()
        self._out_of_scope = True

    def consume(self):
        """Consume the remainder of this result and return a :class:`neo4j.ResultSummary`.

        Example::

            def create_node_tx(tx, name):
                result = tx.run(
                    "CREATE (n:ExampleNode { name: $name }) RETURN n", name=name
                )
                record = result.single()
                value = record.value()
                info = result.consume()
                return value, info

            with driver.session() as session:
                node_id, info = session.write_transaction(create_node_tx, "example")

        Example::

            def get_two_tx(tx):
                result = tx.run("UNWIND [1,2,3,4] AS x RETURN x")
                values = []
                for record in result:
                    if len(values) >= 2:
                        break
                    values.append(record.values())
                # discard the remaining records if there are any
                info = result.consume()
                # use the info for logging etc.
                return values, info

            with driver.session() as session:
                values, info = session.read_transaction(get_two_tx)

        :returns: The :class:`neo4j.ResultSummary` for this result

        :raises ResultConsumedError: if the transaction from which this result
            was obtained has been closed.

        .. versionchanged:: 5.0
            Can raise :exc:`ResultConsumedError`.
        """
        if self._out_of_scope:
            raise ResultConsumedError(self, _RESULT_OUT_OF_SCOPE_ERROR)
        if self._consumed:
            return self._obtain_summary()

        self._exhaust()
        summary = self._obtain_summary()
        self._consumed = True
        return summary

    def single(self, strict=False):
        """Obtain the next and only remaining record or None.

        Calling this method always exhausts the result.

        A warning is generated if more than one record is available but
        the first of these is still returned.

        :param strict:
            If :const:`True`, raise a :class:`neo4j.ResultNotSingleError`
            instead of returning None if there is more than one record or
            warning if there are more than 1 record.
            :const:`False` by default.
        :type strict: bool

        :returns: the next :class:`neo4j.Record` or :const:`None` if none remain
        :warns: if more than one record is available

        :raises ResultNotSingleError:
            If ``strict=True`` and not exactly one record is available.
        :raises ResultConsumedError: if the transaction from which this result
            was obtained has been closed or the Result has been explicitly
            consumed.

        .. versionchanged:: 5.0
            Added ``strict`` parameter.
        .. versionchanged:: 5.0
            Can raise :exc:`ResultConsumedError`.
        """
        self._buffer(2)
        buffer = self._record_buffer
        self._record_buffer = deque()
        self._exhaust()
        if not buffer:
            if not strict:
                return None
            raise ResultNotSingleError(
                self,
                "No records found. "
                "Make sure your query returns exactly one record."
            )
        elif len(buffer) > 1:
            res = buffer.popleft()
            if not strict:
                warn("Expected a result with a single record, "
                     "but found multiple.")
                return res
            else:
                raise ResultNotSingleError(
                    self,
                    "More than one record found. "
                    "Make sure your query returns exactly one record."
                )
        return buffer.popleft()

    def fetch(self, n):
        """Obtain up to n records from this result.

        :param n: the maximum number of records to fetch.
        :type n: int

        :returns: list of :class:`neo4j.Record`

        .. versionadded:: 5.0
        """
        self._buffer(n)
        return [
            self._record_buffer.popleft()
            for _ in range(min(n, len(self._record_buffer)))
        ]

    def peek(self):
        """Obtain the next record from this result without consuming it.
        This leaves the record in the buffer for further processing.

        :returns: the next :class:`.Record` or :const:`None` if none remain

        :raises ResultConsumedError: if the transaction from which this result
            was obtained has been closed or the Result has been explicitly
            consumed.

        .. versionchanged:: 5.0
            Can raise :exc:`ResultConsumedError`.
        """
        self._buffer(1)
        if self._record_buffer:
            return self._record_buffer[0]

    def graph(self):
        """Return a :class:`neo4j.graph.Graph` instance containing all the graph objects
        in the result. After calling this method, the result becomes
        detached, buffering all remaining records.

        :returns: a result graph
        :rtype: :class:`neo4j.graph.Graph`

        :raises ResultConsumedError: if the transaction from which this result
            was obtained has been closed or the Result has been explicitly
            consumed.

        .. versionchanged:: 5.0
            Can raise :exc:`ResultConsumedError`.
        """
        self._buffer_all()
        return self._hydrant.graph

    def value(self, key=0, default=None):
        """Helper function that return the remainder of the result as a list of values.

        See :class:`neo4j.Record.value`

        :param key: field to return for each remaining record. Obtain a single value from the record by index or key.
        :param default: default value, used if the index of key is unavailable

        :returns: list of individual values
        :rtype: list

        :raises ResultConsumedError: if the transaction from which this result
            was obtained has been closed or the Result has been explicitly
            consumed.

        .. versionchanged:: 5.0
            Can raise :exc:`ResultConsumedError`.
        """
        return [record.value(key, default) for record in self]

    def values(self, *keys):
        """Helper function that return the remainder of the result as a list of values lists.

        See :class:`neo4j.Record.values`

        :param keys: fields to return for each remaining record. Optionally filtering to include only certain values by index or key.

        :returns: list of values lists
        :rtype: list

        :raises ResultConsumedError: if the transaction from which this result
            was obtained has been closed or the Result has been explicitly
            consumed.

        .. versionchanged:: 5.0
            Can raise :exc:`ResultConsumedError`.
        """
        return [record.values(*keys) for record in self]

    def data(self, *keys):
        """Helper function that return the remainder of the result as a list of dictionaries.

        See :class:`neo4j.Record.data`

        :param keys: fields to return for each remaining record. Optionally filtering to include only certain values by index or key.

        :returns: list of dictionaries
        :rtype: list

        :raises ResultConsumedError: if the transaction from which this result
            was obtained has been closed or the Result has been explicitly
            consumed.

        .. versionchanged:: 5.0
            Can raise :exc:`ResultConsumedError`.
        """
        return [record.data(*keys) for record in self]

    def closed(self):
        """Return True if the result has been closed.

        When a result gets consumed :meth:`consume` or the transaction that
        owns the result gets closed (committed, rolled back, closed), the
        result cannot be used to acquire further records.

        In such case, all methods that need to access the Result's records,
        will raise a :exc:`ResultConsumedError` when called.

        :returns: whether the result is closed.
        :rtype: bool

        .. versionadded:: 5.0
        """
        return self._out_of_scope or self._consumed
