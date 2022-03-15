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

from ..._async_compat.util import AsyncUtil
from ...data import (
    DataDehydrator,
    RecordTableRowExporter,
)
from ...exceptions import (
    ResultConsumedError,
    ResultNotSingleError,
)
from ...meta import experimental
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


class AsyncResult:
    """A handler for the result of Cypher query execution. Instances
    of this class are typically constructed and returned by
    :meth:`.AyncSession.run` and :meth:`.AsyncTransaction.run`.
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

    async def _tx_ready_run(self, query, parameters, **kwargs):
        # BEGIN+RUN does not carry any extra on the RUN message.
        # BEGIN {extra}
        # RUN "query" {parameters} {extra}
        await self._run(
            query, parameters, None, None, None, None, **kwargs
        )

    async def _run(
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

        async def on_failed_attach(metadata):
            self._metadata.update(metadata)
            self._attached = False
            await AsyncUtil.callback(self._on_closed)

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
        await self._connection.send_all()
        await self._attach()

    def _pull(self):
        def on_records(records):
            if not self._discarding:
                self._record_buffer.extend(self._hydrant.hydrate_records(self._keys, records))

        async def on_summary():
            self._attached = False
            await AsyncUtil.callback(self._on_closed)

        async def on_failure(metadata):
            self._attached = False
            await AsyncUtil.callback(self._on_closed)

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
        async def on_summary():
            self._attached = False
            await AsyncUtil.callback(self._on_closed)

        async def on_failure(metadata):
            self._metadata.update(metadata)
            self._attached = False
            await AsyncUtil.callback(self._on_closed)

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

    async def __aiter__(self):
        """Iterator returning Records.
        :returns: Record, it is an immutable ordered collection of key-value pairs.
        :rtype: :class:`neo4j.AsyncRecord`
        """
        while self._record_buffer or self._attached:
            if self._record_buffer:
                yield self._record_buffer.popleft()
            elif self._streaming:
                await self._connection.fetch_message()
            elif self._discarding:
                self._discard()
                await self._connection.send_all()
            elif self._has_more:
                self._pull()
                await self._connection.send_all()

        self._exhausted = True
        if self._out_of_scope:
            raise ResultConsumedError(self, _RESULT_OUT_OF_SCOPE_ERROR)
        if self._consumed:
            raise ResultConsumedError(self, _RESULT_CONSUMED_ERROR)

    async def __anext__(self):
        return await self.__aiter__().__anext__()

    async def _attach(self):
        """Sets the Result object in an attached state by fetching messages from
        the connection to the buffer.
        """
        if self._exhausted is False:
            while self._attached is False:
                await self._connection.fetch_message()

    async def _buffer(self, n=None):
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
        async for record in self:
            record_buffer.append(record)
            if n is not None and len(record_buffer) >= n:
                break
        if n is None:
            self._record_buffer = record_buffer
        else:
            self._record_buffer.extend(record_buffer)
        self._exhausted = not self._record_buffer

    async def _buffer_all(self):
        """Sets the Result object in an detached state by fetching all records
        from the connection to the buffer.
        """
        await self._buffer()

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

    async def _exhaust(self):
        # Exhaust the result, ditching all remaining records.
        if not self._exhausted:
            self._discarding = True
            self._record_buffer.clear()
            async for _ in self:
                pass

    async def _tx_end(self):
        # Handle closure of the associated transaction.
        #
        # This will consume the result and mark it at out of scope.
        # Subsequent calls to `next` will raise a ResultConsumedError.
        await self._exhaust()
        self._out_of_scope = True

    async def consume(self):
        """Consume the remainder of this result and return a :class:`neo4j.ResultSummary`.

        Example::

            def create_node_tx(tx, name):
                result = await tx.run(
                    "CREATE (n:ExampleNode { name: $name }) RETURN n", name=name
                )
                record = await result.single()
                value = record.value()
                info = await result.consume()
                return value, info

            async with driver.session() as session:
                node_id, info = await session.write_transaction(create_node_tx, "example")

        Example::

            async def get_two_tx(tx):
                result = await tx.run("UNWIND [1,2,3,4] AS x RETURN x")
                values = []
                async for record in result:
                    if len(values) >= 2:
                        break
                    values.append(record.values())
                # discard the remaining records if there are any
                info = await result.consume()
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

        await self._exhaust()
        summary = self._obtain_summary()
        self._consumed = True
        return summary

    async def single(self, strict=False):
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
        await self._buffer(2)
        buffer = self._record_buffer
        self._record_buffer = deque()
        await self._exhaust()
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

    async def fetch(self, n):
        """Obtain up to n records from this result.

        :param n: the maximum number of records to fetch.
        :type n: int

        :returns: list of :class:`neo4j.AsyncRecord`

        .. versionadded:: 5.0
        """
        await self._buffer(n)
        return [
            self._record_buffer.popleft()
            for _ in range(min(n, len(self._record_buffer)))
        ]

    async def peek(self):
        """Obtain the next record from this result without consuming it.
        This leaves the record in the buffer for further processing.

        :returns: the next :class:`.Record` or :const:`None` if none remain

        :raises ResultConsumedError: if the transaction from which this result
            was obtained has been closed or the Result has been explicitly
            consumed.

        .. versionchanged:: 5.0
            Can raise :exc:`ResultConsumedError`.
        """
        await self._buffer(1)
        if self._record_buffer:
            return self._record_buffer[0]

    async def graph(self):
        """Return a :class:`neo4j.graph.Graph` instance containing all the graph objects
        in the result. After calling this method, the result becomes
        detached, buffering all remaining records.

        :returns: a result graph
        :rtype: :class:`neo4j.graph.Graph`

        :raises ResultConsumedError: if the transaction from which this result
            was obtained has been closed or the Result has been explicitly
            consumed.

        **This is experimental.** (See :ref:`filter-warnings-ref`)

        .. versionchanged:: 5.0
            Can raise :exc:`ResultConsumedError`.
        """
        await self._buffer_all()
        return self._hydrant.graph

    async def value(self, key=0, default=None):
        """Helper function that return the remainder of the result as a list of values.

        See :class:`neo4j.AsyncRecord.value`

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
        return [record.value(key, default) async for record in self]

    async def values(self, *keys):
        """Helper function that return the remainder of the result as a list of values lists.

        See :class:`neo4j.AsyncRecord.values`

        :param keys: fields to return for each remaining record. Optionally filtering to include only certain values by index or key.

        :returns: list of values lists
        :rtype: list

        :raises ResultConsumedError: if the transaction from which this result
            was obtained has been closed or the Result has been explicitly
            consumed.

        .. versionchanged:: 5.0
            Can raise :exc:`ResultConsumedError`.
        """
        return [record.values(*keys) async for record in self]

    async def data(self, *keys):
        """Helper function that return the remainder of the result as a list of dictionaries.

        See :class:`neo4j.AsyncRecord.data`

        :param keys: fields to return for each remaining record. Optionally filtering to include only certain values by index or key.

        :returns: list of dictionaries
        :rtype: list

        :raises ResultConsumedError: if the transaction from which this result
            was obtained has been closed or the Result has been explicitly
            consumed.

        .. versionchanged:: 5.0
            Can raise :exc:`ResultConsumedError`.
        """
        return [record.data(*keys) async for record in self]

    @experimental("pandas support is experimental and might be changed or "
                  "removed in future versions")
    async def to_df(self, expand=False):
        r"""Convert (the rest of) the result to a pandas DataFrame.

        This method is only available if the `pandas` library is installed.

        ::

            res = await tx.run("UNWIND range(1, 10) AS n RETURN n, n+1 as m")
            df = await res.to_df()

        for instance will return a DataFrame with two columns: ``n`` and ``m``
        and 10 rows.

        :param expand: if :const:`True`, some structures in the result will be
            recursively expanded (flattened out into multiple columns) like so
            (everything inside ``<...>`` is a placeholder):

            * :class:`.Node` objects under any variable ``<n>`` will be
              expanded into columns (the recursion stops here)

              * ``<n>().prop.<property_name>`` (any) for each property of the
                node.
              * ``<n>().element_id`` (str) the node's element id.
                See :attr:`.Node.element_id`.
              * ``<n>().labels`` (frozenset of str) the node's labels.
                See :attr:`.Node.labels`.

            * :class:`.Relationship` objects under any variable ``<r>``
              will be expanded into columns (the recursion stops here)

              * ``<r>->.prop.<property_name>`` (any) for each property of the
                relationship.
              * ``<r>->.element_id`` (str) the relationship's element id.
                See :attr:`.Relationship.element_id`.
              * ``<r>->.start.element_id`` (str) the relationship's
                start node's element id.
                See :attr:`.Relationship.start_node`.
              * ``<r>->.end.element_id`` (str) the relationship's
                end node's element id.
                See :attr:`.Relationship.end_node`.
              * ``<r>->.type`` (str) the relationship's type.
                See :attr:`.Relationship.type`.

            * :const:`list` objects under any variable ``<l>`` will be expanded
              into

              * ``<l>[].0`` (any) the 1st list element
              * ``<l>[].1`` (any) the 2nd list element
              * ...

            * :const:`dict` objects under any variable ``<d>`` will be expanded
              into

              * ``<d>{}.<key1>`` (any) the 1st key of the dict
              * ``<d>{}.<key2>`` (any) the 2nd key of the dict
              * ...

            * :const:`list` and :const:`dict` objects are expanded recursively.
              Example::

                variable x: [{"foo": "bar", "baz": [42, 0]}, "foobar"]

              will be expanded to::

                {
                    "x[].0{}.foo": "bar",
                    "x[].0{}.baz[].0": 42,
                    "n[].0{}.baz[].1": 0,
                    "n[].1": "foobar"
                }

            * Everything else (including :class:`.Path` objects) will not
              be flattened.

            :const:`dict` keys and variable names that contain ``.``  or ``\``
            will be escaped with a backslash (``\.`` and ``\\`` respectively).
        :type expand: bool

        :rtype: :py:class:`pandas.DataFrame`
        :raises ImportError: if `pandas` library is not available.
        :raises ResultConsumedError: if the transaction from which this result
            was obtained has been closed or the Result has been explicitly
            consumed.

        **This is experimental.**
        ``pandas`` support might be changed or removed in future versions
        without warning. (See :ref:`filter-warnings-ref`)
        """
        import pandas as pd

        if not expand:
            return pd.DataFrame(await self.values(), columns=self._keys)
        else:
            df_keys = None
            rows = []
            async for record in self:
                row = RecordTableRowExporter().transform(dict(record.items()))
                if df_keys == row.keys():
                    rows.append(row.values())
                elif df_keys is None:
                    df_keys = row.keys()
                    rows.append(row.values())
                elif df_keys is False:
                    rows.append(row)
                else:
                    # The rows have different keys. We need to pass a list
                    # of dicts to pandas
                    rows = [{k: v for k, v in zip(df_keys, r)} for r in rows]
                    df_keys = False
                    rows.append(row)
            if df_keys is False:
                return pd.DataFrame(rows)
            else:
                columns = df_keys or [
                    k.replace(".", "\\.").replace("\\", "\\\\")
                    for k in self._keys
                ]
                return pd.DataFrame(rows, columns=columns)

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
