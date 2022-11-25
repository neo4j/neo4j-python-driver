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

import typing as t
from collections import deque
from warnings import warn


if t.TYPE_CHECKING:
    import typing_extensions as te



from ..._async_compat.util import Util
from ..._codec.hydration import BrokenHydrationObject
from ..._data import (
    Record,
    RecordTableRowExporter,
)
from ..._meta import experimental
from ...exceptions import (
    ResultConsumedError,
    ResultNotSingleError,
)
from ...time import (
    Date,
    DateTime,
)
from ...work import ResultSummary
from ..io import ConnectionErrorHandler


if t.TYPE_CHECKING:
    import pandas  # type: ignore[import]

    from ...graph import Graph


_T = t.TypeVar("_T")
_TResultKey = t.Union[int, str]


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
    """Handler for the result of Cypher query execution.

    Instances of this class are typically constructed and returned by
    :meth:`.Session.run` and :meth:`.Transaction.run`.
    """

    def __init__(self, connection, fetch_size, on_closed, on_error):
        self._connection = ConnectionErrorHandler(connection, on_error)
        self._hydration_scope = connection.new_hydration_scope()
        self._on_closed = on_closed
        self._metadata = None
        self._keys = None
        self._record_buffer = deque()
        self._summary = None
        self._database = None
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

    def _tx_ready_run(self, query, parameters):
        # BEGIN+RUN does not carry any extra on the RUN message.
        # BEGIN {extra}
        # RUN "query" {parameters} {extra}
        self._run(query, parameters, None, None, None, None)

    def _run(
        self, query, parameters, db, imp_user, access_mode, bookmarks
    ):
        query_text = str(query)  # Query or string object
        query_metadata = getattr(query, "metadata", None)
        query_timeout = getattr(query, "timeout", None)

        self._metadata = {
            "query": query_text,
            "parameters": parameters,
            "server": self._connection.server_info,
            "database": db,
        }
        self._database = db

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
            dehydration_hooks=self._hydration_scope.dehydration_hooks,
            on_success=on_attached,
            on_failure=on_failed_attach,
        )
        self._pull()
        self._connection.send_all()
        self._attach()

    def _pull(self):
        def on_records(records):
            if not self._discarding:
                records = (
                    record.raw_data
                    if isinstance(record, BrokenHydrationObject) else record
                    for record in records
                )
                self._record_buffer.extend((
                    Record(zip(self._keys, record))
                    for record in records
                ))

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
            self._database = summary_metadata.get("db", self._database)

        self._connection.pull(
            n=self._fetch_size,
            qid=self._qid,
            hydration_hooks=self._hydration_scope.hydration_hooks,
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
            self._database = summary_metadata.get("db", self._database)

        # This was the last page received, discard the rest
        self._connection.discard(
            n=-1,
            qid=self._qid,
            on_success=on_success,
            on_failure=on_failure,
            on_summary=on_summary,
        )
        self._streaming = True

    def __iter__(self) -> t.Iterator[Record]:
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

    def __next__(self) -> Record:
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

    def keys(self) -> t.Tuple[str, ...]:
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

    def consume(self) -> ResultSummary:
        """Consume the remainder of this result and return a :class:`neo4j.ResultSummary`.

        Example::

            def create_node_tx(tx, name):
                result = tx.run(
                    "CREATE (n:ExampleNode {name: $name}) RETURN n", name=name
                )
                record = result.single()
                value = record.value()
                summary = result.consume()
                return value, summary

            with driver.session() as session:
                node_id, summary = session.execute_write(
                    create_node_tx, "example"
                )

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
                return values, summary

            with driver.session() as session:
                values, summary = session.execute_read(get_two_tx)

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

    @t.overload
    def single(
        self, strict: te.Literal[False] = False
    ) -> t.Optional[Record]:
        ...

    @t.overload
    def single(self, strict: te.Literal[True]) -> Record:
        ...

    def single(self, strict: bool = False) -> t.Optional[Record]:
        """Obtain the next and only remaining record or None.

        Calling this method always exhausts the result.

        If ``strict`` is :const:`True`, this method will raise an exception if
        there is not exactly one record left.

        If ``strict`` is :const:`False`, fewer than one record will make this
        method return :const:`None`, more than one record will make this method
        emit a warning and return the first record.

        :param strict:
            If :const:`True`, raise a :class:`neo4j.ResultNotSingleError`
            instead of returning None if there is more than one record or
            warning if there are more than 1 record.
            :const:`False` by default.

        :returns: the next :class:`neo4j.Record` or :const:`None` if none remain

        :warns: if more than one record is available and
            ``strict`` is :const:`False`

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

    def fetch(self, n: int) -> t.List[Record]:
        """Obtain up to n records from this result.

        Fetch ``min(n, records_left)`` records from this result and return them
        as a list.

        :param n: the maximum number of records to fetch.

        :returns: list of :class:`neo4j.Record`

        :raises ResultConsumedError: if the transaction from which this result
            was obtained has been closed or the Result has been explicitly
            consumed.

        .. versionadded:: 5.0
        """
        self._buffer(n)
        return [
            self._record_buffer.popleft()
            for _ in range(min(n, len(self._record_buffer)))
        ]

    def peek(self) -> t.Optional[Record]:
        """Obtain the next record from this result without consuming it.

        This leaves the record in the buffer for further processing.

        :returns: the next :class:`neo4j.Record` or :const:`None` if none
            remain.

        :raises ResultConsumedError: if the transaction from which this result
            was obtained has been closed or the Result has been explicitly
            consumed.

        .. versionchanged:: 5.0
            Can raise :exc:`ResultConsumedError`.
        """
        self._buffer(1)
        if self._record_buffer:
            return self._record_buffer[0]
        return None

    def graph(self) -> Graph:
        """Turn the result into a :class:`neo4j.Graph`.

        Return a :class:`neo4j.graph.Graph` instance containing all the graph
        objects in the result. This graph will also contain already consumed
        records.

        After calling this method, the result becomes
        detached, buffering all remaining records.

        :returns: a result graph

        :raises ResultConsumedError: if the transaction from which this result
            was obtained has been closed or the Result has been explicitly
            consumed.

        .. versionchanged:: 5.0
            Can raise :exc:`ResultConsumedError`.
        """
        self._buffer_all()
        return self._hydration_scope.get_graph()

    def value(
        self, key: _TResultKey = 0, default: t.Optional[object] = None
    ) -> t.List[t.Any]:
        """Return the remainder of the result as a list of values.

        :param key: field to return for each remaining record. Obtain a single value from the record by index or key.
        :param default: default value, used if the index of key is unavailable

        :returns: list of individual values

        :raises ResultConsumedError: if the transaction from which this result
            was obtained has been closed or the Result has been explicitly
            consumed.

        .. versionchanged:: 5.0
            Can raise :exc:`ResultConsumedError`.

        .. seealso:: :meth:`.Record.value`
        """
        return [record.value(key, default) for record in self]

    def values(
        self, *keys: _TResultKey
    ) -> t.List[t.List[t.Any]]:
        """Return the remainder of the result as a list of values lists.

        :param keys: fields to return for each remaining record. Optionally filtering to include only certain values by index or key.

        :returns: list of values lists

        :raises ResultConsumedError: if the transaction from which this result
            was obtained has been closed or the Result has been explicitly
            consumed.

        .. versionchanged:: 5.0
            Can raise :exc:`ResultConsumedError`.

        .. seealso:: :meth:`.Record.values`
        """
        return [record.values(*keys) for record in self]

    def data(self, *keys: _TResultKey) -> t.List[t.Dict[str, t.Any]]:
        """Return the remainder of the result as a list of dictionaries.

        This function provides a convenient but opinionated way to obtain the
        remainder of the result as mostly JSON serializable data. It is mainly
        useful for interactive sessions and rapid prototyping.

        For instance, node and relationship labels are not included. You will
        have to implement a custom serializer should you need more control over
        the output format.

        :param keys: fields to return for each remaining record. Optionally filtering to include only certain values by index or key.

        :returns: list of dictionaries

        :raises ResultConsumedError: if the transaction from which this result
            was obtained has been closed or the Result has been explicitly
            consumed.

        .. versionchanged:: 5.0
            Can raise :exc:`ResultConsumedError`.

        .. seealso:: :meth:`.Record.data`
        """
        return [record.data(*keys) for record in self]

    def to_df(
        self,
        expand: bool = False,
        parse_dates: bool = False
    ) -> pandas.DataFrame:
        r"""Convert (the rest of) the result to a pandas DataFrame.

        This method is only available if the `pandas` library is installed.

        ::

            res = tx.run("UNWIND range(1, 10) AS n RETURN n, n+1 as m")
            df = res.to_df()

        for instance will return a DataFrame with two columns: ``n`` and ``m``
        and 10 rows.

        :param expand: If :const:`True`, some structures in the result will be
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
        :param parse_dates:
            If :const:`True`, columns that exclusively contain
            :class:`time.DateTime` objects, :class:`time.Date` objects, or
            :const:`None`, will be converted to :class:`pandas.Timestamp`.

        :raises ImportError: if `pandas` library is not available.
        :raises ResultConsumedError: if the transaction from which this result
            was obtained has been closed or the Result has been explicitly
            consumed.
        """
        import pandas as pd  # type: ignore[import]

        if not expand:
            df = pd.DataFrame(self.values(), columns=self._keys)
        else:
            df_keys = None
            rows = []
            for record in self:
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
                df = pd.DataFrame(rows)
            else:
                columns = df_keys or [
                    k.replace(".", "\\.").replace("\\", "\\\\")
                    for k in self._keys
                ]
                df = pd.DataFrame(rows, columns=columns)
        if not parse_dates:
            return df
        dt_columns = df.columns[df.apply(
            lambda col: pd.api.types.infer_dtype(col) == "mixed" and col.map(
                lambda x: isinstance(x, (DateTime, Date, type(None)))
            ).all()
        )]
        df[dt_columns] = df[dt_columns].apply(
            lambda col: col.map(
                lambda x:
                    pd.Timestamp(x.iso_format())
                        .replace(tzinfo=getattr(x, "tzinfo", None))
                    if x else pd.NaT
            )
        )
        return df

    def closed(self) -> bool:
        """Return True if the result has been closed.

        When a result gets consumed :meth:`consume` or the transaction that
        owns the result gets closed (committed, rolled back, closed), the
        result cannot be used to acquire further records.

        In such case, all methods that need to access the Result's records,
        will raise a :exc:`ResultConsumedError` when called.

        :returns: whether the result is closed.

        .. versionadded:: 5.0
        """
        return self._out_of_scope or self._consumed
