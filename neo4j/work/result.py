#!/usr/bin/env python
# -*- encoding: utf-8 -*-

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

from neo4j.data import DataDehydrator
from neo4j.io import ConnectionErrorHandler
from neo4j.meta import experimental
from neo4j.work.summary import ResultSummary


class Result:
    """A handler for the result of Cypher query execution. Instances
    of this class are typically constructed and returned by
    :meth:`.Session.run` and :meth:`.Transaction.run`.
    """

    def __init__(self, connection, hydrant, fetch_size, on_closed,
                 on_error):
        self._connection = ConnectionErrorHandler(connection, on_error)
        self._hydrant = hydrant
        self._on_closed = on_closed
        self._metadata = None
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
        self._closed = False

    @property
    def _qid(self):
        if self._raw_qid == self._connection.most_recent_qid:
            return -1
        else:
            return self._raw_qid

    def _tx_ready_run(self, query, parameters, **kwparameters):
        # BEGIN+RUN does not carry any extra on the RUN message.
        # BEGIN {extra}
        # RUN "query" {parameters} {extra}
        self._run(query, parameters, None, None, None, None, **kwparameters)

    def _run(self, query, parameters, db, imp_user, access_mode, bookmarks,
             **kwparameters):
        query_text = str(query)  # Query or string object
        query_metadata = getattr(query, "metadata", None)
        query_timeout = getattr(query, "timeout", None)

        parameters = DataDehydrator.fix_parameters(dict(parameters or {}, **kwparameters))

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
            self._on_closed()

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
            self._on_closed()

        def on_failure(metadata):
            self._attached = False
            self._on_closed()

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
            self._on_closed()

        def on_failure(metadata):
            self._metadata.update(metadata)
            self._attached = False
            self._on_closed()

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

        self._closed = True

    def _attach(self):
        """Sets the Result object in an attached state by fetching messages from
        the connection to the buffer.
        """
        if self._closed is False:
            while self._attached is False:
                self._connection.fetch_message()

    def _buffer(self, n=None):
        """Try to fill `self_record_buffer` with n records.

        Might end up with more records in the buffer if the fetch size makes it
        overshoot.
        Might ent up with fewer records in the buffer if there are not enough
        records available.
        """
        record_buffer = deque()
        for record in self:
            record_buffer.append(record)
            if n is not None and len(record_buffer) >= n:
                break
        self._closed = False
        if n is None:
            self._record_buffer = record_buffer
        else:
            self._record_buffer.extend(record_buffer)

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

    def consume(self):
        """Consume the remainder of this result and return a :class:`neo4j.ResultSummary`.

        Example::

            def create_node_tx(tx, name):
                result = tx.run("CREATE (n:ExampleNode { name: $name }) RETURN n", name=name)
                record = result.single()
                value = record.value()
                summary = result.consume()
                return value, summary

            with driver.session() as session:
                node_id, summary = session.write_transaction(create_node_tx, "example")

        Example::

            def get_two_tx(tx):
                result = tx.run("UNWIND [1,2,3,4] AS x RETURN x")
                values = []
                for ix, record in enumerate(result):
                    if x > 1:
                        break
                    values.append(record.values())
                summary = result.consume()  # discard the remaining records if there are any
                # use the summary for logging etc.
                return values, summary

            with driver.session() as session:
                values, summary = session.read_transaction(get_two_tx)

        :returns: The :class:`neo4j.ResultSummary` for this result
        """
        if self._closed is False:
            self._discarding = True
            for _ in self:
                pass

        return self._obtain_summary()

    def single(self):
        """Obtain the next and only remaining record from this result if available else return None.
        Calling this method always exhausts the result.

        A warning is generated if more than one record is available but
        the first of these is still returned.

        :returns: the next :class:`neo4j.Record` or :const:`None` if none remain
        :warns: if more than one record is available
        """
        # TODO in 5.0 replace with this code that raises an error if there's not
        # exactly one record in the left result stream.
        # self._buffer(2).
        # if len(self._record_buffer) != 1:
        #     raise SomeError("Expected exactly 1 record, found %i"
        #                      % len(self._record_buffer))
        # return self._record_buffer.popleft()
        records = list(self)  # TODO: exhausts the result with self.consume if there are more records.
        size = len(records)
        if size == 0:
            return None
        if size != 1:
            warn("Expected a result with a single record, but this result contains %d" % size)
        return records[0]

    def peek(self):
        """Obtain the next record from this result without consuming it.
        This leaves the record in the buffer for further processing.

        :returns: the next :class:`neo4j.Record` or :const:`None` if none remain
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

        **This is experimental.** (See :ref:`filter-warnings-ref`)
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
        """
        return [record.value(key, default) for record in self]

    def values(self, *keys):
        """Helper function that return the remainder of the result as a list of values lists.

        See :class:`neo4j.Record.values`

        :param keys: fields to return for each remaining record. Optionally filtering to include only certain values by index or key.
        :returns: list of values lists
        :rtype: list
        """
        return [record.values(*keys) for record in self]

    def data(self, *keys):
        """Helper function that return the remainder of the result as a list of dictionaries.

        See :class:`neo4j.Record.data`

        :param keys: fields to return for each remaining record. Optionally filtering to include only certain values by index or key.
        :returns: list of dictionaries
        :rtype: list
        """
        return [record.data(*keys) for record in self]

    @experimental("pandas support is experimental and might be changed or "
                  "removed in future versions")
    def to_df(self):
        """Convert (the rest of) the result to a pandas DataFrame.

        This method is only available if the `pandas` library is installed.

        ``tx.run("UNWIND range(1, 10) AS n RETURN n, n+1 as m").to_df()``, for
        instance will return a DataFrame with two columns: ``n`` and ``m`` and
        10 rows.

        :rtype: :py:class:`pandas.DataFrame`
        :raises ImportError: if `pandas` library is not available.

        .. versionadded:: 5.0
            This method was backported from 5.0 for preview purposes.

        **This is experimental.**
        ``pandas`` support might be changed or removed in future versions
        without warning. (See :ref:`filter-warnings-ref`)
        """
        import pandas as pd

        return pd.DataFrame(self.values(), columns=self._keys)
