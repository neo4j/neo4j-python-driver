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


from collections import deque
from warnings import warn

from neo4j.data import DataDehydrator
from neo4j.work.summary import ResultSummary


class Result:
    """A handler for the result of Cypher query execution. Instances
    of this class are typically constructed and returned by
    :meth:`.Session.run` and :meth:`.Transaction.run`.
    """

    def __init__(self, connection, hydrant, fetch_size, on_closed):
        self._connection = connection
        self._hydrant = hydrant
        self._on_closed = on_closed
        self._metadata = None
        self._record_buffer = deque()
        self._summary = None
        self._bookmark = None
        self._qid = -1
        self._fetch_size = fetch_size

        # states
        self._discarding = False    # discard the remainder of records
        self._attached = False      # attached to a connection
        self._streaming = False     # there is still more records to buffer upp on the wire
        self._has_more = False      # there is more records available to pull from the server
        self._closed = False        # the result have been properly iterated or consumed fully

    def _tx_ready_run(self, query, parameters, **kwparameters):
        # BEGIN+RUN does not carry any extra on the RUN message.
        # BEGIN {extra}
        # RUN "query" {parameters} {extra}
        self._run(query, parameters, None, None, None, **kwparameters)

    def _run(self, query, parameters, db, access_mode, bookmarks, **kwparameters):
        query_text = str(query)  # Query or string object
        query_metadata = getattr(query, "metadata", None)
        query_timeout = getattr(query, "timeout", None)

        parameters = DataDehydrator.fix_parameters(dict(parameters or {}, **kwparameters))

        self._metadata = {
            "query": query_text,
            "parameters": parameters,
            "server": self._connection.server_info,
        }

        run_metadata = {
            "metadata": query_metadata,
            "timeout": query_timeout,
        }

        def on_attached(metadata):
            self._metadata.update(metadata)
            self._qid = metadata.get("qid", -1)  # For auto-commit there is no qid and Bolt 3 do not support qid
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
            on_success=on_attached,
            on_failure=on_failed_attach,
        )
        self._pull()
        self._connection.send_all()
        self._attach()

    def _pull(self):

        def on_records(records):
            self._streaming = True
            if not self._discarding:
                self._record_buffer.extend(self._hydrant.hydrate_records(self._keys, records))

        def on_summary():
            self._attached = False
            self._on_closed()

        def on_failure(metadata):
            self._attached = False
            self._on_closed()

        def on_success(summary_metadata):
            has_more = summary_metadata.get("has_more")
            if has_more:
                self._has_more = True
                self._streaming = False
                return
            else:
                self._has_more = False

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

    def _discard(self):
        def on_records(records):
            pass

        def on_summary():
            self._attached = False
            self._on_closed()

        def on_failure(metadata):
            self._metadata.update(metadata)
            self._attached = False
            self._on_closed()

        def on_success(summary_metadata):
            has_more = summary_metadata.get("has_more")
            if has_more:
                self._has_more = True
                self._streaming = False
            else:
                self._has_more = False
                self._discarding = False

            self._metadata.update(summary_metadata)
            self._bookmark = summary_metadata.get("bookmark")

        # This was the last page received, discard the rest
        self._connection.discard(
            n=-1,
            qid=self._qid,
            on_records=on_records,
            on_success=on_success,
            on_failure=on_failure,
            on_summary=on_summary,
        )

    def __iter__(self):
        """Iterator returning Records.
        :returns: Record, it is an immutable ordered collection of key-value pairs.
        :rtype: :class:`neo4j.Record`
        """
        while self._record_buffer or self._attached:
            while self._record_buffer:
                yield self._record_buffer.popleft()

            while self._attached is True:  # _attached is set to False for _pull on_summary and _discard on_summary
                self._connection.fetch_message()  # Receive at least one message from the server, if available.
                if self._attached:
                    if self._record_buffer:
                        yield self._record_buffer.popleft()
                    elif self._discarding and self._streaming is False:
                        self._discard()
                        self._connection.send_all()
                    elif self._has_more and self._streaming is False:
                        self._pull()
                        self._connection.send_all()

        self._closed = True

    def _attach(self):
        """Sets the Result object in an attached state by fetching messages from the connection to the buffer.
        """
        if self._closed is False:
            while self._attached is False:
                self._connection.fetch_message()

    def _buffer_all(self):
        """Sets the Result object in an detached state by fetching all records from the connection to the buffer.
        """
        record_buffer = deque()
        for record in self:
            record_buffer.append(record)
        self._closed = False
        self._record_buffer = record_buffer

    def _obtain_summary(self):
        """Obtain the summary of this result, buffering any remaining records.

        :returns: The :class:`neo4j.ResultSummary` for this result
        """
        if self._summary is None:
            if self._metadata:
                self._summary = ResultSummary(**self._metadata)
            elif self._connection:
                self._summary = ResultSummary(server=self._connection.server_info)

        return self._summary

    def keys(self):
        """The keys for the records in this result.

        :returns: tuple of key names
        """
        return self._keys

    def consume(self):
        """Consume the remainder of this result and return a ResultSummary.

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

        :returns: the next :class:`.Record` or :const:`None` if none remain
        :warns: if more than one record is available
        """
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

        :returns: the next :class:`.Record` or :const:`None` if none remain
        """
        if self._record_buffer:
            return self._record_buffer[0]
        if not self._attached:
            return None
        while self._attached:
            self._connection.fetch_message()
            if self._record_buffer:
                return self._record_buffer[0]

        return None

    # See Record class for available methods.

    # NOT IN THE API

    def graph(self):
        """Return a Graph instance containing all the graph objects
        in the result. After calling this method, the result becomes
        detached, buffering all remaining records.

        :returns: result graph
        """
        self._buffer_all()
        return self._hydrant.graph

    # def value(self, item=0, default=None):
    #     """Return the remainder of the result as a list of values.
    #
    #     :param item: field to return for each remaining record
    #     :param default: default value, used if the index of key is unavailable
    #     :returns: list of individual values
    #     """
    #     return [record.value(item, default) for record in self._records()]

    # def values(self, *items):
    #     """Return the remainder of the result as a list of tuples.
    #
    #     :param items: fields to return for each remaining record
    #     :returns: list of value tuples
    #     """
    #     return [record.values(*items) for record in self._records()]

    # def data(self, *items):
    #     """Return the remainder of the result as a list of dictionaries.
    #
    #     :param items: fields to return for each remaining record
    #     :returns: list of dictionaries
    #     """
    #     return [record.data(*items) for record in self]

