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

from neo4j.data import DataDehydrator
from neo4j.work.summary import ResultSummary

from logging import getLogger
log = getLogger("neo4j")


class Result:
    """A handler for the result of Cypher query execution. Instances
    of this class are typically constructed and returned by
    :meth:`.Session.run` and :meth:`.Transaction.run`.
    """

    def __init__(self, connection, hydrant, fetch_size):
        self._connection = connection
        self._hydrant = hydrant
        self._metadata = None
        self._record_buffer = deque()
        self._summary = None
        self._discarding = False
        self._attached = False
        self._bookmark = None
        self._qid = -1
        self._fetch_size = fetch_size


    def _run(self, query, parameters, db, access_mode, bookmarks, **kwparameters):
        log.debug("Result._run")
        query_text = str(query)
        query_metadata = getattr(query, "metadata", None)
        query_timeout = getattr(query, "timeout", None)
        parameters = DataDehydrator.fix_parameters(dict(parameters or {}, **kwparameters))

        self._metadata = {
            "query": query_text,
            "parameters": parameters,
            "server": self._connection.server_info,
            # "protocol_version": self._connection.PROTOCOL_VERSION,  # This information is also available in the _connection.server_info object
        }

        run_metadata = {
            "metadata": query_metadata,
            "timeout": query_timeout,
        }

        def on_attached(metadata):
            self._metadata.update(metadata)
            self._qid = metadata.get("qid")
            self._keys = metadata.get("fields")
            log.debug("RESULT qid={} ATTACHED".format(self._qid))
            self._attached = True

        def on_failed_attach(metadata):
            self._attached = False

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
        # ix = self._connection.fetch_message()  # Receive response to run
        # log.debug("RESULT._run qid={} {}".format(self._qid, ix))
        # ix = self._connection.fetch_message()  # Receive response to pull, this will attach the result
        # log.debug("RESULT._run {}".format(ix))

    def _pull(self):

        def on_records(records):
            if not self._discarding:
                self._record_buffer.extend(self._hydrant.hydrate_records(self._keys, records))

        def on_summary():
            log.debug("RESULT qid={} DETACHED".format(self._qid))
            self._attached = False

        def on_failure():
            pass

        def on_success(summary_metadata):
            has_more = summary_metadata.get("has_more")
            if has_more:
                if self._discarding:
                    # This was the last page received, discard the rest
                    self._connection.discard(
                        n=-1,
                        on_success=on_success,
                        on_failure=on_failure,
                        on_summary=on_summary)
                    self._connection.send_all()
                else:
                    self._pull()
                    self._connection.send_all()
                    self._connection.fetch_message()
                return

            self._metadata.update(summary_metadata)
            self._bookmark = summary_metadata.get("bookmark")

        def on_summary():
            self._attached = False

        def on_failure():
            pass

        self._connection.pull(
            n=self._fetch_size,
            qid=self._qid,
            on_records=on_records,
            on_success=on_success,
            on_failure=on_failure,
            on_summary=on_summary,
        )

    def __iter__(self):
        return self._records()

    # TODO: Better name!
    def _detach(self):
        """Detach this result from its parent session by fetching the
        remainder of this result from the network into the buffer.

        :returns: number of records fetched
        """
        while self._attached:
            self._connection.fetch_message()

    def keys(self):
        """The keys for the records in this result.

        :returns: tuple of key names
        """
        return self._keys

    def _records(self):
        """Generator for records obtained from this result.

        :yields: iterable of :class:`.Record` objects
        """
        # while self._records:
        #     yield self._record_buffer.popleft()
        #
        # # Set to False when summary received
        # while self._attached:
        #     self._connection.fetch_message()
        #     if self._attached:
        #         yield self._record_buffer.popleft()

        log.debug("RESULT qid={} RECORDS INIT".format(self._qid))
        while self._record_buffer:
            yield self._record_buffer.popleft()

        nbr = 0
        while not self._attached:
            fetched_detail_messages, fetched_summary_messages = self._connection.fetch_message()  # Receive at least one message from the server, if available.
            log.debug("fetched_detail_messages {}, fetched_summary_messages {}".format(fetched_detail_messages, fetched_summary_messages))
            nbr += 1
            if nbr > 3:
                break

        # Set to False when summary received
        while self._attached:
            self._connection.fetch_message()  # get a record or detach
            if self._attached and self._record_buffer:
                record = self._record_buffer.popleft()
                log.debug("RESULT qid={} {}".format(self._qid, record))
                yield record

        log.debug("RESULT qid={} RECORDS EXIT".format(self._qid))


    def _obtain_summary(self):
        """Obtain the summary of this result, buffering any remaining records.

        :returns: The :class:`neo4j.ResultSummary` for this result
        """
        #self.detach()
        if self._summary is None:
            self._summary = ResultSummary(**self._metadata)
        return self._summary

    def consume(self):
        """Consume the remainder of this result and return a ResultSummary.

        :returns: The :class:`neo4j.ResultSummary` for this result
        """
        self._discarding = True
        while self._attached:
            self._connection.fetch_message()

        return self._obtain_summary()

    def single(self):
        """Obtain the next and only remaining record from this result.

        A warning is generated if more than one record is available but
        the first of these is still returned.

        :returns: the next :class:`.Record` or :const:`None` if none remain
        :warns: if more than one record is available
        """
        records = list(self)
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

    def graph(self):
        """Return a Graph instance containing all the graph objects
        in the result. After calling this method, the result becomes
        detached, buffering all remaining records.

        :returns: result graph
        """
        self.detach()
        return self._hydrant.graph

    def value(self, item=0, default=None):
        """Return the remainder of the result as a list of values.

        :param item: field to return for each remaining record
        :param default: default value, used if the index of key is unavailable
        :returns: list of individual values
        """
        return [record.value(item, default) for record in self.records()]

    def values(self, *items):
        """Return the remainder of the result as a list of tuples.

        :param items: fields to return for each remaining record
        :returns: list of value tuples
        """
        return [record.values(*items) for record in self.records()]

    def data(self, *items):
        """Return the remainder of the result as a list of dictionaries.

        :param items: fields to return for each remaining record
        :returns: list of dictionaries
        """
        return [record.data(*items) for record in self.records()]

