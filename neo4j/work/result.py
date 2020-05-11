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


class Result:
    """A handler for the result of Cypher query execution. Instances
    of this class are typically constructed and returned by
    :meth:`.Session.run` and :meth:`.Transaction.run`.
    """

    def __init__(self, connection, hydrant):
        self._connection = connection
        self._hydrant = hydrant
        self._metadata = None
        self._records = deque()
        self._summary = None
        self._discarding = False
        self._attached = False
        self._qid = -1

    def _run(self, query, parameters, db, access_mode, bookmarks, **kwparameters):
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

        def on_attached(iterable):
            self._metadata.update(iterable)
            self._qid = iterable.get("qid")
            self._keys = iterable.get("fields")
            self._attached = True

        def on_failed_attach():
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
        self._connection.fetch_message() # Receive response to run

    def _pull(self):

        # TODO:
        fetch_size = 10

        def on_records(records):
            if not self._discarding:
                self._records.extend(self._hydrant.hydrate_records(self._keys, records))

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
            # TODO: _metadata["bookmark" should be used!

        def on_summary():
            self._attached = False

        def on_failure():
            pass

        self._connection.pull(
            n=fetch_size,
            qid=self._qid,
            on_records=on_records,
            on_success=on_success,
            on_failure=on_failure,
            on_summary=on_summary)

    def __iter__(self):
        return self.records()

    # TODO: Needed?
    #@property
    #def session(self):
    #    """The :class:`.Session` to which this result is attached, if any.
    #    """
    #    return self._session

    # Not needed
    #def attached(self):
    #    """Indicator for whether or not this result is still attached to
    #    an open :class:`.Session`.
    #    """
    #    #return self._session
    #    return self._attached

    def _detach(self):
        """Detach this result from its parent session by fetching the
        remainder of this result from the network into the buffer.

        :returns: number of records fetched
        """
        #if self._attached:
        #if self.attached():
            #return self._session.detach(self, sync=sync)
        #    re
        #else:
        #    return 0
        #self._connection.send_all()
        #while result.attached():
            #count += self.fetch()
        #self._connection.fetch_message()

        while self._attached:
            self._connection.fetch_message()

    #        detail_count, _ = self._connection.fetch_message()
        

    def keys(self):
        """The keys for the records in this result.

        :returns: tuple of key names
        """
        return self._keys

    def records(self):
        """Generator for records obtained from this result.

        :yields: iterable of :class:`.Record` objects
        """
        while self._records:
            yield _self._records.popleft()
        #attached = self.attached
        #if attached():

        # Set to False when summary received
        while self._attached:
            self._connection.fetch_message()
            if self._attached:
                yield _self._records.popleft()


        # if self._attached:
        #     #self._connection.send_all()
        #     #self._session.send()
        #     while self._attached:

        #     _ = self._session.fetch()  # Blocking call, this call can detach the session
        #     if self._session:
        #         cx_state = self._session.connection_state()
        #         if cx_state == "pull":
        #             log.debug("This should never happen because Session.fetch() is a blocking call")
        #         if cx_state == "streaming_has_more":
        #             if self._state == "discard":
        #                 self._session.set_connection_state("discard_all")
        #                 self._session.discard_all()
        #                 self._session.send()
        #             else:
        #                 self._session.set_connection_state("pull")
        #                 self._session.pull()
        #                 self._session.send()

        #     while records_buffer:
        #         record = next_record()
        #         yield record

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

        #if self._attached:
        #    self._discarding = True
        #    for _ in self:
        #        pass
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
        if self._records:
            return self._records[0]
        if not self._attached:
            return None
        while self._attached:
            self._connection.fetch_message()
            if self._records:
                return self._records[0]

        #if not self.attached():
        #    return None
        #if self.attached():
        #    self._session.send()
        #while self.attached() and not records:
        #    self._session.fetch()
        #    if records:
        #        return records[0]
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

