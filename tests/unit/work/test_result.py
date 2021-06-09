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


import pytest

from neo4j import Record
from neo4j.data import DataHydrator
from neo4j.work.result import Result


class Records:
    def __init__(self, fields, records):
        assert all(len(fields) == len(r) for r in records)
        self.fields = fields
        # self.records = [{"record_values": r} for r in records]
        self.records = records

    def __len__(self):
        return self.records.__len__()

    def __iter__(self):
        return self.records.__iter__()

    def __getitem__(self, item):
        return self.records.__getitem__(item)


class ConnectionStub:
    class Message:
        def __init__(self, message, *args, **kwargs):
            self.message = message
            self.args = args
            self.kwargs = kwargs

        def _cb(self, cb_name, *args, **kwargs):
            # print(self.message, cb_name.upper(), args, kwargs)
            cb = self.kwargs.get(cb_name)
            if callable(self.kwargs.get(cb_name)):
                cb(*args, **kwargs)

        def on_success(self, metadata):
            self._cb("on_success", metadata)

        def on_summary(self):
            self._cb("on_summary")

        def on_records(self, records):
            self._cb("on_records", records)

        def __eq__(self, other):
            return self.message == other

        def __repr__(self):
            return "Message(%s)" % self.message

    def __init__(self, records=None):
        self._records = records
        self.fetch_idx = 0
        self.record_idx = 0
        self.to_pull = None
        self.queued = []
        self.sent = []

    def send_all(self):
        self.sent += self.queued
        self.queued = []

    def fetch_message(self):
        if self.fetch_idx >= len(self.sent):
            pytest.fail("Waits for reply to never sent message")
        msg = self.sent[self.fetch_idx]
        if msg == "RUN":
            self.fetch_idx += 1
            msg.on_success({"fields": self._records.fields})
        elif msg == "DISCARD":
            self.fetch_idx += 1
            self.record_idx = len(self._records)
            msg.on_success()
        elif msg == "PULL":
            if self.to_pull is None:
                n = msg.kwargs.get("n", -1)
                if n < 0:
                    n = len(self._records)
                self.to_pull = min(n, len(self._records) - self.record_idx)
                # if to == len(self._records):
                #     self.fetch_idx += 1
            if self.to_pull > 0:
                record = self._records[self.record_idx]
                self.record_idx += 1
                self.to_pull -= 1
                msg.on_records([record])
            elif self.to_pull == 0:
                self.to_pull = None
                self.fetch_idx += 1
                if self.record_idx < len(self._records):
                    msg.on_success({"has_more": True})
                else:
                    msg.on_success({"bookmark": "foo"})
                    msg.on_summary()

    def run(self, *args, **kwargs):
        self.queued.append(ConnectionStub.Message("RUN", *args, **kwargs))

    def discard(self, *args, **kwargs):
        self.queued.append(ConnectionStub.Message("DISCARD", *args, **kwargs))

    def pull(self, *args, **kwargs):
        self.queued.append(ConnectionStub.Message("PULL", *args, **kwargs))

    server_info = "ServerInfo"

    def defunct(self):
        return False


class HydratorStub(DataHydrator):
    def hydrate(self, values):
        return values


def noop(*_, **__):
    pass


def test_result_iteration():
    records = [[1], [2], [3], [4], [5]]
    connection = ConnectionStub(records=Records("x", records))
    result = Result(connection, HydratorStub(), 2, noop, noop)
    result._run("CYPHER", {}, None, "r", None)
    received = []
    for i, record in enumerate(result):
        assert isinstance(record, Record)
        received.append([record.data().get("x", None)])
    assert received == records


def test_result_next():
    records = [[1], [2], [3], [4], [5]]
    connection = ConnectionStub(records=Records("x", records))
    result = Result(connection, HydratorStub(), 2, noop, noop)
    result._run("CYPHER", {}, None, "r", None)
    iter_ = iter(result)
    received = []
    for _ in range(len(records)):
        received.append([next(iter_).get("x", None)])
    with pytest.raises(StopIteration):
        received.append([next(iter_).get("x", None)])
    assert received == records


@pytest.mark.parametrize("records", ([[1], [2]], [[1]], []))
@pytest.mark.parametrize("fetch_size", (1, 2))
def test_result_peek(records, fetch_size):
    connection = ConnectionStub(records=Records("x", records))
    result = Result(connection, HydratorStub(), fetch_size, noop, noop)
    result._run("CYPHER", {}, None, "r", None)
    record = result.peek()
    if not records:
        assert record is None
    else:
        assert isinstance(record, Record)
        assert record.get("x") == records[0][0]


@pytest.mark.parametrize("records", ([[1], [2]], [[1]], []))
@pytest.mark.parametrize("fetch_size", (1, 2))
def test_result_single(records, fetch_size):
    connection = ConnectionStub(records=Records("x", records))
    result = Result(connection, HydratorStub(), fetch_size, noop, noop)
    result._run("CYPHER", {}, None, "r", None)
    with pytest.warns(None) as warning_record:
        record = result.single()
    if not records:
        assert not warning_record
        assert record is None
    else:
        if len(records) > 1:
            assert len(warning_record) == 1
        else:
            assert not warning_record
        assert isinstance(record, Record)
        assert record.get("x") == records[0][0]
