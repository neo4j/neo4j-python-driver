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

from contextlib import contextmanager

import pytest

from neo4j import (
    Session,
    SessionConfig,
    Transaction,
    unit_of_work,
)
from neo4j.io import IOPool

from ._fake_connection import fake_connection_generator


@pytest.fixture
def pool(mocker, fake_connection_generator):
    pool = mocker.Mock(spec=IOPool)
    assert not hasattr(pool, "acquired_connection_mocks")
    pool.acquired_connection_mocks = []

    def acquire_side_effect(*_, **__):
        connection = fake_connection_generator()
        pool.acquired_connection_mocks.append(connection)
        return connection

    pool.acquire.side_effect = acquire_side_effect
    return pool


def test_session_context_calls_close(mocker):
    s = Session(None, SessionConfig())
    mock_close = mocker.patch.object(s, 'close', autospec=True)
    with s:
        pass
    mock_close.assert_called_once_with()


@pytest.mark.parametrize("test_run_args", (
    ("RETURN $x", {"x": 1}), ("RETURN 1",)
))
@pytest.mark.parametrize(("repetitions", "consume"), (
    (1, False), (2, False), (2, True)
))
def test_opens_connection_on_run(pool, test_run_args, repetitions, consume):
    with Session(pool, SessionConfig()) as session:
        assert session._connection is None
        result = session.run(*test_run_args)
        assert session._connection is not None
        if consume:
            result.consume()


@pytest.mark.parametrize("test_run_args", (
    ("RETURN $x", {"x": 1}), ("RETURN 1",)
))
@pytest.mark.parametrize("repetitions", range(1, 3))
def test_closes_connection_after_consume(pool, test_run_args, repetitions):
    with Session(pool, SessionConfig()) as session:
        result = session.run(*test_run_args)
        result.consume()
        assert session._connection is None
    assert session._connection is None


@pytest.mark.parametrize("test_run_args", (
    ("RETURN $x", {"x": 1}), ("RETURN 1",)
))
def test_keeps_connection_until_last_result_consumed(pool, test_run_args):
    with Session(pool, SessionConfig()) as session:
        result1 = session.run(*test_run_args)
        result2 = session.run(*test_run_args)
        assert session._connection is not None
        result1.consume()
        assert session._connection is not None
        result2.consume()
        assert session._connection is None


def test_opens_connection_on_tx_begin(pool):
    with Session(pool, SessionConfig()) as session:
        assert session._connection is None
        with session.begin_transaction() as _:
            assert session._connection is not None


@pytest.mark.parametrize("test_run_args", (
    ("RETURN $x", {"x": 1}), ("RETURN 1",)
))
@pytest.mark.parametrize("repetitions", range(1, 3))
def test_keeps_connection_on_tx_run(pool, test_run_args, repetitions):
    with Session(pool, SessionConfig()) as session:
        with session.begin_transaction() as tx:
            for _ in range(repetitions):
                tx.run(*test_run_args)
                assert session._connection is not None


@pytest.mark.parametrize("test_run_args", (
        ("RETURN $x", {"x": 1}), ("RETURN 1",)
))
@pytest.mark.parametrize("repetitions", range(1, 3))
def test_keeps_connection_on_tx_consume(pool, test_run_args, repetitions):
    with Session(pool, SessionConfig()) as session:
        with session.begin_transaction() as tx:
            for _ in range(repetitions):
                result = tx.run(*test_run_args)
                result.consume()
                assert session._connection is not None


@pytest.mark.parametrize("test_run_args", (
        ("RETURN $x", {"x": 1}), ("RETURN 1",)
))
def test_closes_connection_after_tx_close(pool, test_run_args):
    with Session(pool, SessionConfig()) as session:
        with session.begin_transaction() as tx:
            for _ in range(2):
                result = tx.run(*test_run_args)
                result.consume()
            tx.close()
            assert session._connection is None
        assert session._connection is None


@pytest.mark.parametrize("test_run_args", (
        ("RETURN $x", {"x": 1}), ("RETURN 1",)
))
def test_closes_connection_after_tx_commit(pool, test_run_args):
    with Session(pool, SessionConfig()) as session:
        with session.begin_transaction() as tx:
            for _ in range(2):
                result = tx.run(*test_run_args)
                result.consume()
            tx.commit()
            assert session._connection is None
        assert session._connection is None


@pytest.mark.parametrize("bookmarks", (None, [], ["abc"], ["foo", "bar"]))
def test_session_returns_bookmark_directly(pool, bookmarks):
    with Session(pool, SessionConfig(bookmarks=bookmarks)) as session:
        if bookmarks:
            assert session.last_bookmark() == bookmarks[-1]
        else:
            assert session.last_bookmark() is None


@pytest.mark.parametrize(("query", "error_type"), (
    (None, ValueError),
    (1234, TypeError),
    ({"how about": "no?"}, TypeError),
    (["I don't", "think so"], TypeError),
))
def test_session_run_wrong_types(pool, query, error_type):
    with Session(pool, SessionConfig()) as session:
        with pytest.raises(error_type):
            session.run(query)


@pytest.mark.parametrize("tx_type", ("write_transaction", "read_transaction"))
def test_tx_function_argument_type(pool, tx_type):
    def work(tx):
        assert isinstance(tx, Transaction)

    with Session(pool, SessionConfig()) as session:
        getattr(session, tx_type)(work)


@pytest.mark.parametrize("tx_type", ("write_transaction", "read_transaction"))
@pytest.mark.parametrize("decorator_kwargs", (
    {},
    {"timeout": 5},
    {"metadata": {"foo": "bar"}},
    {"timeout": 5, "metadata": {"foo": "bar"}},

))
def test_decorated_tx_function_argument_type(pool, tx_type, decorator_kwargs):
    @unit_of_work(**decorator_kwargs)
    def work(tx):
        assert isinstance(tx, Transaction)

    with Session(pool, SessionConfig()) as session:
        getattr(session, tx_type)(work)


def test_session_tx_type(pool):
    with Session(pool, SessionConfig()) as session:
        tx = session.begin_transaction()
        assert isinstance(tx, Transaction)


@pytest.mark.parametrize(("parameters", "error_type"), (
    ({"x": None}, None),
    ({"x": True}, None),
    ({"x": False}, None),
    ({"x": 123456789}, None),
    ({"x": 3.1415926}, None),
    ({"x": float("nan")}, None),
    ({"x": float("inf")}, None),
    ({"x": float("-inf")}, None),
    ({"x": "foo"}, None),
    ({"x": bytearray([0x00, 0x33, 0x66, 0x99, 0xCC, 0xFF])}, None),
    ({"x": b"\x00\x33\x66\x99\xcc\xff"}, None),
    ({"x": [1, 2, 3]}, None),
    ({"x": ["a", "b", "c"]}, None),
    ({"x": ["a", 2, 1.234]}, None),
    ({"x": ["a", 2, ["c"]]}, None),
    ({"x": {"one": "eins", "two": "zwei", "three": "drei"}}, None),
    ({"x": {"one": ["eins", "uno", 1], "two": ["zwei", "dos", 2]}}, None),

    # maps must have string keys
    ({"x": {1: 'eins', 2: 'zwei', 3: 'drei'}}, TypeError),
    ({"x": {(1, 2): '1+2i', (2, 0): '2'}}, TypeError),
))
@pytest.mark.parametrize("run_type", ("auto", "unmanaged", "managed"))
def test_session_run_with_parameters(pool, parameters, error_type, run_type):
    @contextmanager
    def raises():
        if error_type is not None:
            with pytest.raises(error_type) as exc:
                yield exc
        else:
            yield None

    with Session(pool, SessionConfig()) as session:
        if run_type == "auto":
            with raises():
                session.run("RETURN $x", **parameters)
        elif run_type == "unmanaged":
            tx = session.begin_transaction()
            with raises():
                tx.run("RETURN $x", **parameters)
        elif run_type == "managed":
            def work(tx):
                with raises() as exc:
                    tx.run("RETURN $x", **parameters)
                if exc is not None:
                    raise exc
            with raises():
                session.write_transaction(work)
        else:
            raise ValueError(run_type)


@pytest.mark.parametrize(
    ("params", "kw_params", "expected_params"),
    (
        ({"x": 1}, {}, {"x": 1}),
        ({}, {"x": 1}, {"x": 1}),
        ({"x": 1}, {"y": 2}, {"x": 1, "y": 2}),
        ({"x": 1}, {"x": 2}, {"x": 2}),
        ({"x": 1}, {"x": 2}, {"x": 2}),
        ({"x": 1, "y": 3}, {"x": 2}, {"x": 2, "y": 3}),
        ({"x": 1}, {"x": 2, "y": 3}, {"x": 2, "y": 3}),
        # potentially internally used keyword arguments
        ({}, {"timeout": 2}, {"timeout": 2}),
        ({"timeout": 2}, {}, {"timeout": 2}),
        ({}, {"imp_user": "hans"}, {"imp_user": "hans"}),
        ({"imp_user": "hans"}, {}, {"imp_user": "hans"}),
        ({}, {"db": "neo4j"}, {"db": "neo4j"}),
        ({"db": "neo4j"}, {}, {"db": "neo4j"}),
        ({}, {"database": "neo4j"}, {"database": "neo4j"}),
        ({"database": "neo4j"}, {}, {"database": "neo4j"}),
    )
)
@pytest.mark.parametrize("run_type", ("auto", "unmanaged", "managed"))
def test_session_run_parameter_precedence(
    pool, params, kw_params, expected_params, run_type
):
    with Session(pool, SessionConfig()) as session:
        if run_type == "auto":
            session.run("RETURN $x", params, **kw_params)
        elif run_type == "unmanaged":
            tx = session.begin_transaction()
            tx.run("RETURN $x", params, **kw_params)
        elif run_type == "managed":
            def work(tx):
                tx.run("RETURN $x", params, **kw_params)
            session.write_transaction(work)
        else:
            raise ValueError(run_type)

    assert len(pool.acquired_connection_mocks) == 1
    connection_mock = pool.acquired_connection_mocks[0]
    connection_mock.run.assert_called_once()
    call_args, call_kwargs = connection_mock.run.call_args
    assert call_args[0] == "RETURN $x"
    assert call_kwargs["parameters"] == expected_params
