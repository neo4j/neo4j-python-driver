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

import asyncio
import io
import logging
import sys
import typing as t

import pytest


if t.TYPE_CHECKING:
    import typing_extensions as te

from neo4j import debug as neo4j_debug

from ..._async_compat import mark_async_test


if t.TYPE_CHECKING:

    class _TSetupMockProtocol(te.Protocol):
        def __call__(self, *args: str) -> t.Sequence[t.Any]:
            ...


@pytest.fixture
def logger_mocker(mocker) -> _TSetupMockProtocol:
    def setup_mock(*logger_names):
        loggers = [logging.getLogger(name) for name in logger_names]
        for logger in loggers:
            logger.addHandler = mocker.Mock()
            logger.addFilter = mocker.Mock(side_effect=logger.addFilter)
            logger.removeHandler = mocker.Mock()
            logger.setLevel = mocker.Mock()
        return loggers

    return setup_mock


def test_watch_returns_watcher(logger_mocker) -> None:
    logger_name = "neo4j"
    logger_mocker(logger_name)
    watcher = neo4j_debug.watch(logger_name)
    assert isinstance(watcher, neo4j_debug.Watcher)


@pytest.mark.parametrize("logger_names",
                         (("neo4j",), ("foobar",), ("neo4j", "foobar")))
def test_watch_enables_logging(logger_names, logger_mocker) -> None:
    loggers = logger_mocker(*logger_names)
    neo4j_debug.watch(*logger_names)
    for logger in loggers:
        logger.addHandler.assert_called_once()


def test_watcher_watch_adds_logger(logger_mocker) -> None:
    logger_name = "neo4j"
    logger = logger_mocker(logger_name)[0]
    watcher = neo4j_debug.Watcher(logger_name)

    logger.addHandler.assert_not_called()
    watcher.watch()
    logger.addHandler.assert_called_once()


def test_watcher_stop_removes_logger(logger_mocker) -> None:
    logger_name = "neo4j"
    logger = logger_mocker(logger_name)[0]
    watcher = neo4j_debug.Watcher(logger_name)

    watcher.watch()
    (handler,), _ = logger.addHandler.call_args

    logger.removeHandler.assert_not_called()
    watcher.stop()
    logger.removeHandler.assert_called_once_with(handler)


def test_watcher_context_manager(mocker) -> None:
    logger_name = "neo4j"
    watcher: t.Any = neo4j_debug.Watcher(logger_name)
    watcher.watch = mocker.Mock()
    watcher.stop = mocker.Mock()

    with watcher:
        watcher.watch.assert_called_once()
        watcher.stop.assert_not_called()
    watcher.stop.assert_called_once()


@pytest.mark.parametrize(
    ("default_level", "level", "expected_level"),
    (
        (None, None, logging.DEBUG),
        (logging.WARNING, None, logging.WARNING),
        (logging.WARNING, logging.DEBUG, logging.DEBUG),
        (logging.DEBUG, logging.WARNING, logging.WARNING),
        (1, None, 1),
        (None, 1, 1),
    )
)
def test_watcher_level(
    logger_mocker, default_level, level, expected_level
) -> None:
    logger_name = "neo4j"
    logger = logger_mocker(logger_name)[0]
    kwargs = {}
    if default_level is not None:
        kwargs["default_level"] = default_level
    watcher = neo4j_debug.Watcher(logger_name, **kwargs)
    kwargs = {}
    if level is not None:
        kwargs["level"] = level
    watcher.watch(**kwargs)

    (handler,), _ = logger.addHandler.call_args
    assert handler.level == logging.NOTSET
    logger.setLevel.assert_called_once_with(expected_level)


custom_log_out = io.StringIO()

@pytest.mark.parametrize(
    ("default_out", "out", "expected_out"),
    (
        (None, None, sys.stderr),
        (sys.stdout, None, sys.stdout),
        (sys.stdout, sys.stderr, sys.stderr),
        (sys.stderr, sys.stdout, sys.stdout),
        (custom_log_out, None, custom_log_out),
        (None, custom_log_out, custom_log_out),
    )
)
def test_watcher_out(
    logger_mocker, default_out, out, expected_out
) -> None:
    logger_name = "neo4j"
    logger = logger_mocker(logger_name)[0]
    kwargs = {}
    if default_out is not None:
        kwargs["default_out"] = default_out
    watcher = neo4j_debug.Watcher(logger_name, **kwargs)
    kwargs = {}
    if out is not None:
        kwargs["out"] = out
    watcher.watch(**kwargs)

    (handler,), _ = logger.addHandler.call_args
    assert isinstance(handler, logging.StreamHandler)
    assert handler.stream == expected_out


@pytest.mark.parametrize("colour", (True, False))
@pytest.mark.parametrize("thread", (True, False))
@pytest.mark.parametrize("task", (True, False))
def test_watcher_colour(logger_mocker, colour, thread, task) -> None:
    logger_name = "neo4j"
    logger = logger_mocker(logger_name)[0]
    watcher = neo4j_debug.Watcher(logger_name, colour=colour,
                                  thread_info=thread, task_info=task)
    watcher.watch()

    (handler,), _ = logger.addHandler.call_args
    assert isinstance(handler, logging.Handler)
    assert isinstance(handler.formatter, logging.Formatter)
    if colour:
        assert isinstance(handler.formatter, neo4j_debug.ColourFormatter)
    else:
        assert not isinstance(handler.formatter, neo4j_debug.ColourFormatter)


@pytest.mark.parametrize("colour", (True, False))
@pytest.mark.parametrize("thread", (True, False))
@pytest.mark.parametrize("task", (True, False))
def test_watcher_format(logger_mocker, colour, thread, task) -> None:
    logger_name = "neo4j"
    logger = logger_mocker(logger_name)[0]
    watcher = neo4j_debug.Watcher(logger_name, colour=colour,
                                  thread_info=thread, task_info=task)
    watcher.watch()

    (handler,), _ = logger.addHandler.call_args
    assert isinstance(handler, logging.Handler)
    assert isinstance(handler.formatter, logging.Formatter)
    expected_format = "%(asctime)s  %(message)s"
    if task:
        expected_format = "[Task %(task)-15s] " + expected_format
    if thread:
        expected_format = "[Thread %(thread)d] " + expected_format
    if not colour:
        expected_format = "[%(levelname)-8s] " + expected_format
    # Don't look at me like that. It's just for testing.
    format_ = handler.formatter._fmt
    assert format_ == expected_format


@pytest.mark.parametrize("colour", (True, False))
@pytest.mark.parametrize("thread", (True, False))
@pytest.mark.parametrize("task", (True, False))
def test_watcher_task_injection(
    mocker, logger_mocker, colour, thread, task
) -> None:
    logger_name = "neo4j"
    logger = logger_mocker(logger_name)[0]
    watcher = neo4j_debug.Watcher(logger_name, colour=colour,
                                  thread_info=thread, task_info=task)
    record_mock = mocker.Mock(spec=logging.LogRecord)
    assert not hasattr(record_mock, "task")

    watcher.watch()

    if task:
        (filter_,), _ = logger.addFilter.call_args
        assert isinstance(filter_, logging.Filter)
        filter_.filter(record_mock)
        assert record_mock.task is None
    else:
        logger.addFilter.assert_not_called()


@pytest.mark.parametrize("colour", (True, False))
@pytest.mark.parametrize("thread", (True, False))
@pytest.mark.parametrize("task", (True, False))
@mark_async_test
async def test_async_watcher_task_injection(
    mocker, logger_mocker, colour, thread, task
) -> None:
    logger_name = "neo4j"
    logger = logger_mocker(logger_name)[0]
    watcher = neo4j_debug.Watcher(logger_name, colour=colour,
                                  thread_info=thread, task_info=task)
    record_mock = mocker.Mock(spec=logging.LogRecord)
    assert not hasattr(record_mock, "task")

    watcher.watch()

    if task:
        (filter_,), _ = logger.addFilter.call_args
        assert isinstance(filter_, logging.Filter)
        filter_.filter(record_mock)
        assert record_mock.task == id(asyncio.current_task())
    else:
        logger.addFilter.assert_not_called()
