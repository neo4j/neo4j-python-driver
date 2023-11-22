# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
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
def logger_mocker(mocker) -> t.Generator[_TSetupMockProtocol, None, None]:
    original_levels = {}

    def setup_mock(*logger_names):
        nonlocal original_levels

        loggers = [logging.getLogger(name) for name in logger_names]
        for logger in loggers:
            original_levels[logger] = logger.level
            mocker.patch.object(logger, "addHandler")
            mocker.patch.object(logger, "removeHandler")
            mocker.spy(logger, "setLevel")
        return loggers

    yield setup_mock

    for logger, level in original_levels.items():
        logger.setLevel(level)


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


WATCH_ARGS = (
    #default_level, level, expected_level
    (None, None, logging.DEBUG),
    (logging.WARNING, None, logging.WARNING),
    (logging.WARNING, logging.DEBUG, logging.DEBUG),
    (logging.DEBUG, logging.WARNING, logging.WARNING),
    (logging.INFO, None, logging.INFO),
    (logging.INFO, logging.DEBUG, logging.DEBUG),
    (logging.DEBUG, logging.INFO, logging.INFO),
    (1, None, 1),
    (None, 1, 1),
)


def _setup_watch(logger_name, default_level, level):
    kwargs = {}
    if default_level is not None:
        kwargs["default_level"] = default_level
    watcher = neo4j_debug.Watcher(logger_name, **kwargs)
    kwargs = {}
    if level is not None:
        kwargs["level"] = level
    watcher.watch(**kwargs)


@pytest.mark.parametrize(
    ("default_level", "level", "expected_level"),
    WATCH_ARGS
)
@pytest.mark.parametrize(
    "effective_level",
    (logging.DEBUG, logging.WARNING, logging.INFO, logging.ERROR)
)
def test_watcher_level(
    logger_mocker, default_level, level, expected_level, effective_level,
) -> None:
    logger_name = "neo4j"
    logger = logger_mocker(logger_name)[0]
    logger.setLevel(effective_level)
    logger.setLevel.reset_mock()
    _setup_watch(logger_name, default_level, level)

    (handler,), _ = logger.addHandler.call_args
    assert handler.level == expected_level
    if effective_level <= expected_level:
        logger.setLevel.assert_not_called()
    else:
        logger.setLevel.assert_called_once_with(expected_level)


@pytest.mark.parametrize(
    ("default_level1", "level1", "expected_level1"),
    WATCH_ARGS
)
@pytest.mark.parametrize(
    ("default_level2", "level2", "expected_level2"),
    WATCH_ARGS
)
@pytest.mark.parametrize(
    "effective_level",
    (logging.DEBUG, logging.WARNING, logging.INFO, logging.ERROR)
)
def test_watcher_level_multiple_watchers(
    logger_mocker, default_level1, level1, expected_level1,
    default_level2, level2, expected_level2,
    effective_level,
) -> None:
    logger_name = "neo4j"
    logger = logger_mocker(logger_name)[0]
    logger.setLevel(effective_level)
    logger.setLevel.reset_mock()
    _setup_watch(logger_name, default_level1, level1)
    _setup_watch(logger_name, default_level2, level2)

    assert logger.addHandler.call_count == 2
    (handler1,), _ = logger.addHandler.call_args_list[0]
    (handler2,), _ = logger.addHandler.call_args_list[1]

    assert handler1.level == expected_level1
    assert handler2.level == expected_level2

    expected_logger_level = min(expected_level1, expected_level2)
    if effective_level <= expected_logger_level:
        logger.setLevel.assert_not_called()
    else:
        if effective_level > expected_level1 > expected_level2:
            assert logger.setLevel.call_count == 2
        else:
            assert logger.setLevel.call_count == 1
        (level,), _ = logger.setLevel.call_args_list[-1]
        assert level == expected_logger_level


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

    logger.addHandler.assert_called_once()
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

    logger.addHandler.assert_called_once()
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


def _assert_task_injection(
    async_: bool, mocker, logger_mocker, colour: bool, thread: bool, task: bool
) -> None:
    handler_cls_mock = mocker.patch("neo4j.debug.StreamHandler", autospec=True)
    handler_mock = handler_cls_mock.return_value
    logger_name = "neo4j"
    logger_mocker(logger_name)[0]
    watcher = neo4j_debug.Watcher(logger_name, colour=colour,
                                  thread_info=thread, task_info=task)
    record_mock = mocker.Mock(spec=logging.LogRecord)
    assert not hasattr(record_mock, "task")

    watcher.watch()

    if task:
        handler_mock.addFilter.assert_called_once()
        (filter_,), _ = handler_mock.addFilter.call_args
        assert isinstance(filter_, logging.Filter)
        filter_.filter(record_mock)
        if async_:
            assert record_mock.task == id(asyncio.current_task())
        else:
            assert record_mock.task is None
    else:
        handler_mock.addFilter.assert_not_called()


@pytest.mark.parametrize("colour", (True, False))
@pytest.mark.parametrize("thread", (True, False))
@pytest.mark.parametrize("task", (True, False))
def test_watcher_task_injection(
    mocker, logger_mocker, colour, thread, task
) -> None:
    _assert_task_injection(False, mocker, logger_mocker, colour, thread, task)


@pytest.mark.parametrize("colour", (True, False))
@pytest.mark.parametrize("thread", (True, False))
@pytest.mark.parametrize("task", (True, False))
@mark_async_test
async def test_async_watcher_task_injection(
    mocker, logger_mocker, colour, thread, task
) -> None:
    _assert_task_injection(True, mocker, logger_mocker, colour, thread, task)
