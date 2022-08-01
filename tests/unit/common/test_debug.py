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

import io
import logging
import sys
import typing as t

import pytest


if t.TYPE_CHECKING:
    import typing_extensions as te

from neo4j import debug as neo4j_debug


if t.TYPE_CHECKING:

    class _TSetupMockProtocol(te.Protocol):
        def __call__(self, *args: str) -> t.Sequence[t.Any]:
            ...


@pytest.fixture
def add_handler_mocker(mocker) -> _TSetupMockProtocol:
    def setup_mock(*logger_names):
        loggers = [logging.getLogger(name) for name in logger_names]
        for logger in loggers:
            logger.addHandler = mocker.Mock()
            logger.removeHandler = mocker.Mock()
            logger.setLevel = mocker.Mock()
        return loggers

    return setup_mock


def test_watch_returns_watcher(add_handler_mocker) -> None:
    logger_name = "neo4j"
    add_handler_mocker(logger_name)
    watcher = neo4j_debug.watch(logger_name)
    assert isinstance(watcher, neo4j_debug.Watcher)


@pytest.mark.parametrize("logger_names",
                         (("neo4j",), ("foobar",), ("neo4j", "foobar")))
def test_watch_enables_logging(logger_names, add_handler_mocker) -> None:
    loggers = add_handler_mocker(*logger_names)
    neo4j_debug.watch(*logger_names)
    for logger in loggers:
        logger.addHandler.assert_called_once()


def test_watcher_watch_adds_logger(add_handler_mocker) -> None:
    logger_name = "neo4j"
    logger = add_handler_mocker(logger_name)[0]
    watcher = neo4j_debug.Watcher(logger_name)

    logger.addHandler.assert_not_called()
    watcher.watch()
    logger.addHandler.assert_called_once()


def test_watcher_stop_removes_logger(add_handler_mocker) -> None:
    logger_name = "neo4j"
    logger = add_handler_mocker(logger_name)[0]
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
    add_handler_mocker, default_level, level, expected_level
) -> None:
    logger_name = "neo4j"
    logger = add_handler_mocker(logger_name)[0]
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
    add_handler_mocker, default_out, out, expected_out
) -> None:
    logger_name = "neo4j"
    logger = add_handler_mocker(logger_name)[0]
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
def test_watcher_colour(add_handler_mocker, colour) -> None:
    logger_name = "neo4j"
    logger = add_handler_mocker(logger_name)[0]
    watcher = neo4j_debug.Watcher(logger_name, colour=colour)
    watcher.watch()

    (handler,), _ = logger.addHandler.call_args
    assert isinstance(handler, logging.Handler)
    assert isinstance(handler.formatter, logging.Formatter)
    if colour:
        assert isinstance(handler.formatter, neo4j_debug.ColourFormatter)
    else:
        assert not isinstance(handler.formatter, neo4j_debug.ColourFormatter)


@pytest.mark.parametrize("colour", (True, False))
def test_watcher_format(add_handler_mocker, colour) -> None:
    logger_name = "neo4j"
    logger = add_handler_mocker(logger_name)[0]
    watcher = neo4j_debug.Watcher(logger_name, colour=colour)
    watcher.watch()

    (handler,), _ = logger.addHandler.call_args
    assert isinstance(handler, logging.Handler)
    assert isinstance(handler.formatter, logging.Formatter)
    # Don't look at me like that. It's just for testing.
    format = handler.formatter._fmt
    if colour:
        assert format == "%(threadName)s(%(thread)d) %(asctime)s  %(message)s"
    else:
        assert format == "[%(levelname)-8s] " \
                         "%(threadName)s(%(thread)d) %(asctime)s  %(message)s"
