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
import typing as t
from logging import (
    CRITICAL,
    DEBUG,
    ERROR,
    Filter,
    Formatter,
    getLogger,
    INFO,
    StreamHandler,
    WARNING,
)
from sys import stderr


__all__ = [
    "Watcher",
    "watch"
]


class ColourFormatter(Formatter):
    """ Colour formatter for pretty log output.
    """

    def format(self, record):
        s = super().format(record)
        if record.levelno == CRITICAL:
            return "\x1b[31;1m%s\x1b[0m" % s  # bright red
        elif record.levelno == ERROR:
            return "\x1b[33;1m%s\x1b[0m" % s  # bright yellow
        elif record.levelno == WARNING:
            return "\x1b[33m%s\x1b[0m" % s    # yellow
        elif record.levelno == INFO:
            return "\x1b[37m%s\x1b[0m" % s    # white
        elif record.levelno == DEBUG:
            return "\x1b[36m%s\x1b[0m" % s    # cyan
        else:
            return s


class TaskIdFilter(Filter):
    """Injecting async task id into log records."""

    def filter(self, record):
        try:
            record.task = id(asyncio.current_task())
        except RuntimeError:
            record.task = None
        return True


class Watcher:
    """Log watcher for easier logging setup.

    Example::

        from neo4j.debug import Watcher

        with Watcher("neo4j"):
            # DEBUG logging to stderr enabled within this context
            ...  # do something

    .. note:: The Watcher class is not thread-safe. Having Watchers in multiple
        threads can lead to duplicate log messages as the context manager will
        enable logging for all threads.

    .. note::
        The exact logging format and messages are not part of the API contract
        and might change at any time without notice. They are meant for
        debugging purposes and human consumption only.

    :param logger_names: Names of loggers to watch.
    :param default_level: Default minimum log level to show.
        The level can be overridden by setting ``level`` when calling
        :meth:`.watch`.
    :param default_out: Default output stream for all loggers.
        The level can be overridden by setting ``out`` when calling
        :meth:`.watch`.
    :type default_out: stream or file-like object
    :param colour: Whether the log levels should be indicated with ANSI colour
        codes.
    :param thread_info: whether to include information about the current
        thread in the log message. Defaults to :const:`True`.
    :param task_info: whether to include information about the current
        async task in the log message. Defaults to :const:`True`.

    .. versionchanged:: 5.3

        * Added ``thread_info`` and ``task_info`` parameters.
        * Logging format around thread and task information changed.
    """

    def __init__(
        self,
        *logger_names: t.Optional[str],
        default_level: int = DEBUG,
        default_out: t.TextIO = stderr,
        colour: bool = False,
        thread_info: bool = True,
        task_info: bool = True,
    ) -> None:
        super(Watcher, self).__init__()
        self.logger_names = logger_names
        self._loggers = [getLogger(name) for name in self.logger_names]
        self.default_level = default_level
        self.default_out = default_out
        self._handlers: t.Dict[str, StreamHandler] = {}
        self._task_info = task_info

        format_ = "%(asctime)s  %(message)s"
        if task_info:
            format_ = "[Task %(task)-15s] " + format_
        if thread_info:
            format_ = "[Thread %(thread)d] " + format_
        if not colour:
            format_ = "[%(levelname)-8s] " + format_
        formatter_cls = ColourFormatter if colour else Formatter
        self.formatter = formatter_cls(format_)

    def __enter__(self) -> Watcher:
        """Enable logging for all loggers."""
        self.watch()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Disable logging for all loggers."""
        self.stop()

    def watch(
        self, level: t.Optional[int] = None, out: t.Optional[t.TextIO] = None
    ) -> None:
        """Enable logging for all loggers.

        :param level: Minimum log level to show.
            If :data:`None`, the ``default_level`` is used.
        :param out: Output stream for all loggers.
            If :data:`None`, the ``default_out`` is used.
        :type out: stream or file-like object
        """
        if level is None:
            level = self.default_level
        if out is None:
            out = self.default_out
        self.stop()
        handler = StreamHandler(out)
        handler.setFormatter(self.formatter)
        handler.setLevel(level)
        if self._task_info:
            handler.addFilter(TaskIdFilter())
        for logger in self. _loggers:
            self._handlers[logger.name] = handler
            logger.addHandler(handler)
            if logger.getEffectiveLevel() > level:
                logger.setLevel(level)

    def stop(self) -> None:
        """Disable logging for all loggers."""
        for logger in self._loggers:
            try:
                logger.removeHandler(self._handlers.pop(logger.name))
            except KeyError:
                pass


def watch(
    *logger_names: t.Optional[str],
    level: int = DEBUG,
    out: t.TextIO = stderr,
    colour: bool = False,
    thread_info: bool = True,
    task_info: bool = True,
) -> Watcher:
    """Quick wrapper for using  :class:`.Watcher`.

    Create a Watcher with the given configuration, enable watching and return
    it.

    Example::

        from neo4j.debug import watch

        watch("neo4j")
        # from now on, DEBUG logging to stderr is enabled in the driver

    .. note::
        The exact logging format and messages are not part of the API contract
        and might change at any time without notice. They are meant for
        debugging purposes and human consumption only.

    :param logger_names: Names of loggers to watch.
    :param level: see ``default_level`` of :class:`.Watcher`.
    :param out: see ``default_out`` of :class:`.Watcher`.
    :type out: stream or file-like object
    :param colour: see ``colour`` of :class:`.Watcher`.
    :param thread_info: see ``thread_info`` of :class:`.Watcher`.
    :param task_info: see ``task_info`` of :class:`.Watcher`.

    :returns: Watcher instance
    :rtype: :class:`.Watcher`

    .. versionchanged:: 5.3

        * Added ``thread_info`` and ``task_info`` parameters.
        * Logging format around thread and task information changed.
    """
    watcher = Watcher(*logger_names, default_level=level, default_out=out,
                      colour=colour, thread_info=thread_info,
                      task_info=task_info)
    watcher.watch()
    return watcher


class Connection:
    def connect(self):
        self.hello()  # buffer HELLO message
        self.logon()  # buffer LOGON message
        self.send_and_receive()  # send HELLO and LOGON, receive 2x SUCCESS

    def reauth(self):
        self.logoff()  # buffer LOGOFF message
        self.logon()  # buffer LOGON message
        self.send_and_receive()  # send LOGOFF and LOGON, receive 2x SUCCESS
