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


from logging import (
    CRITICAL,
    DEBUG,
    ERROR,
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
        s = super(ColourFormatter, self).format(record)
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

    :param logger_names: Names of loggers to watch.
    :type logger_names: str
    :param default_level: Default minimum log level to show.
        The level can be overridden by setting the level a level when calling
        :meth:`.watch`.
    :type default_level: int
    :param default_out: Default output stream for all loggers.
        The level can be overridden by setting the level a level when calling
        :meth:`.watch`.
    :type default_out: stream or file-like object
    :param colour: Whether the log levels should be indicated with ANSI colour
        codes.
    :type colour: bool
    """

    def __init__(self, *logger_names, default_level=DEBUG, default_out=stderr,
                 colour=False):
        super(Watcher, self).__init__()
        self.logger_names = logger_names
        self._loggers = [getLogger(name) for name in self.logger_names]
        self.default_level = default_level
        self.default_out = default_out
        self._handlers = {}

        format = "%(threadName)s(%(thread)d) %(asctime)s  %(message)s"
        if not colour:
            format = "[%(levelname)s] " + format

        formatter_cls = ColourFormatter if colour else Formatter
        self.formatter = formatter_cls(format)

    def __enter__(self):
        """Enable logging for all loggers."""
        self.watch()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Disable logging for all loggers."""
        self.stop()

    def watch(self, level=None, out=None):
        """Enable logging for all loggers.

        :param level: Minimum log level to show.
            If :const:`None`, the ``default_level`` is used.
        :type level: int or :const:`None`
        :param out: Output stream for all loggers.
            If :const:`None`, the ``default_out`` is used.
        :type out: stream or file-like object or :const:`None`
        """
        if level is None:
            level = self.default_level
        if out is None:
            out = self.default_out
        self.stop()
        handler = StreamHandler(out)
        handler.setFormatter(self.formatter)
        for logger in self. _loggers:
            self._handlers[logger.name] = handler
            logger.addHandler(handler)
            logger.setLevel(level)

    def stop(self):
        """Disable logging for all loggers."""
        for logger in self._loggers:
            try:
                logger.removeHandler(self._handlers.pop(logger.name))
            except KeyError:
                pass


def watch(*logger_names, level=DEBUG, out=stderr, colour=False):
    """Quick wrapper for using  :class:`.Watcher`.

    Create a Wathcer with the given configuration, enable watching and return
    it.

    Example::

        from neo4j.debug import watch

        watch("neo4j")
        # from now on, DEBUG logging to stderr is enabled in the driver

    :param logger_names: Names of loggers to watch.
    :type logger_names: str
    :param level: see ``default_level`` of :class:`.Watcher`.
    :type level: int
    :param out: see ``default_out`` of :class:`.Watcher`.
    :type out: stream or file-like object
    :param colour: see ``colour`` of :class:`.Watcher`.
    :type colour: bool

    :return: Watcher instance
    :rtype: :class:`.Watcher`
    """
    watcher = Watcher(*logger_names, colour=colour, default_level=level,
                      default_out=out)
    watcher.watch()
    return watcher
