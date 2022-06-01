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


from logging import CRITICAL, ERROR, WARNING, INFO, DEBUG, Formatter, StreamHandler, getLogger
from sys import stderr


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
    """

    def __init__(self, *logger_names):
        super(Watcher, self).__init__()
        self.logger_names = logger_names
        self.loggers = [getLogger(name) for name in self.logger_names]
        self.formatter = ColourFormatter("%(asctime)s  %(message)s")
        self.handlers = {}

    def __enter__(self):
        self.watch()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def watch(self, level=DEBUG, out=stderr):
        """Enable logging for all loggers.

        :param level: Minimum log level to show.
        :type level: int
        :param out: Output stream for all loggers.
        :type out: stream or file-like object
        """
        self.stop()
        handler = StreamHandler(out)
        handler.setFormatter(self.formatter)
        for logger in self. loggers:
            self.handlers[logger.name] = handler
            logger.addHandler(handler)
            logger.setLevel(level)

    def stop(self):
        """Disable logging for all loggers."""
        for logger in self.loggers:
            try:
                logger.removeHandler(self.handlers.pop(logger.name))
            except KeyError:
                pass


def watch(*logger_names, level=DEBUG, out=stderr):
    """Quick wrapper for using  :class:`.Watcher`.

    Create a Wathcer with the given configuration, enable watching and return
    it.

    Example::

        from neo4j.debug import watch

        watch("neo4j")
        # from now on, DEBUG logging to stderr is enabled in the driver

    :param logger_names: name of logger to watch
    :type logger_names: str
    :param level: minimum log level to show (default ``logging.DEBUG``)
    :type level: int
    :param out: where to send output (default ``sys.stderr``)
    :type out: stream or file-like object

    :return: Watcher instance
    :rtype: :class:`.Watcher`
    """
    watcher = Watcher(*logger_names)
    watcher.watch(level, out)
    return watcher
