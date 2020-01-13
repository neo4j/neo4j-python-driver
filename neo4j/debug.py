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
    """ Log watcher for monitoring driver and protocol activity.
    """

    handlers = {}

    def __init__(self, *logger_names):
        super(Watcher, self).__init__()
        self.logger_names = logger_names
        self.loggers = [getLogger(name) for name in self.logger_names]
        self.formatter = ColourFormatter("%(asctime)s  %(message)s")

    def __enter__(self):
        self.watch()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def watch(self, level=DEBUG, out=stderr):
        self.stop()
        handler = StreamHandler(out)
        handler.setFormatter(self.formatter)
        for logger in self. loggers:
            self.handlers[logger.name] = handler
            logger.addHandler(handler)
            logger.setLevel(level)

    def stop(self):
        try:
            for logger in self.loggers:
                logger.removeHandler(self.handlers[logger.name])
        except KeyError:
            pass


def watch(*logger_names, level=DEBUG, out=stderr):
    """ Quick wrapper for using the Watcher.

    :param logger_name: name of logger to watch
    :param level: minimum log level to show (default DEBUG)
    :param out: where to send output (default stderr)
    :return: Watcher instance
    """
    watcher = Watcher(*logger_names)
    watcher.watch(level, out)
    return watcher
