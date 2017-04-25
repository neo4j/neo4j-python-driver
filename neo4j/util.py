#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2017 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
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


from importlib import import_module
import logging
from os import getenv as getenv
from sys import stdout


class ServerVersion(object):
    def __init__(self, product, version_tuple, tags_tuple):
        self.product = product
        self.version_tuple = version_tuple
        self.tags_tuple = tags_tuple

    def at_least_version(self, major, minor):
        return self.version_tuple >= (major, minor)

    @classmethod
    def from_str(cls, full_version):
        if full_version is None:
            return ServerVersion("Neo4j", (3, 0), ())
        product, _, tagged_version = full_version.partition("/")
        tags = tagged_version.split("-")
        version = map(int, tags[0].split("."))
        return ServerVersion(product, tuple(version), tuple(tags[1:]))


class ColourFormatter(logging.Formatter):
    """ Colour formatter for pretty log output.
    """

    def format(self, record):
        s = super(ColourFormatter, self).format(record)
        if record.levelno == logging.CRITICAL:
            return "\x1b[31;1m%s\x1b[0m" % s  # bright red
        elif record.levelno == logging.ERROR:
            return "\x1b[33;1m%s\x1b[0m" % s  # bright yellow
        elif record.levelno == logging.WARNING:
            return "\x1b[33m%s\x1b[0m" % s    # yellow
        elif record.levelno == logging.INFO:
            return "\x1b[36m%s\x1b[0m" % s    # cyan
        elif record.levelno == logging.DEBUG:
            return "\x1b[34m%s\x1b[0m" % s    # blue
        else:
            return s


class Watcher(object):
    """ Log watcher for monitoring driver and protocol activity.
    """

    handlers = {}

    def __init__(self, logger_name):
        super(Watcher, self).__init__()
        self.logger_name = logger_name
        self.logger = logging.getLogger(self.logger_name)
        self.formatter = ColourFormatter("%(asctime)s  %(message)s")

    def __enter__(self):
        self.watch()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def watch(self, level=logging.INFO, out=stdout):
        self.stop()
        handler = logging.StreamHandler(out)
        handler.setFormatter(self.formatter)
        self.handlers[self.logger_name] = handler
        self.logger.addHandler(handler)
        self.logger.setLevel(level)

    def stop(self):
        try:
            self.logger.removeHandler(self.handlers[self.logger_name])
        except KeyError:
            pass


def watch(logger_name, level=logging.INFO, out=stdout):
    """ Quick wrapper for using the Watcher.

    :param logger_name: name of logger to watch
    :param level: minimum log level to show (default INFO)
    :param out: where to send output (default stdout)
    :return: Watcher instance
    """
    watcher = Watcher(logger_name)
    watcher.watch(level, out)
    return watcher


def import_best(c_module, py_module):
    pure_python = getenv("PURE_PYTHON", "")
    if pure_python:
        return import_module(py_module)
    else:
        try:
            return import_module(c_module)
        except ImportError:
            return import_module(py_module)
