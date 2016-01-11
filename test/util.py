#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2016 "Neo Technology,"
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


import functools

from neo4j.util import Watcher


def watch(f):
    """ Decorator to enable log watching for the lifetime of a function.
    Useful for debugging unit tests.

    :param f: the function to decorate
    :return: a decorated function
    """
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        watcher = Watcher("neo4j")
        watcher.watch()
        f(*args, **kwargs)
        watcher.stop()
    return wrapper
