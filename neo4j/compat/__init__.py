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


"""
This module provides compatibility functions between different versions
and flavours of Python. It is separate for clarity and deliberately
excluded from test coverage.
"""


# Workaround for Python 2/3 type differences
try:
    unicode
except NameError:
    # Python 3

    integer = int
    string = str
    unicode = str

    def ustr(x):
        if isinstance(x, bytes):
            return x.decode("utf-8")
        elif isinstance(x, str):
            return x
        else:
            return str(x)

    def memoryview_at(view, index):
        return view[index]

else:
    # Python 2

    integer = (int, long)
    string = (str, unicode)
    unicode = unicode

    def ustr(x):
        if isinstance(x, str):
            return x.decode("utf-8")
        elif isinstance(x, unicode):
            return x
        else:
            return unicode(x)

    def memoryview_at(view, index):
        return ord(view[index])

try:
    from multiprocessing import Array, Process
except ImportError:
    # Workaround for Jython

    from array import array
    from threading import Thread as Process

    def Array(typecode, size):
        return array(typecode, [0] * size)


# Obtain a performance timer - this varies by platform and
# Jython support is even more tricky as the standard timer
# does not support nanoseconds. The combination below
# works with Python 2, Python 3 and Jython.
try:
    from java.lang.System import nanoTime
except ImportError:
    JYTHON = False

    try:
        from time import perf_counter
    except ImportError:
        from time import time as perf_counter
else:
    JYTHON = True

    def perf_counter():
        return nanoTime() / 1000000000


# The location of urlparse varies between Python 2 and 3
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse
