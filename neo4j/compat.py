#!/usr/bin/env python
#! -*- encoding: UTF-8 -*-

# Copyright (c) 2002-2015 "Neo Technology,"
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


__all__ = ["integer", "perf_counter", "string", "urlparse"]


# Workaround for Python 2/3 type differences
try:
    unicode
except NameError:
    integer = int
    string = str
else:
    integer = (int, long)
    string = (str, unicode)


# Obtain a performance timer - this varies by platform and
# Jython support is even more tricky as the standard timer
# does not support nanoseconds. The combination below
# works with Python 2, Python 3 and Jython.
try:
    from java.lang.System import nanoTime
except ImportError:
    try:
        from time import perf_counter
    except ImportError:
        from time import time as perf_counter
else:
    def perf_counter():
        return nanoTime() / 1000000000


# The location of urlparse varies between Python 2 and 3
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse
