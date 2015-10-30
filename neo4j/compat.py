#!/usr/bin/env python
# -*- encoding: utf-8 -*-

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


"""
This module provides compatibility functions between different versions
and flavours of Python. It is separate for clarity and deliberately
excluded from test coverage.
"""

__all__ = ["integer", "perf_counter", "secure_socket", "string", "urlparse"]


# Workaround for Python 2/3 type differences
try:
    unicode
except NameError:
    integer = int
    string = str

    def hex2(x):
        if x < 0x10:
            return "0" + hex(x)[2:].upper()
        else:
            return hex(x)[2:].upper()

else:
    integer = (int, long)
    string = (str, unicode)

    def hex2(x):
        x = ord(x)
        if x < 0x10:
            return "0" + hex(x)[2:].upper()
        else:
            return hex(x)[2:].upper()


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


try:
    from ssl import SSLContext, PROTOCOL_SSLv23, OP_NO_SSLv2, HAS_SNI
except ImportError:
    from ssl import wrap_socket, PROTOCOL_SSLv23

    def secure_socket(s, host):
        return wrap_socket(s, ssl_version=PROTOCOL_SSLv23)

else:

    def secure_socket(s, host):
        ssl_context = SSLContext(PROTOCOL_SSLv23)
        ssl_context.options |= OP_NO_SSLv2
        return ssl_context.wrap_socket(s, server_hostname=host if HAS_SNI else None)
