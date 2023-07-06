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


from ctypes import (
    byref,
    c_long,
    c_longlong,
    CDLL,
    Structure,
)
from platform import uname
from time import time

from . import (
    Clock,
    ClockTime,
)
from ._arithmetic import nano_divmod


__all__ = [
    "SafeClock",
    "PEP564Clock",
    "LibCClock",
]


class SafeClock(Clock):
    """ Clock implementation that should work for any variant of Python.
    This clock is guaranteed microsecond precision.
    """

    @classmethod
    def precision(cls):
        return 6

    @classmethod
    def available(cls):
        return True

    def utc_time(self):
        seconds, nanoseconds = nano_divmod(int(time() * 1000000), 1000000)
        return ClockTime(seconds, nanoseconds * 1000)


class PEP564Clock(Clock):
    """ Clock implementation based on the PEP564 additions to Python 3.7.
    This clock is guaranteed nanosecond precision.
    """

    @classmethod
    def precision(cls):
        return 9

    @classmethod
    def available(cls):
        try:
            from time import time_ns
        except ImportError:
            return False
        else:
            return True

    def utc_time(self):
        from time import time_ns
        t = time_ns()
        seconds, nanoseconds = divmod(t, 1000000000)
        return ClockTime(seconds, nanoseconds)


class LibCClock(Clock):
    """ Clock implementation that works only on platforms that provide
    libc. This clock is guaranteed nanosecond precision.
    """

    __libc = "libc.dylib" if uname()[0] == "Darwin" else "libc.so.6"

    class _TimeSpec(Structure):
        _fields_ = [
            ("seconds", c_longlong),
            ("nanoseconds", c_long),
        ]

    @classmethod
    def precision(cls):
        return 9

    @classmethod
    def available(cls):
        try:
            _ = CDLL(cls.__libc)
        except OSError:
            return False
        else:
            return True

    def utc_time(self):
        libc = CDLL(self.__libc)
        ts = self._TimeSpec()
        status = libc.clock_gettime(0, byref(ts))
        if status == 0:
            return ClockTime(ts.seconds, ts.nanoseconds)
        else:
            raise RuntimeError("clock_gettime failed with status %d" % status)
