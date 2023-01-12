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


"""
Functions to decide whether to run a subtest or not.

They take the subtest parameters as arguments and return
 - a string with describing the reason why the subtest should be skipped
 - None if the subtest should be run
"""


import pytz

from . import fromtestkit


def tz_id(**params):
    # We could do this automatically, but with an explicit black list we
    # make sure we know what we test and what we don't.
    # if params["tz_id"] not in pytz.common_timezones_set:
    #     return (
    #         "timezone id %s is not supported by the system" % params["tz_id"]
    #     )

    if params["tz_id"] in {
        "SystemV/AST4",
        "SystemV/AST4ADT",
        "SystemV/CST6",
        "SystemV/CST6CDT",
        "SystemV/EST5",
        "SystemV/EST5EDT",
        "SystemV/HST10",
        "SystemV/MST7",
        "SystemV/MST7MDT",
        "SystemV/PST8",
        "SystemV/PST8PDT",
        "SystemV/YST9",
        "SystemV/YST9YDT",
    }:
        return (
            "timezone id %s is not supported by the system" % params["tz_id"]
        )


def dt_conversion(**params):
    dt = params["dt"]
    try:
        fromtestkit.to_param(dt)
    except (pytz.UnknownTimeZoneError, ValueError) as e:
        return "cannot create desired dt %s: %r" % (dt, e)
