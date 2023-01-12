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


import re
import warnings
from contextlib import contextmanager


@contextmanager
def warning_check(category, message):
    with warnings.catch_warnings(record=True) as warn_log:
        warnings.filterwarnings("always", category=category, message=message)
        yield
    if len(warn_log) != 1:
        raise AssertionError("Expected 1 warning, found %d: %s"
                             % (len(warn_log), warn_log))


@contextmanager
def warnings_check(category_message_pairs):
    with warnings.catch_warnings(record=True) as warn_log:
        for category, message in category_message_pairs:
            warnings.filterwarnings("always", category=category,
                                    message=message)
        yield
    if len(warn_log) != len(category_message_pairs):
        raise AssertionError(
            "Expected %d warnings, found %d: %s"
            % (len(category_message_pairs), len(warn_log), warn_log)
        )
    category_message_pairs = [
        (category, re.compile(message, re.I))
        for category, message in category_message_pairs
    ]
    for category, matcher in category_message_pairs:
        match = None
        for i, warning in enumerate(warn_log):
            if (
                warning.category == category
                and matcher.match(warning.message.args[0])
            ):
                match = i
                break
        if match is None:
            raise AssertionError(
                "Expected warning not found: %r %r"
                % (category, matcher.pattern)
            )
        warn_log.pop(match)
