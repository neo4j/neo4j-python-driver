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


from unittest import TestLoader, TextTestRunner

try:
    from coverage import Coverage
except ImportError:
    Coverage = None


def _run_tests(here):
    test_suite = TestLoader().discover(here)
    TextTestRunner(verbosity=2).run(test_suite)


def run_tests(*here):
    if Coverage is None:
        for x in here:
            _run_tests(x)
    else:
        coverage = Coverage()
        coverage.start()
        for x in here:
            _run_tests(x)
        coverage.stop()
        coverage.save()
