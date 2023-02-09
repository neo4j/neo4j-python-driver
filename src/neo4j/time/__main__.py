#!/usr/bin/env python

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


def main():
    from . import (
        Clock,
        DateTime,
        UnixEpoch,
    )
    clock = Clock()
    time = clock.utc_time()
    print("Using %s" % type(clock).__name__)
    print("%s -> %s" % (time, DateTime.from_clock_time(time, UnixEpoch)))


if __name__ == "__main__":
    main()
