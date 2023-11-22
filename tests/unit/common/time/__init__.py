# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
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


from neo4j.time import (
    Clock,
    ClockTime,
)


# The existence of this class will make the driver's custom date time
# implementation use it instead of a real clock since its precision it higher
# than all the other clocks (only up to nanoseconds).
class FixedClock(Clock):
    @classmethod
    def available(cls):
        return True

    @classmethod
    def precision(cls):
        return 12

    @classmethod
    def local_offset(cls):
        return ClockTime()

    def utc_time(self):
        return ClockTime(45296, 789000001)
