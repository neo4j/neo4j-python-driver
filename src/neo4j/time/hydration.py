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

# TODO: 6.0 - remove this file


from .._codec.hydration.v1.temporal import (
    dehydrate_date,
    dehydrate_datetime,
    dehydrate_duration,
    dehydrate_time,
    dehydrate_timedelta,
    get_date_unix_epoch,
    get_date_unix_epoch_ordinal,
    get_datetime_unix_epoch_utc,
    hydrate_date,
    hydrate_datetime,
    hydrate_duration,
    hydrate_time,
)
from .._meta import deprecation_warn


__all__ = [
    "get_date_unix_epoch",
    "get_date_unix_epoch_ordinal",
    "get_datetime_unix_epoch_utc",
    "hydrate_date",
    "dehydrate_date",
    "hydrate_time",
    "dehydrate_time",
    "hydrate_datetime",
    "dehydrate_datetime",
    "hydrate_duration",
    "dehydrate_duration",
    "dehydrate_timedelta",
]

deprecation_warn(
    "The module `neo4j.time.hydration` was made internal and will "
    "no longer be available for import in future versions.",
    stack_level=2
)
