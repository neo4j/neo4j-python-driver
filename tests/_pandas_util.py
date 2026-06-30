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


from __future__ import annotations

import warnings

import pandas as pd

from neo4j._meta import copy_signature


warnings.filterwarnings(
    "ignore",
    "The 'generic' unit for NumPy timedelta is deprecated",
    DeprecationWarning,
    __name__,
)


@copy_signature(pd.Timedelta)
def pd_timedelta(*args, **kwargs):
    # Pandas is using a deprecated numpy API.
    # However, we can't mute the error for pandas only as the offending line
    # reported is the one calling pandas. Not sure why, maybe it's a
    # Cython thing.
    return pd.Timedelta(*args, **kwargs)
