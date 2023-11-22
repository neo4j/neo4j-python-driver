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


from ..v1.hydration_handler import *
from ..v1.hydration_handler import _GraphHydrator
from . import temporal  # type: ignore[no-redef]


class HydrationHandler(HydrationHandlerABC):  # type: ignore[no-redef]
    def __init__(self):
        super().__init__()
        self._created_scope = False
        self.struct_hydration_functions = {
            **self.struct_hydration_functions,
            b"X": spatial.hydrate_point,
            b"Y": spatial.hydrate_point,
            b"D": temporal.hydrate_date,
            b"T": temporal.hydrate_time,         # time zone offset
            b"t": temporal.hydrate_time,         # no time zone
            b"I": temporal.hydrate_datetime,     # time zone offset
            b"i": temporal.hydrate_datetime,     # time zone name
            b"d": temporal.hydrate_datetime,     # no time zone
            b"E": temporal.hydrate_duration,
        }
        self.dehydration_hooks.update(exact_types={
            Point: spatial.dehydrate_point,
            CartesianPoint: spatial.dehydrate_point,
            WGS84Point: spatial.dehydrate_point,
            Date: temporal.dehydrate_date,
            date: temporal.dehydrate_date,
            Time: temporal.dehydrate_time,
            time: temporal.dehydrate_time,
            DateTime: temporal.dehydrate_datetime,
            datetime: temporal.dehydrate_datetime,
            Duration: temporal.dehydrate_duration,
            timedelta: temporal.dehydrate_timedelta,
        })
        if np is not None:
            self.dehydration_hooks.update(exact_types={
                np.datetime64: temporal.dehydrate_np_datetime,
                np.timedelta64: temporal.dehydrate_np_timedelta,
            })
        if pd is not None:
            self.dehydration_hooks.update(exact_types={
                pd.Timestamp: temporal.dehydrate_pandas_datetime,
                pd.Timedelta: temporal.dehydrate_pandas_timedelta,
                type(pd.NaT): lambda _: None,
            })

    def new_hydration_scope(self):
        self._created_scope = True
        return HydrationScope(self, _GraphHydrator())
