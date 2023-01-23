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


import pytest

from ..v2.test_temporal_dehydration import (
    TestTimeDehydration as _TestTimeDehydrationV2,
)
from .test_temporal_dehydration import (
    HydrationHandler,  # testing the same hydration handler
)
from .test_temporal_dehydration import (
    TestTimeDehydration as _TestTimeDehydrationV1,
)


class UTCPatchedTimeDehydrationMeta(type):
    def __new__(mcs, name, bases, attrs):
        for test_func in (
            "test_date_time_fixed_offset",
            "test_native_date_time_fixed_offset",
            "test_pandas_date_time_fixed_offset",
            "test_date_time_fixed_negative_offset",
            "test_native_date_time_fixed_negative_offset",
            "test_pandas_date_time_fixed_negative_offset",
            "test_date_time_zone_id",
            "test_native_date_time_zone_id",
            "test_pandas_date_time_zone_id",
        ):
            if not hasattr(_TestTimeDehydrationV2, test_func):
                continue
            attrs[test_func] = getattr(_TestTimeDehydrationV2, test_func)

        return super(UTCPatchedTimeDehydrationMeta, mcs).__new__(
            mcs, name, bases, attrs
        )


class TestUTCPatchedTimeDehydration(
    _TestTimeDehydrationV1, metaclass=UTCPatchedTimeDehydrationMeta
):
    @pytest.fixture
    def hydration_handler(self):
        handler = HydrationHandler()
        handler.patch_utc()
        return handler
