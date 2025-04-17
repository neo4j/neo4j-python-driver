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

import os
import typing as t


if t.TYPE_CHECKING:
    import typing_extensions as te


__all__ = [
    "BLOCKED_TESTKIT_FEATURES",
    "BMM_SUPPORT",
    "EXECUTE_QUERY_STABILIZED",
    "EXECUTE_QUERY_SUPPORT",
    "EXTRA_TESTKIT_FEATURES",
    "GQL_ERROR_SUPPORT",
    "GQL_STATUS_SUPPORT",
    "LIVENESS_CHECK_SUPPORT",
    "MTLS_SUPPORT",
    "NOTIFICATION_FILTER_SUPPORTED",
    "NOTIFICATION_WARNINGS_SUPPORTED",
    "PREVIEW_WARNING_SUPPORTED",
    "RESULT_FAILED_ERROR_SUPPORT",
    "SESSION_AUTH_STABILIZED",
    "SESSION_AUTH_SUPPORTED",
    "SUMMARY_NOTIFICATION_SUPPORTED",
    "TELEMETRY_SUPPORT",
    "VERSION",
    "is_gql_error",
    "is_result_failed_error",
]


def _get_time_warp_version() -> tuple[float, ...]:
    time_warp_env = os.environ.get("DRIVER_TIME_WARP")
    if not time_warp_env:
        return (float("inf"),)
    return tuple(int(e) for e in time_warp_env.split("."))


VERSION: te.Final[tuple[float, ...]] = _get_time_warp_version()
# was added as preview in 5.0, but changed in 5.3
BMM_SUPPORT: te.Final[bool] = VERSION >= (5, 3)
EXECUTE_QUERY_SUPPORT: te.Final[bool] = VERSION >= (5, 5)
SUMMARY_NOTIFICATION_SUPPORTED: te.Final[bool] = VERSION >= (5, 7)
NOTIFICATION_FILTER_SUPPORTED: te.Final[bool] = VERSION >= (5, 7)
EXECUTE_QUERY_STABILIZED: te.Final[bool] = VERSION >= (5, 8)
PREVIEW_WARNING_SUPPORTED: te.Final[bool] = VERSION >= (5, 8)
SESSION_AUTH_SUPPORTED: te.Final[bool] = VERSION >= (5, 8)
TELEMETRY_SUPPORT: te.Final[bool] = VERSION >= (5, 13)
SESSION_AUTH_STABILIZED: te.Final[bool] = VERSION >= (5, 14)
RESULT_FAILED_ERROR_SUPPORT: te.Final[bool] = VERSION >= (5, 14)
LIVENESS_CHECK_SUPPORT: te.Final[bool] = VERSION >= (5, 14)
MTLS_SUPPORT: te.Final[bool] = VERSION >= (5, 19)
NOTIFICATION_WARNINGS_SUPPORTED: te.Final[bool] = VERSION >= (5, 21)
GQL_STATUS_SUPPORT: te.Final[bool] = VERSION >= (5, 22)
GQL_ERROR_SUPPORT: te.Final[bool] = VERSION >= (5, 26)


def _get_blocked_testkit_features() -> frozenset[str]:
    blocked: list[str] = []
    if not BMM_SUPPORT:  # 5.3
        blocked.extend(("Feature:API:BookmarkManager",))
    if not NOTIFICATION_FILTER_SUPPORTED:  # 5.7
        blocked.extend(
            (
                "Feature:API:Driver:NotificationsConfig",
                "Feature:API:Session:NotificationsConfig",
            )
        )
    if VERSION < (5, 7):
        blocked.extend(
            (
                "Feature:API:Session:AuthConfig",
                "Feature:Bolt:5.1",
                "Feature:Bolt:5.2",
                "Optimization:AuthPipelining",
            )
        )
    if VERSION < (5, 8):
        blocked.extend(
            (
                "Feature:API:Driver.VerifyAuthentication",
                "Feature:API:Driver.SupportsSessionAuth",
                "Feature:API:Driver.ExecuteQuery:WithAuth",
                "Feature:API:Session:AuthConfig",
            )
        )
    if VERSION < (5, 9):
        blocked.extend(("Feature:Bolt:5.3",))
    if VERSION < (5, 11):
        blocked.extend(("Optimization:ExecuteQueryPipelining",))
    if not SESSION_AUTH_STABILIZED:  # 5.12
        blocked.extend(("Feature:Auth:Managed",))
    if not TELEMETRY_SUPPORT:  # 5.13
        blocked.extend(("Feature:Bolt:5.4",))
    if not LIVENESS_CHECK_SUPPORT:  # 5.14
        blocked.extend(("Feature:API:Liveness.Check",))
    if not MTLS_SUPPORT:  # 5.19
        blocked.extend(("Feature:API:SSLClientCertificate",))
    if not GQL_STATUS_SUPPORT:  # 5.22
        blocked.extend(("Feature:API:Summary:GqlStatusObjects",))
    if VERSION < (5, 23):
        blocked.extend(("Feature:Bolt:5.6",))
    if VERSION < (5, 26):
        blocked.extend(("Feature:Bolt:5.7",))
    if VERSION < (5, 28):
        blocked.extend(
            (
                "Feature:Bolt:HandshakeManifestV1",
                "Feature:Bolt:5.8",
                "Optimization:HomeDatabaseCache",
                "Optimization:HomeDbCacheBasicPrincipalIsImpersonatedUser",
            )
        )

    return frozenset(blocked)


def _get_extra_testkit_features() -> frozenset[str]:
    extra: list[str] = []
    if VERSION < (5, 28):
        extra.extend(("Feature:Bolt:4.0",))
    return frozenset(extra)


BLOCKED_TESTKIT_FEATURES: te.Final[frozenset[str]] = (
    _get_blocked_testkit_features()
)
EXTRA_TESTKIT_FEATURES: te.Final[frozenset[str]] = frozenset()


if GQL_ERROR_SUPPORT:
    from neo4j.exceptions import GqlError

    def is_gql_error(exc):
        return isinstance(exc, GqlError)
else:

    def is_gql_error(exc):
        return False


if RESULT_FAILED_ERROR_SUPPORT:
    from neo4j.exceptions import ResultFailedError

    def is_result_failed_error(exc):
        return isinstance(exc, ResultFailedError)
else:

    def is_result_failed_error(exc):
        return False
