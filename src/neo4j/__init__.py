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


import typing as _t

from ._api import (  # noqa: F401 dynamic attributes
    NotificationCategory,
    NotificationDisabledCategory,
    NotificationDisabledClassification as _NotificationDisabledClassification,
    NotificationMinimumSeverity,
    NotificationSeverity,
    RoutingControl,
)
from ._async.driver import (
    AsyncBoltDriver,
    AsyncDriver,
    AsyncGraphDatabase,
    AsyncNeo4jDriver,
)
from ._async.work import (
    AsyncManagedTransaction,
    AsyncResult,
    AsyncSession,
    AsyncTransaction,
)
from ._conf import (
    TrustAll,
    TrustCustomCAs,
    TrustSystemCAs,
)
from ._data import Record
from ._meta import (
    ExperimentalWarning,
    get_user_agent,
    preview_warn as _preview_warn,
    PreviewWarning,
    version as __version__,
)
from ._sync.driver import (
    BoltDriver,
    Driver,
    GraphDatabase,
    Neo4jDriver,
)
from ._sync.work import (
    ManagedTransaction,
    Result,
    Session,
    Transaction,
)
from ._work import (  # noqa: F401 dynamic attribute
    EagerResult,
    GqlStatusObject as _GqlStatusObject,
    NotificationClassification as _NotificationClassification,
    Query,
    ResultSummary,
    SummaryCounters,
    SummaryInputPosition,
    SummaryNotification,
    unit_of_work,
)


if _t.TYPE_CHECKING:
    from ._api import NotificationDisabledClassification  # noqa: TCH004 false positive (dynamic attribute)
    from ._work import (
        GqlStatusObject,  # noqa: TCH004 false positive (dynamic attribute)
        NotificationClassification,  # noqa: TCH004 false positive (dynamic attribute)
    )

from ._addressing import (
    Address,
    IPv4Address,
    IPv6Address,
)
from .api import (
    Auth,  # TODO: Validate naming for Auth compared to other drivers.
)
from .api import (
    AuthToken,
    basic_auth,
    bearer_auth,
    Bookmarks,
    custom_auth,
    DEFAULT_DATABASE,
    kerberos_auth,
    READ_ACCESS,
    ServerInfo,
    SYSTEM_DATABASE,
    TRUST_ALL_CERTIFICATES,
    TRUST_SYSTEM_CA_SIGNED_CERTIFICATES,
    WRITE_ACCESS,
)


__all__ = [
    "DEFAULT_DATABASE",
    "READ_ACCESS",
    "SYSTEM_DATABASE",
    "TRUST_ALL_CERTIFICATES",
    "TRUST_SYSTEM_CA_SIGNED_CERTIFICATES",
    "WRITE_ACCESS",
    "Address",
    "AsyncBoltDriver",
    "AsyncDriver",
    "AsyncGraphDatabase",
    "AsyncManagedTransaction",
    "AsyncNeo4jDriver",
    "AsyncResult",
    "AsyncSession",
    "AsyncTransaction",
    "Auth",
    "AuthToken",
    "BoltDriver",
    "Bookmarks",
    "Driver",
    "EagerResult",
    "ExperimentalWarning",
    "GqlStatusObject",
    "GraphDatabase",
    "IPv4Address",
    "IPv6Address",
    "ManagedTransaction",
    "Neo4jDriver",
    "NotificationCategory",
    "NotificationClassification",
    "NotificationDisabledCategory",
    "NotificationDisabledClassification",
    "NotificationMinimumSeverity",
    "NotificationSeverity",
    "PreviewWarning",
    "Query",
    "Record",
    "Result",
    "ResultSummary",
    "RoutingControl",
    "ServerInfo",
    "Session",
    "SummaryCounters",
    "SummaryInputPosition",
    "SummaryNotification",
    "Transaction",
    "TrustAll",
    "TrustCustomCAs",
    "TrustSystemCAs",
    "__version__",
    "basic_auth",
    "bearer_auth",
    "custom_auth",
    "get_user_agent",
    "kerberos_auth",
    "unit_of_work",
]


def __getattr__(name) -> _t.Any:
    # TODO: 6.0 - remove this
    if name in {
        "NotificationClassification",
        "GqlStatusObject",
        "NotificationDisabledClassification",
    }:
        _preview_warn(
            f"{name} is part of GQLSTATUS support, "
            "which is a preview feature.",
            stack_level=2,
        )
        return globals()[f"_{name}"]
    raise AttributeError(f"module {__name__} has no attribute {name}")


def __dir__() -> list[str]:
    return __all__
