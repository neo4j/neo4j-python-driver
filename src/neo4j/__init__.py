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


from logging import getLogger as _getLogger

from ._api import (
    NotificationCategory,
    NotificationDisabledCategory,
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
    Config as _Config,
    PoolConfig as _PoolConfig,
    SessionConfig as _SessionConfig,
    TrustAll,
    TrustCustomCAs,
    TrustSystemCAs,
    WorkspaceConfig as _WorkspaceConfig,
)
from ._data import Record
from ._meta import (
    deprecated_package as _deprecated_package,
    deprecation_warn as _deprecation_warn,
    ExperimentalWarning,
    get_user_agent,
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
from ._work import (
    EagerResult,
    Query,
    ResultSummary,
    SummaryCounters,
    SummaryNotification,
    SummaryNotificationPosition,
    unit_of_work,
)
from .addressing import (
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
    Bookmark,
    Bookmarks,
    custom_auth,
    DEFAULT_DATABASE,
    kerberos_auth,
    READ_ACCESS,
    ServerInfo,
    SYSTEM_DATABASE,
    TRUST_ALL_CERTIFICATES,
    TRUST_SYSTEM_CA_SIGNED_CERTIFICATES,
    Version,
    WRITE_ACCESS,
)


__all__ = [
    "__version__",
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
    "basic_auth",
    "bearer_auth",
    "BoltDriver",
    "Bookmark",
    "Bookmarks",
    "Config",
    "custom_auth",
    "DEFAULT_DATABASE",
    "NotificationDisabledCategory",
    "Driver",
    "EagerResult",
    "ExperimentalWarning",
    "get_user_agent",
    "GraphDatabase",
    "IPv4Address",
    "IPv6Address",
    "kerberos_auth",
    "log",
    "ManagedTransaction",
    "NotificationMinimumSeverity",
    "Neo4jDriver",
    "NotificationCategory",
    "NotificationFilter",
    "NotificationSeverity",
    "PoolConfig",
    "PreviewWarning",
    "Query",
    "READ_ACCESS",
    "Record",
    "Result",
    "ResultSummary",
    "RenewableAuth",
    "RoutingControl",
    "ServerInfo",
    "Session",
    "SessionConfig",
    "SummaryCounters",
    "SummaryNotification",
    "SummaryNotificationPosition",
    "Transaction",
    "TRUST_ALL_CERTIFICATES",
    "TRUST_SYSTEM_CA_SIGNED_CERTIFICATES",
    "TrustAll",
    "TrustCustomCAs",
    "TrustSystemCAs",
    "unit_of_work",
    "Version",
    "WorkspaceConfig",
    "WRITE_ACCESS",
]


_log = _getLogger("neo4j")


def __getattr__(name):
    # TODO: 6.0 - remove this
    if name in (
        "log", "Config", "PoolConfig", "SessionConfig", "WorkspaceConfig"
    ):
        from ._meta import deprecation_warn
        deprecation_warn(
            "Importing {} from neo4j is deprecated without replacement. It's "
            "internal and will be removed in a future version."
            .format(name),
            stack_level=2
        )
        return globals()[f"_{name}"]
    raise AttributeError(f"module {__name__} has no attribute {name}")


def __dir__():
    return __all__


if _deprecated_package:
    _deprecation_warn(
        "The neo4j driver was installed under the package name `noe4j-driver` "
        "which is deprecated and will stop receiving updates starting with "
        "version 6.0.0. Please install `neo4j` instead (which is an alias, "
        "i.e., a drop-in replacement). See https://pypi.org/project/neo4j/ ."
    )
