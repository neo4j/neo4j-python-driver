# Copyright (c) "Neo4j"
# Neo4j Sweden AB [http://neo4j.com]
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


__all__ = [
    "__version__",
    "Address",
    "AsyncBoltDriver",
    "AsyncDriver",
    "AsyncGraphDatabase",
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
    "Config",
    "custom_auth",
    "DEFAULT_DATABASE",
    "Driver",
    "ExperimentalWarning",
    "GraphDatabase",
    "IPv4Address",
    "IPv6Address",
    "kerberos_auth",
    "Neo4jDriver",
    "PoolConfig",
    "Query",
    "READ_ACCESS",
    "Record",
    "Result",
    "ResultSummary",
    "ServerInfo",
    "Session",
    "SessionConfig",
    "SummaryCounters",
    "Transaction",
    "TRUST_ALL_CERTIFICATES",
    "TRUST_SYSTEM_CA_SIGNED_CERTIFICATES",
    "unit_of_work",
    "Version",
    "WorkspaceConfig",
    "WRITE_ACCESS",
]


from logging import getLogger

from ._async.driver import (
    AsyncBoltDriver,
    AsyncDriver,
    AsyncGraphDatabase,
    AsyncNeo4jDriver,
)
from ._async.work import (
    AsyncResult,
    AsyncSession,
    AsyncTransaction,
)
from ._sync.driver import (
    BoltDriver,
    Driver,
    GraphDatabase,
    Neo4jDriver,
)
from ._sync.work import (
    Result,
    Session,
    Transaction,
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
from .conf import (
    Config,
    PoolConfig,
    SessionConfig,
    WorkspaceConfig,
)
from .data import Record
from .meta import (
    experimental,
    ExperimentalWarning,
    get_user_agent,
    version as __version__,
)
from .work import (
    Query,
    ResultSummary,
    SummaryCounters,
    unit_of_work,
)


log = getLogger("neo4j")
