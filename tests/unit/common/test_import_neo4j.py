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


import importlib
import re

import pytest

from neo4j import PreviewWarning


def test_import_neo4j():
    import neo4j


NEO4J_ATTRIBUTES = (
    # (name, warning)
    ("__version__", None),
    ("Address", None),
    ("AsyncBoltDriver", None),
    ("AsyncDriver", None),
    ("AsyncGraphDatabase", None),
    ("AsyncManagedTransaction", None),
    ("AsyncNeo4jDriver", None),
    ("AsyncResult", None),
    ("AsyncSession", None),
    ("AsyncTransaction", None),
    ("Auth", None),
    ("AuthToken", None),
    ("basic_auth", None),
    ("bearer_auth", None),
    ("BoltDriver", None),
    ("Bookmark", None),
    ("Bookmarks", None),
    ("Config", DeprecationWarning),
    ("custom_auth", None),
    ("DEFAULT_DATABASE", None),
    ("Driver", None),
    ("EagerResult", None),
    ("ExperimentalWarning", None),
    ("get_user_agent", None),
    ("GqlStatusObject", PreviewWarning),
    ("GraphDatabase", None),
    ("IPv4Address", None),
    ("IPv6Address", None),
    ("kerberos_auth", None),
    ("log", DeprecationWarning),
    ("ManagedTransaction", None),
    ("Neo4jDriver", None),
    ("NotificationCategory", None),
    ("NotificationClassification", PreviewWarning),
    ("NotificationDisabledCategory", None),
    ("NotificationDisabledClassification", PreviewWarning),
    ("NotificationMinimumSeverity", None),
    ("NotificationSeverity", None),
    ("PoolConfig", DeprecationWarning),
    ("PreviewWarning", None),
    ("Query", None),
    ("READ_ACCESS", None),
    ("Record", None),
    ("Result", None),
    ("ResultSummary", None),
    ("RoutingControl", None),
    ("ServerInfo", None),
    ("Session", None),
    ("SessionConfig", DeprecationWarning),
    ("SummaryCounters", None),
    ("SummaryInputPosition", None),
    ("SummaryNotification", None),
    ("SummaryNotificationPosition", DeprecationWarning),
    ("Transaction", None),
    ("TRUST_ALL_CERTIFICATES", None),
    ("TRUST_SYSTEM_CA_SIGNED_CERTIFICATES", None),
    ("TrustAll", None),
    ("TrustCustomCAs", None),
    ("TrustSystemCAs", None),
    ("unit_of_work", None),
    ("Version", None),
    ("WorkspaceConfig", DeprecationWarning),
    ("WRITE_ACCESS", None),
)


@pytest.mark.parametrize(("name", "warning"), NEO4J_ATTRIBUTES)
def test_attribute_import(name, warning):
    neo4j = importlib.__import__("neo4j")
    if warning:
        with pytest.warns(warning):
            getattr(neo4j, name)
    else:
        getattr(neo4j, name)


@pytest.mark.parametrize(("name", "warning"), NEO4J_ATTRIBUTES)
def test_attribute_from_import(name, warning):
    if warning:
        with pytest.warns(warning):
            importlib.__import__("neo4j", fromlist=(name,))
    else:
        importlib.__import__("neo4j", fromlist=(name,))


def test_all():
    import neo4j

    assert sorted(neo4j.__all__) == sorted([i[0] for i in NEO4J_ATTRIBUTES])



def test_dir():
    import neo4j

    assert sorted(dir(neo4j)) == sorted([i[0] for i in NEO4J_ATTRIBUTES])


def test_import_star():
    with pytest.warns() as warnings:
        importlib.__import__("neo4j", fromlist=("*",))
    assert len(warnings) == 9
    assert all(issubclass(w.category, (DeprecationWarning, PreviewWarning))
               for w in warnings)

    for name in (
        "log", "Config", "PoolConfig", "SessionConfig", "WorkspaceConfig",
        "SummaryNotificationPosition",
    ):
        assert sum(
            bool(re.match(rf".*\b{name}\b.*", str(w.message)))
            for w in warnings
            if issubclass(w.category, DeprecationWarning)
        ) == 1

    for name in (
        "NotificationClassification", "GqlStatusObject",
        "NotificationDisabledClassification",
    ):
        assert sum(
            bool(re.match(rf".*\b{name}\b.*", str(w.message)))
            for w in warnings
            if issubclass(w.category, PreviewWarning)
        ) == 1


NEO4J_MODULES = (
    ("addressing", None),
    ("api", None),
    ("auth_management", None),
    ("conf", DeprecationWarning),
    ("data", DeprecationWarning),
    ("debug", None),
    ("exceptions", None),
    ("meta", DeprecationWarning),
    ("packstream", DeprecationWarning),
    ("routing", DeprecationWarning),
    ("warnings", None),
)


@pytest.mark.parametrize(("name", "warning"), NEO4J_MODULES)
def test_module_import(name, warning):
    if warning:
        with pytest.warns(warning):
            importlib.__import__(f"neo4j.{name}")
    else:
        importlib.__import__(f"neo4j.{name}")
