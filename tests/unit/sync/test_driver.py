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


from __future__ import annotations

import ssl
import typing as t

import pytest
import typing_extensions as te

from neo4j import (
    BoltDriver,
    ExperimentalWarning,
    GraphDatabase,
    Neo4jDriver,
    NotificationFilter,
    TRUST_ALL_CERTIFICATES,
    TRUST_SYSTEM_CA_SIGNED_CERTIFICATES,
    TrustAll,
    TrustCustomCAs,
    TrustSystemCAs,
)
from neo4j._conf import PoolConfig
from neo4j._sync.io import (
    BoltPool,
    Neo4jPool,
)
from neo4j.api import (
    BookmarkManager,
    READ_ACCESS,
    WRITE_ACCESS,
)
from neo4j.exceptions import ConfigurationError

from ..._async_compat import (
    mark_sync_test,
    TestDecorators,
)


@pytest.mark.parametrize("protocol", ("bolt://", "bolt+s://", "bolt+ssc://"))
@pytest.mark.parametrize("host", ("localhost", "127.0.0.1",
                                  "[::1]", "[0:0:0:0:0:0:0:1]"))
@pytest.mark.parametrize("port", (":1234", "", ":7687"))
@pytest.mark.parametrize("params", ("", "?routing_context=test"))
@pytest.mark.parametrize("auth_token", (("test", "test"), None))
@mark_sync_test
def test_direct_driver_constructor(protocol, host, port, params, auth_token):
    uri = protocol + host + port + params
    if params:
        with pytest.warns(DeprecationWarning, match="routing context"):
            driver = GraphDatabase.driver(uri, auth=auth_token)
    else:
        driver = GraphDatabase.driver(uri, auth=auth_token)
    assert isinstance(driver, BoltDriver)
    driver.close()


@pytest.mark.parametrize("protocol",
                         ("neo4j://", "neo4j+s://", "neo4j+ssc://"))
@pytest.mark.parametrize("host", ("localhost", "127.0.0.1",
                                  "[::1]", "[0:0:0:0:0:0:0:1]"))
@pytest.mark.parametrize("port", (":1234", "", ":7687"))
@pytest.mark.parametrize("params", ("", "?routing_context=test"))
@pytest.mark.parametrize("auth_token", (("test", "test"), None))
@mark_sync_test
def test_routing_driver_constructor(protocol, host, port, params, auth_token):
    uri = protocol + host + port + params
    driver = GraphDatabase.driver(uri, auth=auth_token)
    assert isinstance(driver, Neo4jDriver)
    driver.close()


@pytest.mark.parametrize("test_uri", (
    "bolt+ssc://127.0.0.1:9001",
    "bolt+s://127.0.0.1:9001",
    "bolt://127.0.0.1:9001",
    "neo4j+ssc://127.0.0.1:9001",
    "neo4j+s://127.0.0.1:9001",
    "neo4j://127.0.0.1:9001",
))
@pytest.mark.parametrize(
    ("test_config", "expected_failure", "expected_failure_message"),
    (
        ({"encrypted": False}, ConfigurationError, "The config settings"),
        ({"encrypted": True}, ConfigurationError, "The config settings"),
        (
            {"encrypted": True, "trust": TRUST_ALL_CERTIFICATES},
            ConfigurationError, "The config settings"
        ),
        (
            {"trust": TRUST_ALL_CERTIFICATES},
            ConfigurationError, "The config settings"
        ),
        (
            {"trust": TRUST_SYSTEM_CA_SIGNED_CERTIFICATES},
            ConfigurationError, "The config settings"
        ),
        (
            {"encrypted": True, "trusted_certificates": TrustAll()},
            ConfigurationError, "The config settings"
        ),
        (
            {"trusted_certificates": TrustAll()},
            ConfigurationError, "The config settings"
        ),
        (
            {"trusted_certificates": TrustSystemCAs()},
            ConfigurationError, "The config settings"
        ),
        (
            {"trusted_certificates": TrustCustomCAs("foo", "bar")},
            ConfigurationError, "The config settings"
        ),
        (
            {"ssl_context": None},
            ConfigurationError, "The config settings"
        ),
        (
            {"ssl_context": ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)},
            ConfigurationError, "The config settings"
        ),
    )
)
@mark_sync_test
def test_driver_config_error(
    test_uri, test_config, expected_failure, expected_failure_message
):
    def driver_builder():
        if "trust" in test_config:
            with pytest.warns(DeprecationWarning, match="trust"):
                return GraphDatabase.driver(test_uri, **test_config)
        else:
            return GraphDatabase.driver(test_uri, **test_config)

    if "+" in test_uri:
        # `+s` and `+ssc` are short hand syntax for not having to configure the
        # encryption behavior of the driver. Specifying both is invalid.
        with pytest.raises(expected_failure, match=expected_failure_message):
            driver_builder()
    else:
        driver = driver_builder()
        driver.close()


@pytest.mark.parametrize("test_uri", (
    "http://localhost:9001",
    "ftp://localhost:9001",
    "x://localhost:9001",
))
def test_invalid_protocol(test_uri):
    with pytest.raises(ConfigurationError, match="scheme"):
        GraphDatabase.driver(test_uri)


@pytest.mark.parametrize(
    ("test_config", "expected_failure", "expected_failure_message"),
    (
        ({"trust": 1}, ConfigurationError, "The config setting `trust`"),
        ({"trust": True}, ConfigurationError, "The config setting `trust`"),
        ({"trust": None}, ConfigurationError, "The config setting `trust`"),
    )
)
def test_driver_trust_config_error(
    test_config, expected_failure, expected_failure_message
):
    with pytest.raises(expected_failure, match=expected_failure_message):
        GraphDatabase.driver("bolt://127.0.0.1:9001", **test_config)


@pytest.mark.parametrize("uri", (
    "bolt://127.0.0.1:9000",
    "neo4j://127.0.0.1:9000",
))
@mark_sync_test
def test_driver_opens_write_session_by_default(uri, fake_pool, mocker):
    driver = GraphDatabase.driver(uri)
    # we set a specific db, because else the driver would try to fetch a RT
    # to get hold of the actual home database (which won't work in this
    # unittest)
    driver._pool = fake_pool
    with driver.session(database="foobar") as session:
        mocker.patch("neo4j._sync.work.session.Transaction",
                     autospec=True)
        tx = session.begin_transaction()
    fake_pool.acquire.assert_called_once_with(
        access_mode=WRITE_ACCESS,
        timeout=mocker.ANY,
        database=mocker.ANY,
        bookmarks=mocker.ANY,
        liveness_check_timeout=mocker.ANY
    )
    tx._begin.assert_called_once_with(
        mocker.ANY,
        mocker.ANY,
        mocker.ANY,
        WRITE_ACCESS,
        mocker.ANY,
        mocker.ANY,
        mocker.ANY
    )

    driver.close()


@pytest.mark.parametrize("uri", (
    "bolt://127.0.0.1:9000",
    "neo4j://127.0.0.1:9000",
))
@mark_sync_test
def test_verify_connectivity(uri, mocker):
    driver = GraphDatabase.driver(uri)
    pool_mock = mocker.patch.object(driver, "_pool", autospec=True)

    try:
        ret = driver.verify_connectivity()
    finally:
        driver.close()

    assert ret is None
    pool_mock.acquire.assert_called_once()
    assert pool_mock.acquire.call_args.kwargs["liveness_check_timeout"] == 0
    pool_mock.release.assert_called_once()


@pytest.mark.parametrize("uri", (
    "bolt://127.0.0.1:9000",
    "neo4j://127.0.0.1:9000",
))
@pytest.mark.parametrize("kwargs", (
    {"default_access_mode": WRITE_ACCESS},
    {"default_access_mode": READ_ACCESS},
    {"fetch_size": 69},
))
@mark_sync_test
def test_verify_connectivity_parameters_are_deprecated(
    uri, kwargs, mocker
):
    driver = GraphDatabase.driver(uri)
    mocker.patch.object(driver, "_pool", autospec=True)

    try:
        with pytest.warns(ExperimentalWarning, match="configuration"):
            driver.verify_connectivity(**kwargs)
    finally:
        driver.close()


@pytest.mark.parametrize("uri", (
    "bolt://127.0.0.1:9000",
    "neo4j://127.0.0.1:9000",
))
@pytest.mark.parametrize("kwargs", (
    {"default_access_mode": WRITE_ACCESS},
    {"default_access_mode": READ_ACCESS},
    {"fetch_size": 69},
))
@mark_sync_test
def test_get_server_info_parameters_are_experimental(
    uri, kwargs, mocker
):
    driver = GraphDatabase.driver(uri)
    mocker.patch.object(driver, "_pool", autospec=True)

    try:
        with pytest.warns(ExperimentalWarning, match="configuration"):
            driver.get_server_info(**kwargs)
    finally:
        driver.close()


@mark_sync_test
def test_with_builtin_bookmark_manager(mocker) -> None:
    with pytest.warns(ExperimentalWarning, match="bookmark manager"):
        bmm = GraphDatabase.bookmark_manager()
    # could be one line, but want to make sure the type checker assigns
    # bmm whatever type AsyncGraphDatabase.bookmark_manager() returns
    session_cls_mock = mocker.patch("neo4j._sync.driver.Session",
                                    autospec=True)
    driver = GraphDatabase.driver("bolt://localhost")
    with driver as driver:
        with pytest.warns(ExperimentalWarning, match="bookmark_manager"):
            _ = driver.session(bookmark_manager=bmm)
        session_cls_mock.assert_called_once()
        assert session_cls_mock.call_args[0][1].bookmark_manager is bmm


@TestDecorators.mark_async_only_test
def test_with_custom_inherited_async_bookmark_manager(mocker) -> None:
    class BMM(BookmarkManager):
        def update_bookmarks(
            self, database: str, previous_bookmarks: t.Iterable[str],
            new_bookmarks: t.Iterable[str]
        ) -> None:
            ...

        def get_bookmarks(self, database: str) -> t.Collection[str]:
            ...

        def get_all_bookmarks(self) -> t.Collection[str]:
            ...

        def forget(self, databases: t.Iterable[str]) -> None:
            ...

    bmm = BMM()
    # could be one line, but want to make sure the type checker assigns
    # bmm whatever type AsyncGraphDatabase.bookmark_manager() returns
    session_cls_mock = mocker.patch("neo4j._sync.driver.Session",
                                    autospec=True)
    driver = GraphDatabase.driver("bolt://localhost")
    with driver as driver:
        with pytest.warns(ExperimentalWarning, match="bookmark_manager"):
            _ = driver.session(bookmark_manager=bmm)
        session_cls_mock.assert_called_once()
        assert session_cls_mock.call_args[0][1].bookmark_manager is bmm


@mark_sync_test
def test_with_custom_inherited_sync_bookmark_manager(mocker) -> None:
    class BMM(BookmarkManager):
        def update_bookmarks(
            self, database: str, previous_bookmarks: t.Iterable[str],
            new_bookmarks: t.Iterable[str]
        ) -> None:
            ...

        def get_bookmarks(self, database: str) -> t.Collection[str]:
            ...

        def get_all_bookmarks(self) -> t.Collection[str]:
            ...

        def forget(self, databases: t.Iterable[str]) -> None:
            ...

    bmm = BMM()
    # could be one line, but want to make sure the type checker assigns
    # bmm whatever type AsyncGraphDatabase.bookmark_manager() returns
    session_cls_mock = mocker.patch("neo4j._sync.driver.Session",
                                    autospec=True)
    driver = GraphDatabase.driver("bolt://localhost")
    with driver as driver:
        with pytest.warns(ExperimentalWarning, match="bookmark_manager"):
            _ = driver.session(bookmark_manager=bmm)
        session_cls_mock.assert_called_once()
        assert session_cls_mock.call_args[0][1].bookmark_manager is bmm


@TestDecorators.mark_async_only_test
def test_with_custom_ducktype_async_bookmark_manager(mocker) -> None:
    class BMM:
        def update_bookmarks(
            self, database: str, previous_bookmarks: t.Iterable[str],
            new_bookmarks: t.Iterable[str]
        ) -> None:
            ...

        def get_bookmarks(self, database: str) -> t.Collection[str]:
            ...

        def get_all_bookmarks(self) -> t.Collection[str]:
            ...

        def forget(self, databases: t.Iterable[str]) -> None:
            ...

    bmm = BMM()
    # could be one line, but want to make sure the type checker assigns
    # bmm whatever type AsyncGraphDatabase.bookmark_manager() returns
    session_cls_mock = mocker.patch("neo4j._sync.driver.Session",
                                    autospec=True)
    driver = GraphDatabase.driver("bolt://localhost")
    with driver as driver:
        with pytest.warns(ExperimentalWarning, match="bookmark_manager"):
            _ = driver.session(bookmark_manager=bmm)
        session_cls_mock.assert_called_once()
        assert session_cls_mock.call_args[0][1].bookmark_manager is bmm


@mark_sync_test
def test_with_custom_ducktype_sync_bookmark_manager(mocker) -> None:
    class BMM:
        def update_bookmarks(
            self, database: str, previous_bookmarks: t.Iterable[str],
            new_bookmarks: t.Iterable[str]
        ) -> None:
            ...

        def get_bookmarks(self, database: str) -> t.Collection[str]:
            ...

        def get_all_bookmarks(self) -> t.Collection[str]:
            ...

        def forget(self, databases: t.Iterable[str]) -> None:
            ...

    bmm = BMM()
    # could be one line, but want to make sure the type checker assigns
    # bmm whatever type AsyncGraphDatabase.bookmark_manager() returns
    session_cls_mock = mocker.patch("neo4j._sync.driver.Session",
                                    autospec=True)
    driver = GraphDatabase.driver("bolt://localhost")
    with driver as driver:
        with pytest.warns(ExperimentalWarning, match="bookmark_manager"):
            _ = driver.session(bookmark_manager=bmm)
        session_cls_mock.assert_called_once()
        assert session_cls_mock.call_args[0][1].bookmark_manager is bmm


_T_NotificationFilter = t.Union[
    NotificationFilter,
    te.Literal[
        "*.*",
        "WARNING.*",
        "WARNING.DEPRECATION",
        "WARNING.HINT",
        "WARNING.UNRECOGNIZED",
        "WARNING.UNSUPPORTED",
        "WARNING.GENERIC",
        "WARNING.PERFORMANCE",
        "INFORMATION.*",
        "INFORMATION.DEPRECATION",
        "INFORMATION.HINT",
        "INFORMATION.UNRECOGNIZED",
        "INFORMATION.UNSUPPORTED",
        "INFORMATION.GENERIC",
        "INFORMATION.PERFORMANCE",
        "*.DEPRECATION",
        "*.HINT",
        "*.UNRECOGNIZED",
        "*.UNSUPPORTED",
        "*.GENERIC",
        "*.PERFORMANCE",
    ]
]


@pytest.mark.parametrize("filters", (
    ...,
    None,
    NotificationFilter.none(),
    [],
    NotificationFilter.server_default(),
    "*.*",
    NotificationFilter.ALL_ALL,
    ["*.*", "WARNING.*"],
    [NotificationFilter.ALL_ALL, NotificationFilter.WARNING_ALL],
))
@pytest.mark.parametrize("uri", [
    "bolt://localhost:7687",
    "neo4j://localhost:7687",
])
@mark_sync_test
def test_driver_factory_with_notification_filters(
    uri: str,
    mocker,
    fake_pool,
    filters: t.Union[None, _T_NotificationFilter,
                     t.Iterable[_T_NotificationFilter]]
) -> None:
    pool_cls = Neo4jPool if uri.startswith("neo4j://") else BoltPool
    open_mock = mocker.patch.object(
        pool_cls, "open",
        return_value=mocker.MagicMock(spec=pool_cls)
    )
    if pool_cls is BoltPool:
        open_mock.return_value.address = mocker.Mock()
    mocker.patch.object(BoltPool, "open", new=open_mock)

    if filters is ...:
        driver = GraphDatabase.driver(uri, auth=None)
    else:
        driver = GraphDatabase.driver(uri, auth=None,
                                           notification_filters=filters)
    with driver:
        if filters is ...:
            expected_conf = PoolConfig()
        else:
            expected_conf = PoolConfig(notification_filters=filters)
        open_mock.assert_called_once()
        open_pool_conf = open_mock.call_args.kwargs["pool_config"]
        assert (open_pool_conf.notification_filters
                == expected_conf.notification_filters)


@pytest.mark.parametrize("filters", (
    ...,
    None,
    NotificationFilter.none(),
    [],
    NotificationFilter.server_default(),
    "*.*",
    NotificationFilter.ALL_ALL,
    ["*.*", "WARNING.*"],
    [NotificationFilter.ALL_ALL, NotificationFilter.WARNING_ALL],
))
@pytest.mark.parametrize("uri", [
    "bolt://localhost:7687",
    "neo4j://localhost:7687",
])
@mark_sync_test
def test_session_factory_with_notification_filter(
    uri: str, mocker, filters: t.Union[None, _T_NotificationFilter,
                                       t.Iterable[_T_NotificationFilter]]
) -> None:
    pool_cls = Neo4jPool if uri.startswith("neo4j://") else BoltPool
    pool_mock: t.Any = mocker.MagicMock(spec=pool_cls)
    mocker.patch.object(pool_cls, "open", return_value=pool_mock)
    if pool_cls is BoltPool:
        pool_mock.address = mocker.Mock()
    driver_filters = object()
    pool_mock.pool_config = PoolConfig(notification_filters=driver_filters)
    session_cls_mock = mocker.patch("neo4j._sync.driver.Session",
                                    autospec=True)

    with GraphDatabase.driver(uri, auth=None) as driver:
        if filters is ...:
            session = driver.session()
        else:
            session = driver.session(notification_filters=filters)

        with session:
            session_cls_mock.assert_called_once()
            (_, session_config), _ = session_cls_mock.call_args

            if filters is ...:
                assert session_config.notification_filters is driver_filters
            else:
                assert session_config.notification_filters == filters
