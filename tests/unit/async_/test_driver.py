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
from contextlib import contextmanager

import pytest

from neo4j import (
    AsyncBoltDriver,
    AsyncGraphDatabase,
    AsyncNeo4jDriver,
    ExperimentalWarning,
    TRUST_ALL_CERTIFICATES,
    TRUST_SYSTEM_CA_SIGNED_CERTIFICATES,
    TrustAll,
    TrustCustomCAs,
    TrustSystemCAs,
)
from neo4j._async_compat.util import AsyncUtil
from neo4j.api import (
    AsyncBookmarkManager,
    BookmarkManager,
    READ_ACCESS,
    WRITE_ACCESS,
)
from neo4j.exceptions import ConfigurationError

from ..._async_compat import (
    AsyncTestDecorators,
    mark_async_test,
)


@contextmanager
def expect_async_experimental_warning():
    if AsyncUtil.is_async_code:
        with pytest.warns(ExperimentalWarning, match="async"):
            yield
    else:
        yield


@pytest.mark.parametrize("protocol", ("bolt://", "bolt+s://", "bolt+ssc://"))
@pytest.mark.parametrize("host", ("localhost", "127.0.0.1",
                                  "[::1]", "[0:0:0:0:0:0:0:1]"))
@pytest.mark.parametrize("port", (":1234", "", ":7687"))
@pytest.mark.parametrize("params", ("", "?routing_context=test"))
@pytest.mark.parametrize("auth_token", (("test", "test"), None))
@mark_async_test
async def test_direct_driver_constructor(protocol, host, port, params, auth_token):
    uri = protocol + host + port + params
    if params:
        with pytest.warns(DeprecationWarning, match="routing context"):
            driver = AsyncGraphDatabase.driver(uri, auth=auth_token)
    else:
        with expect_async_experimental_warning():
            driver = AsyncGraphDatabase.driver(uri, auth=auth_token)
    assert isinstance(driver, AsyncBoltDriver)
    await driver.close()


@pytest.mark.parametrize("protocol",
                         ("neo4j://", "neo4j+s://", "neo4j+ssc://"))
@pytest.mark.parametrize("host", ("localhost", "127.0.0.1",
                                  "[::1]", "[0:0:0:0:0:0:0:1]"))
@pytest.mark.parametrize("port", (":1234", "", ":7687"))
@pytest.mark.parametrize("params", ("", "?routing_context=test"))
@pytest.mark.parametrize("auth_token", (("test", "test"), None))
@mark_async_test
async def test_routing_driver_constructor(protocol, host, port, params, auth_token):
    uri = protocol + host + port + params
    with expect_async_experimental_warning():
        driver = AsyncGraphDatabase.driver(uri, auth=auth_token)
    assert isinstance(driver, AsyncNeo4jDriver)
    await driver.close()


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
@mark_async_test
async def test_driver_config_error(
    test_uri, test_config, expected_failure, expected_failure_message
):
    def driver_builder():
        if "trust" in test_config:
            with pytest.warns(DeprecationWarning, match="trust"):
                return AsyncGraphDatabase.driver(test_uri, **test_config)
        else:
            with expect_async_experimental_warning():
                return AsyncGraphDatabase.driver(test_uri, **test_config)

    if "+" in test_uri:
        # `+s` and `+ssc` are short hand syntax for not having to configure the
        # encryption behavior of the driver. Specifying both is invalid.
        with pytest.raises(expected_failure, match=expected_failure_message):
            driver_builder()
    else:
        driver = driver_builder()
        await driver.close()


@pytest.mark.parametrize("test_uri", (
    "http://localhost:9001",
    "ftp://localhost:9001",
    "x://localhost:9001",
))
def test_invalid_protocol(test_uri):
    with pytest.raises(ConfigurationError, match="scheme"):
        with expect_async_experimental_warning():
            AsyncGraphDatabase.driver(test_uri)


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
        with expect_async_experimental_warning():
            AsyncGraphDatabase.driver("bolt://127.0.0.1:9001", **test_config)


@pytest.mark.parametrize("uri", (
    "bolt://127.0.0.1:9000",
    "neo4j://127.0.0.1:9000",
))
@mark_async_test
async def test_driver_opens_write_session_by_default(uri, fake_pool, mocker):
    with expect_async_experimental_warning():
        driver = AsyncGraphDatabase.driver(uri)
    # we set a specific db, because else the driver would try to fetch a RT
    # to get hold of the actual home database (which won't work in this
    # unittest)
    driver._pool = fake_pool
    async with driver.session(database="foobar") as session:
        mocker.patch("neo4j._async.work.session.AsyncTransaction",
                     autospec=True)
        tx = await session.begin_transaction()
    fake_pool.acquire.assert_awaited_once_with(
        access_mode=WRITE_ACCESS,
        timeout=mocker.ANY,
        database=mocker.ANY,
        bookmarks=mocker.ANY,
        liveness_check_timeout=mocker.ANY
    )
    tx._begin.assert_awaited_once_with(
        mocker.ANY,
        mocker.ANY,
        mocker.ANY,
        WRITE_ACCESS,
        mocker.ANY,
        mocker.ANY
    )

    await driver.close()


@pytest.mark.parametrize("uri", (
    "bolt://127.0.0.1:9000",
    "neo4j://127.0.0.1:9000",
))
@mark_async_test
async def test_verify_connectivity(uri, mocker):
    with expect_async_experimental_warning():
        driver = AsyncGraphDatabase.driver(uri)
    pool_mock = mocker.patch.object(driver, "_pool", autospec=True)

    try:
        ret = await driver.verify_connectivity()
    finally:
        await driver.close()

    assert ret is None
    pool_mock.acquire.assert_awaited_once()
    assert pool_mock.acquire.call_args.kwargs["liveness_check_timeout"] == 0
    pool_mock.release.assert_awaited_once()


@pytest.mark.parametrize("uri", (
    "bolt://127.0.0.1:9000",
    "neo4j://127.0.0.1:9000",
))
@pytest.mark.parametrize("kwargs", (
    {"default_access_mode": WRITE_ACCESS},
    {"default_access_mode": READ_ACCESS},
    {"fetch_size": 69},
))
@mark_async_test
async def test_verify_connectivity_parameters_are_deprecated(
    uri, kwargs, mocker
):
    with expect_async_experimental_warning():
        driver = AsyncGraphDatabase.driver(uri)
    mocker.patch.object(driver, "_pool", autospec=True)

    try:
        with pytest.warns(ExperimentalWarning, match="configuration"):
            await driver.verify_connectivity(**kwargs)
    finally:
        await driver.close()


@pytest.mark.parametrize("uri", (
    "bolt://127.0.0.1:9000",
    "neo4j://127.0.0.1:9000",
))
@pytest.mark.parametrize("kwargs", (
    {"default_access_mode": WRITE_ACCESS},
    {"default_access_mode": READ_ACCESS},
    {"fetch_size": 69},
))
@mark_async_test
async def test_get_server_info_parameters_are_experimental(
    uri, kwargs, mocker
):
    with expect_async_experimental_warning():
        driver = AsyncGraphDatabase.driver(uri)
    mocker.patch.object(driver, "_pool", autospec=True)

    try:
        with pytest.warns(ExperimentalWarning, match="configuration"):
            await driver.get_server_info(**kwargs)
    finally:
        await driver.close()


@mark_async_test
async def test_with_builtin_bookmark_manager(mocker) -> None:
    bmm = AsyncGraphDatabase.bookmark_manager()
    # could be one line, but want to make sure the type checker assigns
    # bmm whatever type AsyncGraphDatabase.bookmark_manager() returns
    session_cls_mock = mocker.patch("neo4j._async.driver.AsyncSession",
                                    autospec=True)
    with expect_async_experimental_warning():
        driver = AsyncGraphDatabase.driver(
            "bolt://localhost", bookmark_manager=bmm
        )
    async with driver as driver:
        _ = driver.session()
        session_cls_mock.assert_called_once()
        assert session_cls_mock.call_args[0][1].bookmark_manager is bmm


@AsyncTestDecorators.mark_async_only_test
async def test_with_custom_inherited_async_bookmark_manager(mocker) -> None:
    class BMM(AsyncBookmarkManager):
        async def update_bookmarks(
            self, database: str, previous_bookmarks: t.Iterable[str],
            new_bookmarks: t.Iterable[str]
        ) -> None:
            ...

        async def get_bookmarks(self, database: str) -> t.Collection[str]:
            ...

        async def get_all_bookmarks(self) -> t.Collection[str]:
            ...

        async def forget(self, databases: t.Iterable[str]) -> None:
            ...

    bmm = BMM()
    # could be one line, but want to make sure the type checker assigns
    # bmm whatever type AsyncGraphDatabase.bookmark_manager() returns
    session_cls_mock = mocker.patch("neo4j._async.driver.AsyncSession",
                                    autospec=True)
    with expect_async_experimental_warning():
        driver = AsyncGraphDatabase.driver(
            "bolt://localhost", bookmark_manager=bmm
        )
    async with driver as driver:
        _ = driver.session()
        session_cls_mock.assert_called_once()
        assert session_cls_mock.call_args[0][1].bookmark_manager is bmm


@mark_async_test
async def test_with_custom_inherited_sync_bookmark_manager(mocker) -> None:
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
    session_cls_mock = mocker.patch("neo4j._async.driver.AsyncSession",
                                    autospec=True)
    with expect_async_experimental_warning():
        driver = AsyncGraphDatabase.driver(
            "bolt://localhost", bookmark_manager=bmm
        )
    async with driver as driver:
        _ = driver.session()
        session_cls_mock.assert_called_once()
        assert session_cls_mock.call_args[0][1].bookmark_manager is bmm


@AsyncTestDecorators.mark_async_only_test
async def test_with_custom_ducktype_async_bookmark_manager(mocker) -> None:
    class BMM:
        async def update_bookmarks(
            self, database: str, previous_bookmarks: t.Iterable[str],
            new_bookmarks: t.Iterable[str]
        ) -> None:
            ...

        async def get_bookmarks(self, database: str) -> t.Collection[str]:
            ...

        async def get_all_bookmarks(self) -> t.Collection[str]:
            ...

        async def forget(self, databases: t.Iterable[str]) -> None:
            ...

    bmm = BMM()
    # could be one line, but want to make sure the type checker assigns
    # bmm whatever type AsyncGraphDatabase.bookmark_manager() returns
    session_cls_mock = mocker.patch("neo4j._async.driver.AsyncSession",
                                    autospec=True)
    with expect_async_experimental_warning():
        driver = AsyncGraphDatabase.driver(
            "bolt://localhost", bookmark_manager=bmm
        )
    async with driver as driver:
        _ = driver.session()
        session_cls_mock.assert_called_once()
        assert session_cls_mock.call_args[0][1].bookmark_manager is bmm


@mark_async_test
async def test_with_custom_ducktype_sync_bookmark_manager(mocker) -> None:
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
    session_cls_mock = mocker.patch("neo4j._async.driver.AsyncSession",
                                    autospec=True)
    with expect_async_experimental_warning():
        driver = AsyncGraphDatabase.driver(
            "bolt://localhost", bookmark_manager=bmm
        )
    async with driver as driver:
        _ = driver.session()
        session_cls_mock.assert_called_once()
        assert session_cls_mock.call_args[0][1].bookmark_manager is bmm
