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

import neo4j
from neo4j import (
    AsyncBoltDriver,
    AsyncGraphDatabase,
    AsyncNeo4jDriver,
    AsyncResult,
    ExperimentalWarning,
    TRUST_ALL_CERTIFICATES,
    TRUST_SYSTEM_CA_SIGNED_CERTIFICATES,
    TrustAll,
    TrustCustomCAs,
    TrustSystemCAs,
)
from neo4j._async.driver import _work
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
        AsyncGraphDatabase.driver("bolt://127.0.0.1:9001", **test_config)


@pytest.mark.parametrize("uri", (
    "bolt://127.0.0.1:9000",
    "neo4j://127.0.0.1:9000",
))
@mark_async_test
async def test_driver_opens_write_session_by_default(uri, fake_pool, mocker):
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
    driver = AsyncGraphDatabase.driver(uri)
    mocker.patch.object(driver, "_pool", autospec=True)

    try:
        with pytest.warns(ExperimentalWarning, match="configuration"):
            await driver.get_server_info(**kwargs)
    finally:
        await driver.close()


@mark_async_test
async def test_with_builtin_bookmark_manager(mocker) -> None:
    with pytest.warns(ExperimentalWarning, match="bookmark manager"):
        bmm = AsyncGraphDatabase.bookmark_manager()
    # could be one line, but want to make sure the type checker assigns
    # bmm whatever type AsyncGraphDatabase.bookmark_manager() returns
    session_cls_mock = mocker.patch("neo4j._async.driver.AsyncSession",
                                    autospec=True)
    driver = AsyncGraphDatabase.driver("bolt://localhost")
    async with driver as driver:
        with pytest.warns(ExperimentalWarning, match="bookmark_manager"):
            _ = driver.session(bookmark_manager=bmm)
        session_cls_mock.assert_called_once()
        assert session_cls_mock.call_args[0][1].bookmark_manager is bmm


@AsyncTestDecorators.mark_async_only_test
async def test_with_custom_inherited_async_bookmark_manager(mocker) -> None:
    class BMM(AsyncBookmarkManager):
        async def update_bookmarks(
            self, previous_bookmarks: t.Iterable[str],
            new_bookmarks: t.Iterable[str]
        ) -> None:
            ...

        async def get_bookmarks(self) -> t.Collection[str]:
            return []

        async def forget(self, databases: t.Iterable[str]) -> None:
            ...

    bmm = BMM()
    # could be one line, but want to make sure the type checker assigns
    # bmm whatever type AsyncGraphDatabase.bookmark_manager() returns
    session_cls_mock = mocker.patch("neo4j._async.driver.AsyncSession",
                                    autospec=True)
    driver = AsyncGraphDatabase.driver("bolt://localhost")
    async with driver as driver:
        with pytest.warns(ExperimentalWarning, match="bookmark_manager"):
            _ = driver.session(bookmark_manager=bmm)
        session_cls_mock.assert_called_once()
        assert session_cls_mock.call_args[0][1].bookmark_manager is bmm


@mark_async_test
async def test_with_custom_inherited_sync_bookmark_manager(mocker) -> None:
    class BMM(BookmarkManager):
        def update_bookmarks(
            self, previous_bookmarks: t.Iterable[str],
            new_bookmarks: t.Iterable[str]
        ) -> None:
            ...

        def get_bookmarks(self) -> t.Collection[str]:
            return []

        def forget(self, databases: t.Iterable[str]) -> None:
            ...

    bmm = BMM()
    # could be one line, but want to make sure the type checker assigns
    # bmm whatever type AsyncGraphDatabase.bookmark_manager() returns
    session_cls_mock = mocker.patch("neo4j._async.driver.AsyncSession",
                                    autospec=True)
    driver = AsyncGraphDatabase.driver("bolt://localhost")
    async with driver as driver:
        with pytest.warns(ExperimentalWarning, match="bookmark_manager"):
            _ = driver.session(bookmark_manager=bmm)
        session_cls_mock.assert_called_once()
        assert session_cls_mock.call_args[0][1].bookmark_manager is bmm


@AsyncTestDecorators.mark_async_only_test
async def test_with_custom_ducktype_async_bookmark_manager(mocker) -> None:
    class BMM:
        async def update_bookmarks(
            self, previous_bookmarks: t.Iterable[str],
            new_bookmarks: t.Iterable[str]
        ) -> None:
            ...

        async def get_bookmarks(self) -> t.Collection[str]:
            return []

        async def forget(self, databases: t.Iterable[str]) -> None:
            ...

    bmm = BMM()
    # could be one line, but want to make sure the type checker assigns
    # bmm whatever type AsyncGraphDatabase.bookmark_manager() returns
    session_cls_mock = mocker.patch("neo4j._async.driver.AsyncSession",
                                    autospec=True)
    driver = AsyncGraphDatabase.driver("bolt://localhost")
    async with driver as driver:
        with pytest.warns(ExperimentalWarning, match="bookmark_manager"):
            _ = driver.session(bookmark_manager=bmm)
        session_cls_mock.assert_called_once()
        assert session_cls_mock.call_args[0][1].bookmark_manager is bmm


@mark_async_test
async def test_with_custom_ducktype_sync_bookmark_manager(mocker) -> None:
    class BMM:
        def update_bookmarks(
            self, previous_bookmarks: t.Iterable[str],
            new_bookmarks: t.Iterable[str]
        ) -> None:
            ...

        def get_bookmarks(self) -> t.Collection[str]:
            return []

        def forget(self, databases: t.Iterable[str]) -> None:
            ...

    bmm = BMM()
    # could be one line, but want to make sure the type checker assigns
    # bmm whatever type AsyncGraphDatabase.bookmark_manager() returns
    session_cls_mock = mocker.patch("neo4j._async.driver.AsyncSession",
                                    autospec=True)
    driver = AsyncGraphDatabase.driver("bolt://localhost")
    async with driver as driver:
        with pytest.warns(ExperimentalWarning, match="bookmark_manager"):
            _ = driver.session(bookmark_manager=bmm)
        session_cls_mock.assert_called_once()
        assert session_cls_mock.call_args[0][1].bookmark_manager is bmm


class SomeClass:
    pass


@mark_async_test
async def test_execute_query_work(mocker):
    tx_mock = mocker.AsyncMock(spec=neo4j.AsyncManagedTransaction)
    transformer_mock = mocker.AsyncMock()
    transformer: t.Callable[[AsyncResult], t.Awaitable[SomeClass]] = \
        transformer_mock
    query = "QUERY"
    parameters = {"para": "meters", "foo": object}

    res: SomeClass = await _work(tx_mock, query, parameters, transformer)

    tx_mock.run.assert_awaited_once_with(query, parameters)
    transformer_mock.assert_awaited_once_with(tx_mock.run.return_value)
    assert res is transformer_mock.return_value


@pytest.mark.parametrize("query", ("foo", "bar", "RETURN 1 AS n"))
@pytest.mark.parametrize("positional", (True, False))
@mark_async_test
async def test_execute_query_query(
    mocker, query: str, positional: bool
) -> None:
    driver = AsyncGraphDatabase.driver("bolt://localhost")
    session_cls_mock = mocker.patch("neo4j._async.driver.AsyncSession",
                                    autospec=True)
    async with driver as driver:
        if positional:
            res = await driver.execute_query(query)
        else:
            res = await driver.execute_query(query_=query)

    session_cls_mock.assert_called_once()
    session_mock = session_cls_mock.return_value
    session_mock.__aenter__.assert_awaited_once()
    session_mock.__aexit__.assert_awaited_once()
    session_executor_mock = session_mock.execute_write
    session_executor_mock.assert_awaited_once_with(
        _work, query, mocker.ANY, mocker.ANY
    )
    assert res is session_executor_mock.return_value


@pytest.mark.parametrize("parameters", (
    ..., None, {}, {"foo": 1}, {"foo": 1, "bar": object()}
))
@pytest.mark.parametrize("positional", (True, False))
@mark_async_test
async def test_execute_query_parameters(
    mocker, parameters: t.Optional[t.Dict[str, t.Any]],
    positional: bool
) -> None:
    driver = AsyncGraphDatabase.driver("bolt://localhost")
    session_cls_mock = mocker.patch("neo4j._async.driver.AsyncSession",
                                    autospec=True)
    async with driver as driver:
        if parameters is Ellipsis:
            parameters = None
            res = await driver.execute_query("")
        else:
            if positional:
                res = await driver.execute_query("", parameters)
            else:
                res = await driver.execute_query("", parameters_=parameters)

    session_cls_mock.assert_called_once()
    session_mock = session_cls_mock.return_value
    session_mock.__aenter__.assert_awaited_once()
    session_mock.__aexit__.assert_awaited_once()
    session_executor_mock = session_mock.execute_write
    session_executor_mock.assert_awaited_once_with(
        _work, mocker.ANY, parameters or {}, mocker.ANY
    )
    assert res is session_executor_mock.return_value


@pytest.mark.parametrize("parameters", (
    None, {}, {"foo": 1}, {"foo": 1, "_bar": object()}, {"__": 1}, {"baz__": 2}
))
@mark_async_test
async def test_execute_query_keyword_parameters(
    mocker, parameters: t.Optional[t.Dict[str, t.Any]],
) -> None:
    driver = AsyncGraphDatabase.driver("bolt://localhost")
    session_cls_mock = mocker.patch("neo4j._async.driver.AsyncSession",
                                    autospec=True)
    async with driver as driver:
        if parameters is None:
            res = await driver.execute_query("")
        else:
            res = await driver.execute_query("", **parameters)

    session_cls_mock.assert_called_once()
    session_mock = session_cls_mock.return_value
    session_mock.__aenter__.assert_awaited_once()
    session_mock.__aexit__.assert_awaited_once()
    session_executor_mock = session_mock.execute_write
    session_executor_mock.assert_awaited_once_with(
        _work, mocker.ANY, parameters or {}, mocker.ANY
    )
    assert res is session_executor_mock.return_value


@pytest.mark.parametrize("parameters", (
    {"_": "a"}, {"foo_": None}, {"foo_": 1, "bar_": 2}
))
async def test_reserved_query_keyword_parameters(
    mocker, parameters: t.Dict[str, t.Any],
) -> None:
    driver = AsyncGraphDatabase.driver("bolt://localhost")
    mocker.patch("neo4j._async.driver.AsyncSession", autospec=True)
    async with driver as driver:
        with pytest.raises(ValueError) as exc:
            await driver.execute_query("", **parameters)
        exc.match("reserved")
        exc.match(", ".join(f"'{k}'" for k in parameters))


@pytest.mark.parametrize(
    ("params", "kw_params", "expected_params"),
    (
        ({"x": 1}, {}, {"x": 1}),
        ({}, {"x": 1}, {"x": 1}),
        (None, {"x": 1}, {"x": 1}),
        ({"x": 1}, {"y": 2}, {"x": 1, "y": 2}),
        ({"x": 1}, {"x": 2}, {"x": 2}),
        ({"x": 1}, {"x": 2}, {"x": 2}),
        ({"x": 1, "y": 3}, {"x": 2}, {"x": 2, "y": 3}),
        ({"x": 1}, {"x": 2, "y": 3}, {"x": 2, "y": 3}),
        # potentially internally used keyword arguments
        ({}, {"timeout": 2}, {"timeout": 2}),
        ({"timeout": 2}, {}, {"timeout": 2}),
        ({}, {"imp_user": "hans"}, {"imp_user": "hans"}),
        ({"imp_user": "hans"}, {}, {"imp_user": "hans"}),
        ({}, {"db": "neo4j"}, {"db": "neo4j"}),
        ({"db": "neo4j"}, {}, {"db": "neo4j"}),
        ({"_": "foobar"}, {}, {"_": "foobar"}),
        ({"__": "foobar"}, {}, {"__": "foobar"}),
        ({"x_": "foobar"}, {}, {"x_": "foobar"}),
        ({"x__": "foobar"}, {}, {"x__": "foobar"}),
        ({}, {"database": "neo4j"}, {"database": "neo4j"}),
        ({"database": "neo4j"}, {}, {"database": "neo4j"}),
        # already taken keyword arguments
        ({}, {"database_": "neo4j"}, {}),
        ({"database_": "neo4j"}, {}, {"database_": "neo4j"}),
    )
)
@pytest.mark.parametrize("positional", (True, False))
@mark_async_test
async def test_execute_query_parameter_precedence(
    params: t.Optional[t.Dict[str, t.Any]],
    kw_params: t.Dict[str, t.Any],
    expected_params: t.Dict[str, t.Any],
    positional: bool,
    mocker
) -> None:
    driver = AsyncGraphDatabase.driver("bolt://localhost")
    session_cls_mock = mocker.patch("neo4j._async.driver.AsyncSession",
                                    autospec=True)
    async with driver as driver:
        if params is None:
            res = await driver.execute_query("", **kw_params)
        else:
            if positional:
                res = await driver.execute_query("", params, **kw_params)
            else:
                res = await driver.execute_query("", parameters_=params,
                                                 **kw_params)

    session_cls_mock.assert_called_once()
    session_mock = session_cls_mock.return_value
    session_mock.__aenter__.assert_awaited_once()
    session_mock.__aexit__.assert_awaited_once()
    session_executor_mock = session_mock.execute_write
    session_executor_mock.assert_awaited_once_with(
        _work, mocker.ANY, expected_params, mocker.ANY
    )
    assert res is session_executor_mock.return_value


@pytest.mark.parametrize(
    ("routing_mode", "session_executor"),
    (
        (None, "execute_write"),
        ("r", "execute_read"),
        ("w", "execute_write"),
        (neo4j.RoutingControl.READERS, "execute_read"),
        (neo4j.RoutingControl.WRITERS, "execute_write"),
    )
)
@pytest.mark.parametrize("positional", (True, False))
@mark_async_test
async def test_execute_query_routing_control(
    mocker, session_executor: str, positional: bool,
    routing_mode: t.Union[neo4j.RoutingControl, te.Literal["r", "w"], None]
) -> None:
    driver = AsyncGraphDatabase.driver("bolt://localhost")
    session_cls_mock = mocker.patch("neo4j._async.driver.AsyncSession",
                                    autospec=True)
    async with driver as driver:
        if routing_mode is None:
            res = await driver.execute_query("")
        else:
            if positional:
                res = await driver.execute_query("", None, routing_mode)
            else:
                res = await driver.execute_query("", routing_=routing_mode)

    session_cls_mock.assert_called_once()
    session_mock = session_cls_mock.return_value
    session_mock.__aenter__.assert_awaited_once()
    session_mock.__aexit__.assert_awaited_once()
    session_executor_mock = getattr(session_mock, session_executor)
    session_executor_mock.assert_awaited_once_with(
        _work, mocker.ANY, mocker.ANY, mocker.ANY
    )
    assert res is session_executor_mock.return_value


@pytest.mark.parametrize("database", (
    ..., None, "foo", "baz", "neo4j", "system"
))
@pytest.mark.parametrize("positional", (True, False))
@mark_async_test
async def test_execute_query_database(
    mocker, database: t.Optional[str], positional: bool
) -> None:
    driver = AsyncGraphDatabase.driver("bolt://localhost")
    session_cls_mock = mocker.patch("neo4j._async.driver.AsyncSession",
                                    autospec=True)
    async with driver as driver:
        if database is Ellipsis:
            database = None
            await driver.execute_query("")
        else:
            if positional:
                await driver.execute_query("", None, "w", database)
            else:
                await driver.execute_query("", database_=database)

    session_cls_mock.assert_called_once()
    session_config = session_cls_mock.call_args.args[1]
    assert session_config.database == database


@pytest.mark.parametrize("impersonated_user", (..., None, "foo", "baz"))
@pytest.mark.parametrize("positional", (True, False))
@mark_async_test
async def test_execute_query_impersonated_user(
    mocker, impersonated_user: t.Optional[str], positional: bool
) -> None:
    driver = AsyncGraphDatabase.driver("bolt://localhost")
    session_cls_mock = mocker.patch("neo4j._async.driver.AsyncSession",
                                    autospec=True)
    async with driver as driver:
        if impersonated_user is Ellipsis:
            impersonated_user = None
            await driver.execute_query("")
        else:
            if positional:
                await driver.execute_query(
                    "", None, "w", None, impersonated_user
                )
            else:
                await driver.execute_query(
                    "", impersonated_user_=impersonated_user
                )

    session_cls_mock.assert_called_once()
    session_config = session_cls_mock.call_args.args[1]
    assert session_config.impersonated_user == impersonated_user


@pytest.mark.parametrize("bookmark_manager", (..., None, object()))
@pytest.mark.parametrize("positional", (True, False))
@mark_async_test
async def test_execute_query_bookmark_manager(
    mocker, positional: bool,
    bookmark_manager: t.Union[AsyncBookmarkManager, BookmarkManager, None]
) -> None:
    driver = AsyncGraphDatabase.driver("bolt://localhost")
    session_cls_mock = mocker.patch("neo4j._async.driver.AsyncSession",
                                    autospec=True)
    async with driver as driver:
        if bookmark_manager is Ellipsis:
            bookmark_manager = driver.query_bookmark_manager
            await driver.execute_query("")
        else:
            if positional:
                await driver.execute_query(
                    "", None, "w", None, None, bookmark_manager
                )
            else:
                await driver.execute_query(
                    "", bookmark_manager_=bookmark_manager
                )

    session_cls_mock.assert_called_once()
    session_config = session_cls_mock.call_args.args[1]
    assert session_config.bookmark_manager == bookmark_manager


@pytest.mark.parametrize("result_transformer", (..., object()))
@pytest.mark.parametrize("positional", (True, False))
@mark_async_test
async def test_execute_query_result_transformer(
    mocker, positional: bool,
    result_transformer: t.Callable[[AsyncResult], t.Awaitable[SomeClass]]
) -> None:
    driver = AsyncGraphDatabase.driver("bolt://localhost")
    session_cls_mock = mocker.patch("neo4j._async.driver.AsyncSession",
                                    autospec=True)
    res: t.Any
    async with driver as driver:
        if result_transformer is Ellipsis:
            result_transformer = AsyncResult.to_eager_result
            res_default: neo4j.EagerResult = await driver.execute_query("")
            res = res_default
        else:
            res_custom: SomeClass
            if positional:
                res_custom = await driver.execute_query(
                    "", None, "w", None, None, driver.query_bookmark_manager,
                    result_transformer
                )
            else:
                res_custom = await driver.execute_query(
                    "", result_transformer_=result_transformer
                )
            res = res_custom

    session_cls_mock.assert_called_once()
    session_mock = session_cls_mock.return_value
    session_mock.__aenter__.assert_awaited_once()
    session_mock.__aexit__.assert_awaited_once()
    session_executor_mock = session_mock.execute_write
    session_executor_mock.assert_awaited_once_with(
        _work, mocker.ANY, mocker.ANY, result_transformer
    )
    assert res is session_executor_mock.return_value
