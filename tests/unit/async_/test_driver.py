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

import inspect
import ssl
import warnings

import pytest

import neo4j
from neo4j import (
    _typing as t,
    AsyncBoltDriver,
    AsyncGraphDatabase,
    AsyncNeo4jDriver,
    AsyncResult,
    NotificationDisabledCategory,
    NotificationMinimumSeverity,
    Query,
    TrustAll,
    TrustCustomCAs,
    TrustSystemCAs,
)
from neo4j._api import TelemetryAPI
from neo4j._async.auth_management import _AsyncStaticClientCertificateProvider
from neo4j._async.config import AsyncPoolConfig
from neo4j._async.driver import _work
from neo4j._async.io import (
    AsyncBoltPool,
    AsyncNeo4jPool,
)
from neo4j._async_compat.util import AsyncUtil
from neo4j._debug import ENABLED as DEBUG_ENABLED
from neo4j.api import (
    AsyncBookmarkManager,
    BookmarkManager,
    READ_ACCESS,
    WRITE_ACCESS,
)
from neo4j.auth_management import (
    AsyncClientCertificateProvider,
    ClientCertificate,
)
from neo4j.exceptions import (
    ConfigurationError,
    DriverError,
)
from neo4j.warnings import PreviewWarning

from ..._async_compat import (
    AsyncTestDecorators,
    mark_async_test,
)
from ..._preview_imports import NotificationDisabledClassification


@pytest.fixture
def session_cls_mock(mocker):
    session_cls_mock = mocker.patch(
        "neo4j._async.driver.AsyncSession", autospec=True
    )
    session_cls_mock.return_value.attach_mock(
        mocker.NonCallableMagicMock(), "_pipelined_begin"
    )
    return session_cls_mock


@pytest.fixture
def unit_of_work_mock(mocker):
    return mocker.patch("neo4j._async.driver.unit_of_work", autospec=True)


@pytest.mark.parametrize("protocol", ("bolt://", "bolt+s://", "bolt+ssc://"))
@pytest.mark.parametrize(
    "host", ("localhost", "127.0.0.1", "[::1]", "[0:0:0:0:0:0:0:1]")
)
@pytest.mark.parametrize("port", (":1234", "", ":7687"))
@pytest.mark.parametrize("params", ("", "?routing_context=test"))
@pytest.mark.parametrize("auth_token", (("test", "test"), None))
@mark_async_test
async def test_direct_driver_constructor(
    protocol, host, port, params, auth_token
):
    uri = protocol + host + port + params
    if params:
        with pytest.raises(ConfigurationError, match="Routing context"):
            AsyncGraphDatabase.driver(uri, auth=auth_token)
    else:
        driver = AsyncGraphDatabase.driver(uri, auth=auth_token)
        await driver.close()
        assert isinstance(driver, AsyncBoltDriver)


@pytest.mark.parametrize(
    "protocol", ("neo4j://", "neo4j+s://", "neo4j+ssc://")
)
@pytest.mark.parametrize(
    "host", ("localhost", "127.0.0.1", "[::1]", "[0:0:0:0:0:0:0:1]")
)
@pytest.mark.parametrize("port", (":1234", "", ":7687"))
@pytest.mark.parametrize("params", ("", "?routing_context=test"))
@pytest.mark.parametrize("auth_token", (("test", "test"), None))
@mark_async_test
async def test_routing_driver_constructor(
    protocol, host, port, params, auth_token
):
    uri = protocol + host + port + params
    driver = AsyncGraphDatabase.driver(uri, auth=auth_token)
    assert isinstance(driver, AsyncNeo4jDriver)
    await driver.close()


@pytest.mark.parametrize(
    "test_uri",
    (
        "bolt+ssc://127.0.0.1:9001",
        "bolt+s://127.0.0.1:9001",
        "bolt://127.0.0.1:9001",
        "neo4j+ssc://127.0.0.1:9001",
        "neo4j+s://127.0.0.1:9001",
        "neo4j://127.0.0.1:9001",
    ),
)
@pytest.mark.parametrize(
    ("test_config", "expected_failure", "expected_failure_message"),
    (
        ({"encrypted": False}, ConfigurationError, '"encrypted"'),
        ({"encrypted": True}, ConfigurationError, '"encrypted"'),
        (
            {"encrypted": True, "trusted_certificates": TrustAll()},
            ConfigurationError,
            '"encrypted"',
        ),
        (
            {"trusted_certificates": TrustAll()},
            ConfigurationError,
            '"trusted_certificates"',
        ),
        (
            {"trusted_certificates": TrustSystemCAs()},
            ConfigurationError,
            '"trusted_certificates"',
        ),
        (
            {"trusted_certificates": TrustCustomCAs("foo", "bar")},
            ConfigurationError,
            '"trusted_certificates"',
        ),
        ({"ssl_context": None}, ConfigurationError, '"ssl_context"'),
        (
            {"ssl_context": ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)},
            ConfigurationError,
            '"ssl_context"',
        ),
    ),
)
@mark_async_test
async def test_driver_config_error_uri_conflict(
    test_uri, test_config, expected_failure, expected_failure_message
):
    if "+" in test_uri:
        # `+s` and `+ssc` are shorthand syntax for not having to configure the
        # encryption behavior of the driver. Specifying both is invalid.
        with pytest.raises(expected_failure, match=expected_failure_message):
            AsyncGraphDatabase.driver(test_uri, **test_config)
    else:
        driver = AsyncGraphDatabase.driver(test_uri, **test_config)
        await driver.close()


@pytest.mark.parametrize(
    "test_uri",
    (
        "http://localhost:9001",
        "ftp://localhost:9001",
        "x://localhost:9001",
    ),
)
def test_invalid_protocol(test_uri):
    with pytest.raises(ConfigurationError, match="scheme"):
        AsyncGraphDatabase.driver(test_uri)


@pytest.mark.parametrize(
    ("test_config", "expected_failure", "expected_failure_message"),
    (
        (
            {"liveness_check_timeout": -1},
            ConfigurationError,
            '"liveness_check_timeout"',
        ),
    ),
)
def test_driver_liveness_timeout_config_error(
    test_config, expected_failure, expected_failure_message
):
    with pytest.raises(expected_failure, match=expected_failure_message):
        AsyncGraphDatabase.driver("bolt://127.0.0.1:9001", **test_config)


@pytest.mark.parametrize(
    "uri",
    (
        "bolt://127.0.0.1:9000",
        "neo4j://127.0.0.1:9000",
    ),
)
@mark_async_test
async def test_driver_opens_write_session_by_default(
    uri, async_fake_pool, mocker
):
    driver = AsyncGraphDatabase.driver(uri)
    # we set a specific db, because else the driver would try to fetch a RT
    # to get hold of the actual home database (which won't work in this
    # unittest)
    driver._pool = async_fake_pool
    async with driver.session(database="foobar") as session:
        mocker.patch(
            "neo4j._async.work.session.AsyncTransaction", autospec=True
        )
        tx = await session.begin_transaction()
    async_fake_pool.acquire.assert_awaited_once_with(
        access_mode=WRITE_ACCESS,
        timeout=mocker.ANY,
        database=mocker.ANY,
        bookmarks=mocker.ANY,
        auth=mocker.ANY,
        liveness_check_timeout=mocker.ANY,
        database_callback=mocker.ANY,
    )
    tx._begin.assert_awaited_once_with(
        mocker.ANY,
        mocker.ANY,
        mocker.ANY,
        WRITE_ACCESS,
        mocker.ANY,
        mocker.ANY,
        mocker.ANY,
        mocker.ANY,
        mocker.ANY,
    )

    await driver.close()


@pytest.mark.parametrize(
    "uri",
    (
        "bolt://127.0.0.1:9000",
        "neo4j://127.0.0.1:9000",
    ),
)
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


@pytest.mark.parametrize(
    "uri",
    (
        "bolt://127.0.0.1:9000",
        "neo4j://127.0.0.1:9000",
    ),
)
@pytest.mark.parametrize(
    "kwargs",
    (
        {"default_access_mode": WRITE_ACCESS},
        {"default_access_mode": READ_ACCESS},
        {"fetch_size": 69},
    ),
)
@mark_async_test
async def test_verify_connectivity_parameters_are_deprecated(
    uri, kwargs, mocker
):
    driver = AsyncGraphDatabase.driver(uri)
    mocker.patch.object(driver, "_pool", autospec=True)

    try:
        with pytest.warns(PreviewWarning, match="key-word arguments"):
            await driver.verify_connectivity(**kwargs)
    finally:
        await driver.close()


@pytest.mark.parametrize(
    "uri",
    (
        "bolt://127.0.0.1:9000",
        "neo4j://127.0.0.1:9000",
    ),
)
@pytest.mark.parametrize(
    "kwargs",
    (
        {"default_access_mode": WRITE_ACCESS},
        {"default_access_mode": READ_ACCESS},
        {"fetch_size": 69},
    ),
)
@mark_async_test
async def test_get_server_info_parameters_are_experimental(
    uri, kwargs, mocker
):
    driver = AsyncGraphDatabase.driver(uri)
    mocker.patch.object(driver, "_pool", autospec=True)

    try:
        with pytest.warns(PreviewWarning, match="key-word arguments"):
            await driver.get_server_info(**kwargs)
    finally:
        await driver.close()


@mark_async_test
async def test_with_builtin_bookmark_manager(session_cls_mock) -> None:
    bmm = AsyncGraphDatabase.bookmark_manager()
    # could be one line, but want to make sure the type checker assigns
    # bmm whatever type AsyncGraphDatabase.bookmark_manager() returns
    driver = AsyncGraphDatabase.driver("bolt://localhost")
    async with driver as driver:
        _ = driver.session(bookmark_manager=bmm)
        session_cls_mock.assert_called_once()
        assert session_cls_mock.call_args[0][1].bookmark_manager is bmm


@AsyncTestDecorators.mark_async_only_test
async def test_with_custom_inherited_async_bookmark_manager(
    session_cls_mock,
) -> None:
    class BMM(AsyncBookmarkManager):
        async def update_bookmarks(
            self,
            previous_bookmarks: t.Iterable[str],
            new_bookmarks: t.Iterable[str],
        ) -> None: ...

        async def get_bookmarks(self) -> t.Collection[str]:
            return []

        async def forget(self, databases: t.Iterable[str]) -> None: ...

    bmm = BMM()
    driver = AsyncGraphDatabase.driver("bolt://localhost")
    async with driver as driver:
        _ = driver.session(bookmark_manager=bmm)
        session_cls_mock.assert_called_once()
        assert session_cls_mock.call_args[0][1].bookmark_manager is bmm


@mark_async_test
async def test_with_custom_inherited_sync_bookmark_manager(
    session_cls_mock,
) -> None:
    class BMM(BookmarkManager):
        def update_bookmarks(
            self,
            previous_bookmarks: t.Iterable[str],
            new_bookmarks: t.Iterable[str],
        ) -> None: ...

        def get_bookmarks(self) -> t.Collection[str]:
            return []

        def forget(self, databases: t.Iterable[str]) -> None: ...

    bmm = BMM()
    driver = AsyncGraphDatabase.driver("bolt://localhost")
    async with driver as driver:
        _ = driver.session(bookmark_manager=bmm)
        session_cls_mock.assert_called_once()
        assert session_cls_mock.call_args[0][1].bookmark_manager is bmm


@AsyncTestDecorators.mark_async_only_test
async def test_with_custom_ducktype_async_bookmark_manager(
    session_cls_mock,
) -> None:
    class BMM:
        async def update_bookmarks(
            self,
            previous_bookmarks: t.Iterable[str],
            new_bookmarks: t.Iterable[str],
        ) -> None: ...

        async def get_bookmarks(self) -> t.Collection[str]:
            return []

        async def forget(self, databases: t.Iterable[str]) -> None: ...

    bmm = BMM()
    driver = AsyncGraphDatabase.driver("bolt://localhost")
    async with driver as driver:
        _ = driver.session(bookmark_manager=bmm)
        session_cls_mock.assert_called_once()
        assert session_cls_mock.call_args[0][1].bookmark_manager is bmm


@mark_async_test
async def test_with_custom_ducktype_sync_bookmark_manager(
    session_cls_mock,
) -> None:
    class BMM:
        def update_bookmarks(
            self,
            previous_bookmarks: t.Iterable[str],
            new_bookmarks: t.Iterable[str],
        ) -> None: ...

        def get_bookmarks(self) -> t.Collection[str]:
            return []

        def forget(self, databases: t.Iterable[str]) -> None: ...

    bmm = BMM()
    driver = AsyncGraphDatabase.driver("bolt://localhost")
    async with driver as driver:
        _ = driver.session(bookmark_manager=bmm)
        session_cls_mock.assert_called_once()
        assert session_cls_mock.call_args[0][1].bookmark_manager is bmm


@mark_async_test
async def test_with_static_client_certificate() -> None:
    cert = ClientCertificate("foo")
    async with AsyncGraphDatabase.driver(
        "bolt://localhost", client_certificate=cert
    ) as driver:
        passed_provider = driver._pool.pool_config.client_certificate
        assert isinstance(
            passed_provider, _AsyncStaticClientCertificateProvider
        )
        assert passed_provider._cert is cert


@mark_async_test
async def test_with_custom_inherited_client_certificate_provider(
    session_cls_mock,
) -> None:
    class Provider(AsyncClientCertificateProvider):
        async def get_certificate(self) -> ClientCertificate | None:
            return None

    provider = Provider()
    async with AsyncGraphDatabase.driver(
        "bolt://localhost", client_certificate=provider
    ) as driver:
        assert driver._pool.pool_config.client_certificate is provider


@mark_async_test
async def test_with_custom_ducktype_client_certificate_provider(
    session_cls_mock,
) -> None:
    class Provider:
        async def get_certificate(self) -> ClientCertificate | None:
            return None

    provider = Provider()
    async with AsyncGraphDatabase.driver(
        "bolt://localhost", client_certificate=provider
    ) as driver:
        assert driver._pool.pool_config.client_certificate is provider


if t.TYPE_CHECKING:
    _T_NotificationMinimumSeverity: t.TypeAlias = (
        NotificationMinimumSeverity
        | t.Literal[
            "OFF",
            "WARNING",
            "INFORMATION",
        ]
    )

    _T_NotificationDisabledCategory: t.TypeAlias = (
        NotificationDisabledCategory
        | t.Literal[
            "HINT",
            "UNRECOGNIZED",
            "UNSUPPORTED",
            "PERFORMANCE",
            "DEPRECATION",
            "GENERIC",
        ]
    )

    _T_NotificationDisabledClassification: t.TypeAlias = (
        NotificationDisabledClassification
        | t.Literal[
            "HINT",
            "UNRECOGNIZED",
            "UNSUPPORTED",
            "PERFORMANCE",
            "DEPRECATION",
            "GENERIC",
        ]
    )


if t.TYPE_CHECKING:

    class NotificationFilter(t.TypedDict):
        notifications_min_severity: t.NotRequired[
            _T_NotificationMinimumSeverity | None
        ]
        notifications_disabled_categories: t.NotRequired[
            t.Iterable[_T_NotificationDisabledCategory] | None
        ]
        notifications_disabled_classifications: t.NotRequired[
            t.Iterable[_T_NotificationDisabledClassification] | None
        ]


def get_notification_filter_expectation(
    min_sev: _T_NotificationMinimumSeverity | None,
    dis_cats: t.Iterable[_T_NotificationDisabledCategory] | None,
    dis_clss: t.Iterable[_T_NotificationDisabledClassification] | None,
) -> tuple[
    _T_NotificationMinimumSeverity | None,
    t.Iterable[_T_NotificationDisabledClassification] | None,
]:
    default_conf = AsyncPoolConfig()

    if min_sev is None:
        expected_min_sev = min_sev
    elif min_sev is not ...:
        expected_min_sev = getattr(min_sev, "value", min_sev)
    else:
        expected_min_sev = default_conf.notifications_min_severity

    if dis_clss is None:
        expected_dis_clss = dis_clss
    elif dis_clss is not ...:
        expected_dis_clss = [getattr(d, "value", d) for d in dis_clss]
    else:
        expected_dis_clss = default_conf.notifications_disabled_classifications

    if dis_cats is not ... and dis_cats is not None:
        expected_dis_cats = [getattr(d, "value", d) for d in dis_cats]
        if isinstance(expected_dis_clss, list):
            expected_dis_clss = list({*expected_dis_cats, *expected_dis_clss})
        else:
            expected_dis_clss = expected_dis_cats

    return expected_min_sev, expected_dis_clss


@pytest.mark.parametrize(
    "min_sev",
    (
        ...,
        None,
        "OFF",
        NotificationMinimumSeverity.OFF,
        "WARNING",
        NotificationMinimumSeverity.INFORMATION,
    ),
)
@pytest.mark.parametrize(
    "dis_cats",
    (
        ...,
        None,
        [],
        ["GENERIC"],
        [NotificationDisabledCategory.GENERIC],
        [
            NotificationDisabledCategory.GENERIC,
            NotificationDisabledCategory.HINT,
        ],
        (
            NotificationDisabledCategory.GENERIC,
            NotificationDisabledCategory.HINT,
        ),
        (NotificationDisabledCategory.GENERIC, "HINT"),
        {"GENERIC", "HINT"},
        # please no :/
        {"GENERIC": True, NotificationDisabledCategory.HINT: 0},
    ),
)
@pytest.mark.parametrize(
    "dis_clss",
    (
        ...,
        None,
        [],
        ["GENERIC"],
        [NotificationDisabledClassification.GENERIC],
        [
            NotificationDisabledClassification.GENERIC,
            NotificationDisabledClassification.HINT,
        ],
        (
            NotificationDisabledClassification.GENERIC,
            NotificationDisabledClassification.HINT,
        ),
        (NotificationDisabledClassification.GENERIC, "HINT"),
        {"GENERIC", "HINT"},
        # please no :/
        {"GENERIC": True, NotificationDisabledClassification.HINT: 0},
    ),
)
@pytest.mark.parametrize(
    "uri",
    [
        "bolt://localhost:7687",
        "neo4j://localhost:7687",
    ],
)
@mark_async_test
async def test_driver_factory_with_notification_filters(
    uri: str,
    mocker,
    min_sev: _T_NotificationMinimumSeverity | None,
    dis_cats: t.Iterable[_T_NotificationDisabledCategory] | None,
    dis_clss: t.Iterable[_T_NotificationDisabledClassification] | None,
) -> None:
    pool_cls = AsyncNeo4jPool if uri.startswith("neo4j://") else AsyncBoltPool
    open_mock = mocker.patch.object(
        pool_cls, "open", return_value=mocker.AsyncMock(spec=pool_cls)
    )
    open_mock.return_value.address = mocker.Mock()
    mocker.patch.object(AsyncBoltPool, "open", new=open_mock)

    filter_kwargs: NotificationFilter = {}
    if min_sev is not ...:
        filter_kwargs["notifications_min_severity"] = min_sev
    if dis_cats is not ...:
        filter_kwargs["notifications_disabled_categories"] = dis_cats
    if dis_clss is not ...:
        filter_kwargs["notifications_disabled_classifications"] = dis_clss

    if "notifications_disabled_classifications" in filter_kwargs:
        with pytest.warns(
            PreviewWarning, match="notifications_disabled_classifications"
        ):
            driver = AsyncGraphDatabase.driver(uri, auth=None, **filter_kwargs)
    else:
        driver = AsyncGraphDatabase.driver(uri, auth=None, **filter_kwargs)

    async with driver:
        expected_min_sev, expected_dis_clss = (
            get_notification_filter_expectation(min_sev, dis_cats, dis_clss)
        )

        open_mock.assert_called_once()
        open_pool_conf = open_mock.call_args.kwargs["pool_config"]
        assert open_pool_conf.notifications_min_severity == expected_min_sev
        actual_dis_clss = open_pool_conf.notifications_disabled_classifications
        if expected_dis_clss is None:
            assert actual_dis_clss is None
        else:
            assert sorted(actual_dis_clss) == sorted(expected_dis_clss)


@pytest.mark.parametrize(
    "uri",
    [
        "bolt://localhost:7687",
        "neo4j://localhost:7687",
    ],
)
@pytest.mark.parametrize(
    ("min_sev", "expected"),
    (
        (..., "INFORMATION" if DEBUG_ENABLED else None),
        (None, "INFORMATION" if DEBUG_ENABLED else None),
        ("OFF", None),
        (NotificationMinimumSeverity.OFF, None),
        ("INFORMATION", "INFORMATION"),
        (NotificationMinimumSeverity.INFORMATION, "INFORMATION"),
        ("WARNING", "WARNING"),
        (NotificationMinimumSeverity.WARNING, "WARNING"),
        ("FOO", ValueError),
        (NotificationDisabledCategory.GENERIC, ValueError),
    ),
)
@pytest.mark.parametrize(
    "min_sev_session",
    (
        ...,
        None,
        "OFF",
        NotificationMinimumSeverity.OFF,
        "INFORMATION",
        NotificationMinimumSeverity.INFORMATION,
        "WARNING",
        NotificationMinimumSeverity.WARNING,
        "FOO",
        NotificationDisabledCategory.GENERIC,
    ),
)
@mark_async_test
async def test_warn_notification_severity_driver_config(
    uri: str,
    session_cls_mock,
    min_sev: _T_NotificationMinimumSeverity | None,
    min_sev_session: _T_NotificationMinimumSeverity | None,
    expected: NotificationMinimumSeverity | type[Exception] | None,
) -> None:
    if inspect.isclass(expected) and issubclass(expected, Exception):
        assert min_sev is not ...  # makes no sense to test
        with (
            pytest.raises(expected),
            pytest.warns(PreviewWarning, match="notification warnings"),
        ):
            AsyncGraphDatabase.driver(uri, warn_notification_severity=min_sev)
        return
    if min_sev is ...:
        driver = AsyncGraphDatabase.driver(uri)
    else:
        with pytest.warns(PreviewWarning, match="notification warnings"):
            driver = AsyncGraphDatabase.driver(
                uri, warn_notification_severity=min_sev
            )
    async with driver:
        if min_sev_session is ...:
            session = driver.session()
        else:
            # Works at runtime (will be ignored), but should be rejected by
            # type checkers.
            session = driver.session(  # type: ignore[call-arg]
                warn_notification_severity=min_sev_session,
            )
        async with session:
            session_cls_mock.assert_called_once()
            (_, session_config), _ = session_cls_mock.call_args
            assert session_config.warn_notification_severity == expected


@pytest.mark.parametrize(
    "min_sev",
    (
        ...,
        None,
        "OFF",
        NotificationMinimumSeverity.OFF,
        "WARNING",
        NotificationMinimumSeverity.INFORMATION,
    ),
)
@pytest.mark.parametrize(
    "dis_cats",
    (
        ...,
        None,
        [],
        ["GENERIC"],
        [NotificationDisabledCategory.GENERIC],
        [
            NotificationDisabledCategory.GENERIC,
            NotificationDisabledCategory.HINT,
        ],
        (
            NotificationDisabledCategory.GENERIC,
            NotificationDisabledCategory.HINT,
        ),
        (NotificationDisabledCategory.GENERIC, "HINT"),
        {"GENERIC", "HINT"},
        # please no :/
        {"GENERIC": True, NotificationDisabledCategory.HINT: 0},
    ),
)
@pytest.mark.parametrize(
    "dis_clss",
    (
        ...,
        None,
        [],
        ["GENERIC"],
        [NotificationDisabledClassification.GENERIC],
        [
            NotificationDisabledClassification.GENERIC,
            NotificationDisabledClassification.HINT,
        ],
        (
            NotificationDisabledClassification.GENERIC,
            NotificationDisabledClassification.HINT,
        ),
        (NotificationDisabledClassification.GENERIC, "HINT"),
        {"GENERIC", "HINT"},
        # please no :/
        {"GENERIC": True, NotificationDisabledClassification.HINT: 0},
    ),
)
@pytest.mark.parametrize(
    "uri",
    [
        "bolt://localhost:7687",
        "neo4j://localhost:7687",
    ],
)
@mark_async_test
async def test_session_factory_with_notification_filter(
    uri: str,
    session_cls_mock,
    mocker,
    min_sev: _T_NotificationMinimumSeverity | None,
    dis_cats: t.Iterable[_T_NotificationDisabledCategory] | None,
    dis_clss: t.Iterable[_T_NotificationDisabledClassification] | None,
) -> None:
    pool_cls = AsyncNeo4jPool if uri.startswith("neo4j://") else AsyncBoltPool
    pool_mock: t.Any = mocker.AsyncMock(spec=pool_cls)
    mocker.patch.object(pool_cls, "open", return_value=pool_mock)
    pool_mock.address = mocker.Mock()

    filter_kwargs: NotificationFilter = {}
    if min_sev is not ...:
        filter_kwargs["notifications_min_severity"] = min_sev
    if dis_cats is not ...:
        filter_kwargs["notifications_disabled_categories"] = dis_cats
    if dis_clss is not ...:
        filter_kwargs["notifications_disabled_classifications"] = dis_clss

    async with AsyncGraphDatabase.driver(uri, auth=None) as driver:
        if "notifications_disabled_classifications" in filter_kwargs:
            with pytest.warns(
                PreviewWarning, match="notifications_disabled_classifications"
            ):
                session = driver.session(**filter_kwargs)
        else:
            session = driver.session(**filter_kwargs)
        async with session:
            session_cls_mock.assert_called_once()
            (_, session_config), _ = session_cls_mock.call_args

            expected_min_sev, expected_dis_clss = (
                get_notification_filter_expectation(
                    min_sev, dis_cats, dis_clss
                )
            )

            assert (
                session_config.notifications_min_severity == expected_min_sev
            )
            assert (
                session_config.notifications_disabled_classifications
                == expected_dis_clss
            )


class SomeClass:
    pass


@mark_async_test
async def test_execute_query_work(mocker) -> None:
    tx_mock = mocker.AsyncMock(spec=neo4j.AsyncManagedTransaction)
    transformer_mock = mocker.AsyncMock()
    transformer: t.Callable[[AsyncResult], t.Awaitable[SomeClass]] = (
        transformer_mock
    )
    query = "QUERY"
    parameters = {"para": "meters", "foo": object}

    res: SomeClass = await _work(tx_mock, query, parameters, transformer)

    tx_mock.run.assert_awaited_once_with(query, parameters)
    transformer_mock.assert_awaited_once_with(tx_mock.run.return_value)
    assert res is transformer_mock.return_value


@pytest.mark.parametrize(
    "query",
    (
        "foo",
        "bar",
        "RETURN 1 AS n",
        Query("RETURN 1 AS n"),
        Query("RETURN 1 AS n", metadata={"key": "value"}),
        Query("RETURN 1 AS n", timeout=1234),
        Query("RETURN 1 AS n", metadata={"key": "value"}, timeout=1234),
    ),
)
@pytest.mark.parametrize("positional", (True, False))
@mark_async_test
async def test_execute_query_query(
    query: t.LiteralString | Query,
    positional: bool,
    session_cls_mock,
    unit_of_work_mock,
    mocker,
) -> None:
    driver = AsyncGraphDatabase.driver("bolt://localhost")

    async with driver as driver:
        if positional:
            res = await driver.execute_query(query)
        else:
            res = await driver.execute_query(query_=query)

    session_cls_mock.assert_called_once()
    session_mock = session_cls_mock.return_value
    session_mock.__aenter__.assert_awaited_once()
    session_mock.__aexit__.assert_awaited_once()
    session_executor_mock = session_mock._run_transaction
    if isinstance(query, Query):
        unit_of_work_mock.assert_called_once_with(
            query.metadata, query.timeout
        )
        unit_of_work = unit_of_work_mock.return_value
        unit_of_work.assert_called_once_with(_work)
        session_executor_mock.assert_awaited_once_with(
            WRITE_ACCESS,
            TelemetryAPI.DRIVER,
            unit_of_work.return_value,
            (query.text, mocker.ANY, mocker.ANY),
            {},
        )
    else:
        unit_of_work_mock.assert_not_called()
        session_executor_mock.assert_awaited_once_with(
            WRITE_ACCESS,
            TelemetryAPI.DRIVER,
            _work,
            (query, mocker.ANY, mocker.ANY),
            {},
        )
    assert res is session_executor_mock.return_value


@pytest.mark.parametrize(
    "parameters", (..., None, {}, {"foo": 1}, {"foo": 1, "bar": object()})
)
@pytest.mark.parametrize("positional", (True, False))
@mark_async_test
async def test_execute_query_parameters(
    parameters: dict[str, t.Any] | None,
    positional: bool,
    session_cls_mock,
    mocker,
) -> None:
    driver = AsyncGraphDatabase.driver("bolt://localhost")

    async with driver as driver:
        if parameters is Ellipsis:
            parameters = None
            res = await driver.execute_query("")
        elif positional:
            res = await driver.execute_query("", parameters)
        else:
            res = await driver.execute_query("", parameters_=parameters)

    session_cls_mock.assert_called_once()
    session_mock = session_cls_mock.return_value
    session_mock.__aenter__.assert_awaited_once()
    session_mock.__aexit__.assert_awaited_once()
    session_executor_mock = session_mock._run_transaction
    session_executor_mock.assert_awaited_once_with(
        WRITE_ACCESS,
        TelemetryAPI.DRIVER,
        _work,
        (mocker.ANY, parameters or {}, mocker.ANY),
        {},
    )
    assert res is session_executor_mock.return_value


@pytest.mark.parametrize(
    "parameters",
    (
        None,
        {},
        {"foo": 1},
        {"foo": 1, "_bar": object()},
        {"__": 1},
        {"baz__": 2},
    ),
)
@mark_async_test
async def test_execute_query_keyword_parameters(
    parameters: dict[str, t.Any] | None, session_cls_mock, mocker
) -> None:
    driver = AsyncGraphDatabase.driver("bolt://localhost")

    async with driver as driver:
        if parameters is None:
            res = await driver.execute_query("")
        else:
            res = await driver.execute_query("", **parameters)

    session_cls_mock.assert_called_once()
    session_mock = session_cls_mock.return_value
    session_mock.__aenter__.assert_awaited_once()
    session_mock.__aexit__.assert_awaited_once()
    session_executor_mock = session_mock._run_transaction
    session_executor_mock.assert_awaited_once_with(
        WRITE_ACCESS,
        TelemetryAPI.DRIVER,
        _work,
        (mocker.ANY, parameters or {}, mocker.ANY),
        {},
    )
    assert res is session_executor_mock.return_value


@pytest.mark.parametrize(
    "parameters", ({"_": "a"}, {"foo_": None}, {"foo_": 1, "bar_": 2})
)
@mark_async_test
async def test_reserved_query_keyword_parameters(
    mocker,
    parameters: dict[str, t.Any],
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
    ),
)
@pytest.mark.parametrize("positional", (True, False))
@mark_async_test
async def test_execute_query_parameter_precedence(
    params: dict[str, t.Any] | None,
    kw_params: dict[str, t.Any],
    expected_params: dict[str, t.Any],
    positional: bool,
    session_cls_mock,
    mocker,
) -> None:
    driver = AsyncGraphDatabase.driver("bolt://localhost")

    async with driver as driver:
        if params is None:
            res = await driver.execute_query("", **kw_params)
        elif positional:
            res = await driver.execute_query("", params, **kw_params)
        else:
            res = await driver.execute_query(
                "", parameters_=params, **kw_params
            )

    session_cls_mock.assert_called_once()
    session_mock = session_cls_mock.return_value
    session_mock.__aenter__.assert_awaited_once()
    session_mock.__aexit__.assert_awaited_once()
    session_executor_mock = session_mock._run_transaction
    session_executor_mock.assert_awaited_once_with(
        WRITE_ACCESS,
        TelemetryAPI.DRIVER,
        _work,
        (mocker.ANY, expected_params, mocker.ANY),
        {},
    )
    assert res is session_executor_mock.return_value


@pytest.mark.parametrize(
    ("routing_mode", "mode"),
    (
        (None, WRITE_ACCESS),
        ("r", READ_ACCESS),
        ("w", WRITE_ACCESS),
        (neo4j.RoutingControl.READ, READ_ACCESS),
        (neo4j.RoutingControl.WRITE, WRITE_ACCESS),
    ),
)
@pytest.mark.parametrize("positional", (True, False))
@mark_async_test
async def test_execute_query_routing_control(
    mode: str,
    positional: bool,
    routing_mode: neo4j.RoutingControl | t.Literal["r", "w"] | None,
    session_cls_mock,
    mocker,
) -> None:
    driver = AsyncGraphDatabase.driver("bolt://localhost")
    async with driver as driver:
        if routing_mode is None:
            res = await driver.execute_query("")
        elif positional:
            res = await driver.execute_query("", None, routing_mode)
        else:
            res = await driver.execute_query("", routing_=routing_mode)

    session_cls_mock.assert_called_once()
    session_mock = session_cls_mock.return_value
    session_mock.__aenter__.assert_awaited_once()
    session_mock.__aexit__.assert_awaited_once()
    session_executor_mock = session_mock._run_transaction
    session_executor_mock.assert_awaited_once_with(
        mode,
        TelemetryAPI.DRIVER,
        _work,
        (mocker.ANY, mocker.ANY, mocker.ANY),
        {},
    )
    assert res is session_executor_mock.return_value


@pytest.mark.parametrize(
    "database", (..., None, "foo", "baz", "neo4j", "system")
)
@pytest.mark.parametrize("positional", (True, False))
@mark_async_test
async def test_execute_query_database(
    database: str | None, positional: bool, session_cls_mock
) -> None:
    driver = AsyncGraphDatabase.driver("bolt://localhost")
    async with driver as driver:
        if database is Ellipsis:
            database = None
            await driver.execute_query("")
        elif positional:
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
    impersonated_user: str | None, positional: bool, session_cls_mock
) -> None:
    driver = AsyncGraphDatabase.driver("bolt://localhost")
    async with driver as driver:
        if impersonated_user is Ellipsis:
            impersonated_user = None
            await driver.execute_query("")
        elif positional:
            await driver.execute_query("", None, "w", None, impersonated_user)
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
    positional: bool,
    bookmark_manager: AsyncBookmarkManager | BookmarkManager | None,
    session_cls_mock,
) -> None:
    driver = AsyncGraphDatabase.driver("bolt://localhost")
    async with driver as driver:
        if bookmark_manager is Ellipsis:
            bookmark_manager = driver.execute_query_bookmark_manager
            await driver.execute_query("")
        elif positional:
            await driver.execute_query(
                "", None, "w", None, None, bookmark_manager
            )
        else:
            await driver.execute_query("", bookmark_manager_=bookmark_manager)

    session_cls_mock.assert_called_once()
    session_config = session_cls_mock.call_args.args[1]
    assert session_config.bookmark_manager == bookmark_manager


@pytest.mark.parametrize("result_transformer", (..., object()))
@pytest.mark.parametrize("positional", (True, False))
@mark_async_test
async def test_execute_query_result_transformer(
    positional: bool,
    result_transformer: t.Callable[[AsyncResult], t.Awaitable[SomeClass]],
    session_cls_mock,
    mocker,
) -> None:
    driver = AsyncGraphDatabase.driver("bolt://localhost")
    res: t.Any
    async with driver as driver:
        expected_transformer: t.Any = result_transformer
        if result_transformer is Ellipsis:
            expected_transformer = AsyncResult.to_eager_result
            res_default: neo4j.EagerResult = await driver.execute_query("")
            res = res_default
        else:
            res_custom: SomeClass
            if positional:
                bmm = driver.execute_query_bookmark_manager
                res_custom = await driver.execute_query(
                    "", None, "w", None, None, bmm, None, result_transformer
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
    session_executor_mock = session_mock._run_transaction
    session_executor_mock.assert_awaited_once_with(
        WRITE_ACCESS,
        TelemetryAPI.DRIVER,
        _work,
        (mocker.ANY, mocker.ANY, expected_transformer),
        {},
    )
    assert res is session_executor_mock.return_value


@mark_async_test
async def test_supports_session_auth(session_cls_mock) -> None:
    driver = AsyncGraphDatabase.driver("bolt://localhost")
    async with driver as driver:
        res = await driver.supports_session_auth()

    session_cls_mock.assert_called_once()
    session_cls_mock.return_value.__aenter__.assert_awaited_once()
    session_mock = session_cls_mock.return_value.__aenter__.return_value
    connection_mock = session_mock._connection
    assert res is connection_mock.supports_re_auth


@pytest.mark.parametrize(
    ("method_name", "args", "kwargs"),
    (
        ("execute_query", ("",), {}),
        ("session", (), {}),
        ("verify_connectivity", (), {}),
        ("get_server_info", (), {}),
        ("supports_multi_db", (), {}),
        ("supports_session_auth", (), {}),
    ),
)
@mark_async_test
async def test_using_closed_driver_where_forbidden(
    method_name, args, kwargs, session_cls_mock
) -> None:
    driver = AsyncGraphDatabase.driver("bolt://localhost")
    await driver.close()

    method = getattr(driver, method_name)
    with pytest.raises(DriverError, match="closed"):
        await AsyncUtil.callback(method, *args, **kwargs)


@pytest.mark.parametrize(
    ("method_name", "args", "kwargs"), (("close", (), {}),)
)
@mark_async_test
async def test_using_closed_driver_where_no_op(
    method_name, args, kwargs, session_cls_mock
) -> None:
    driver = AsyncGraphDatabase.driver("bolt://localhost")
    await driver.close()

    method = getattr(driver, method_name)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        await AsyncUtil.callback(method, *args, **kwargs)
