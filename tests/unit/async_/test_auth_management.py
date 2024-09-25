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


import itertools
import typing as t

import pytest
from freezegun import freeze_time
from freezegun.api import FrozenDateTimeFactory

from neo4j import (
    Auth,
    basic_auth,
    PreviewWarning,
)
from neo4j._meta import copy_signature
from neo4j.auth_management import (
    AsyncAuthManager,
    AsyncAuthManagers,
    AsyncClientCertificateProvider,
    AsyncClientCertificateProviders,
    AsyncRotatingClientCertificateProvider,
    ClientCertificate,
    ExpiringAuth,
)
from neo4j.exceptions import Neo4jError

from ..._async_compat import mark_async_test


T = t.TypeVar("T")

SAMPLE_AUTHS = (
    None,
    ("user", "password"),
    basic_auth("foo", "bar"),
    basic_auth("foo", "bar", "baz"),
    Auth("scheme", "principal", "credentials", "realm", para="meter"),
)

CODES_HANDLED_BY_BASIC_MANAGER = {
    "Neo.ClientError.Security.Unauthorized",
}
CODES_HANDLED_BY_BEARER_MANAGER = {
    "Neo.ClientError.Security.TokenExpired",
    "Neo.ClientError.Security.Unauthorized",
}
SAMPLE_ERRORS = [
    Neo4jError._hydrate_neo4j(code=code)
    for code in {
        "Neo.ClientError.Security.AuthenticationRateLimit",
        "Neo.ClientError.Security.AuthorizationExpired",
        "Neo.ClientError.Security.CredentialsExpired",
        "Neo.ClientError.Security.Forbidden",
        "Neo.ClientError.Security.TokenExpired",
        "Neo.ClientError.Security.Unauthorized",
        "Neo.ClientError.Security.MadeUp",
        "Neo.ClientError.Statement.SyntaxError",
        *CODES_HANDLED_BY_BASIC_MANAGER,
        *CODES_HANDLED_BY_BEARER_MANAGER,
    }
]


@copy_signature(AsyncAuthManagers.static)
def static_auth_manager(*args, **kwargs):
    return AsyncAuthManagers.static(*args, **kwargs)


@copy_signature(AsyncAuthManagers.basic)
def basic_auth_manager(*args, **kwargs):
    return AsyncAuthManagers.basic(*args, **kwargs)


@copy_signature(AsyncAuthManagers.bearer)
def bearer_auth_manager(*args, **kwargs):
    return AsyncAuthManagers.bearer(*args, **kwargs)


@copy_signature(ExpiringAuth)
def expiring_auth(*args, **kwargs):
    return ExpiringAuth(*args, **kwargs)


@mark_async_test
@pytest.mark.parametrize("auth", SAMPLE_AUTHS)
@pytest.mark.parametrize("error", SAMPLE_ERRORS)
async def test_static_manager(
    auth: t.Union[t.Tuple[str, str], Auth, None], error: Neo4jError
) -> None:
    manager: AsyncAuthManager = static_auth_manager(auth)
    assert await manager.get_auth() is auth

    handled = await manager.handle_security_exception(
        ("something", "else"), error
    )
    assert handled is False
    assert await manager.get_auth() is auth

    handled = await manager.handle_security_exception(auth, error)
    assert handled is False
    assert await manager.get_auth() is auth


@mark_async_test
@pytest.mark.parametrize(
    ("auth1", "auth2"), list(itertools.product(SAMPLE_AUTHS, repeat=2))
)
@pytest.mark.parametrize("error", SAMPLE_ERRORS)
async def test_basic_manager_manual_expiry(
    auth1: t.Union[t.Tuple[str, str], Auth, None],
    auth2: t.Union[t.Tuple[str, str], Auth, None],
    error: Neo4jError,
    mocker,
) -> None:
    def return_value_generator(auth):
        return auth

    await _test_manager(
        auth1,
        auth2,
        return_value_generator,
        basic_auth_manager,
        error,
        CODES_HANDLED_BY_BASIC_MANAGER,
        mocker,
    )


@mark_async_test
@pytest.mark.parametrize(
    ("auth1", "auth2"), itertools.product(SAMPLE_AUTHS, repeat=2)
)
@pytest.mark.parametrize("error", SAMPLE_ERRORS)
@pytest.mark.parametrize("expires_at", (None, 0.001, 1, 1000.0))
async def test_bearer_manager_manual_expiry(
    auth1: t.Union[t.Tuple[str, str], Auth, None],
    auth2: t.Union[t.Tuple[str, str], Auth, None],
    error: Neo4jError,
    expires_at: t.Optional[float],
    mocker,
) -> None:
    def return_value_generator(auth):
        return expiring_auth(auth)

    with freeze_time("1970-01-01 00:00:00") as frozen_time:
        assert isinstance(frozen_time, FrozenDateTimeFactory)
        await _test_manager(
            auth1,
            auth2,
            return_value_generator,
            bearer_auth_manager,
            error,
            CODES_HANDLED_BY_BEARER_MANAGER,
            mocker,
        )


@mark_async_test
@pytest.mark.parametrize(
    ("auth1", "auth2"), itertools.product(SAMPLE_AUTHS, repeat=2)
)
@pytest.mark.parametrize("expires_at", (None, -1, 1.0, 1, 1000.0))
async def test_bearer_manager_time_expiry(
    auth1: t.Union[t.Tuple[str, str], Auth, None],
    auth2: t.Union[t.Tuple[str, str], Auth, None],
    expires_at: t.Optional[float],
    mocker,
) -> None:
    with freeze_time("1970-01-01 00:00:00") as frozen_time:
        assert isinstance(frozen_time, FrozenDateTimeFactory)
        if expires_at is None or expires_at >= 0:
            temporal_auth = expiring_auth(auth1, expires_at)
        else:
            temporal_auth = expiring_auth(auth1)
        provider = mocker.AsyncMock(return_value=temporal_auth)
        manager: AsyncAuthManager = bearer_auth_manager(provider)

        provider.assert_not_called()
        assert await manager.get_auth() is auth1
        provider.assert_awaited_once()
        provider.reset_mock()

        provider.return_value = expiring_auth(auth2)

        if expires_at is None or expires_at < 0:
            frozen_time.tick(1_000_000)
            assert await manager.get_auth() is auth1
            provider.assert_not_called()
        else:
            frozen_time.tick(expires_at - 0.000001)
            assert await manager.get_auth() is auth1
            provider.assert_not_called()
            frozen_time.tick(0.000002)
            assert await manager.get_auth() is auth2
            provider.assert_awaited_once()


async def _test_manager(
    auth1: t.Union[t.Tuple[str, str], Auth, None],
    auth2: t.Union[t.Tuple[str, str], Auth, None],
    return_value_generator: t.Callable[
        [t.Union[t.Tuple[str, str], Auth, None]], T
    ],
    manager_factory: t.Callable[
        [t.Callable[[], t.Awaitable[T]]], AsyncAuthManager
    ],
    error: Neo4jError,
    handled_codes: t.Container[str],
    mocker: t.Any,
) -> None:
    provider = mocker.AsyncMock(return_value=return_value_generator(auth1))
    typed_provider = t.cast(t.Callable[[], t.Awaitable[T]], provider)
    manager: AsyncAuthManager = manager_factory(typed_provider)
    provider.assert_not_called()
    assert await manager.get_auth() is auth1
    provider.assert_awaited_once()
    provider.reset_mock()

    provider.return_value = return_value_generator(auth2)

    should_be_handled = error.code in handled_codes
    handled = await manager.handle_security_exception(
        ("something", "else"), error
    )
    assert handled is should_be_handled
    assert await manager.get_auth() is auth1
    provider.assert_not_called()

    handled = await manager.handle_security_exception(auth1, error)

    if should_be_handled:
        provider.assert_awaited_once()
    else:
        provider.assert_not_called()
    assert handled is should_be_handled
    provider.reset_mock()

    if should_be_handled:
        assert await manager.get_auth() is auth2
    else:
        assert await manager.get_auth() is auth1
    provider.assert_not_called()


@pytest.fixture
def client_cert_factory() -> t.Callable[[], ClientCertificate]:
    i = 0

    def factory() -> ClientCertificate:
        with pytest.warns(PreviewWarning, match="Mutual TLS"):
            return ClientCertificate(f"cert{i}")

    return factory


@copy_signature(AsyncClientCertificateProviders.static)
def static_cert_provider(*args, **kwargs):
    with pytest.warns(PreviewWarning, match="Mutual TLS"):
        return AsyncClientCertificateProviders.static(*args, **kwargs)


@copy_signature(AsyncClientCertificateProviders.rotating)
def rotating_cert_provider(*args, **kwargs):
    with pytest.warns(PreviewWarning, match="Mutual TLS"):
        return AsyncClientCertificateProviders.rotating(*args, **kwargs)


@mark_async_test
async def test_static_client_cert_provider(client_cert_factory) -> None:
    cert1: ClientCertificate = client_cert_factory()
    provider: AsyncClientCertificateProvider = static_cert_provider(cert1)

    assert await provider.get_certificate() is cert1
    for _ in range(10):
        assert await provider.get_certificate() is None


if t.TYPE_CHECKING:
    # Tests for type checker only. No need to run the test.

    async def test_rotating_client_cert_provider_type_factory(
        client_cert_factory,
    ) -> None:
        cert1: ClientCertificate = client_cert_factory()
        provider: AsyncRotatingClientCertificateProvider = (
            rotating_cert_provider(cert1)
        )
        _: AsyncClientCertificateProvider = provider


@mark_async_test
async def test_rotating_client_cert_provider(client_cert_factory) -> None:
    cert1: ClientCertificate = client_cert_factory()
    cert2: ClientCertificate = client_cert_factory()
    assert cert1 is not cert2  # sanity check
    provider: AsyncRotatingClientCertificateProvider = rotating_cert_provider(
        cert1
    )

    assert await provider.get_certificate() is cert1
    for _ in range(10):
        assert await provider.get_certificate() is None

    await provider.update_certificate(cert2)

    assert await provider.get_certificate() is cert2
    for _ in range(10):
        assert await provider.get_certificate() is None
