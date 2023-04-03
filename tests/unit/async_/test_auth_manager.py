import itertools
import typing as t

import pytest
from freezegun import freeze_time
from freezegun.api import FrozenDateTimeFactory

from neo4j import (
    Auth,
    basic_auth,
)
from neo4j.auth_management import (
    AsyncAuthManager,
    AsyncAuthManagers,
    ExpiringAuth,
)

from ..._async_compat import mark_async_test


SAMPLE_AUTHS = (
    None,
    ("user", "password"),
    basic_auth("foo", "bar"),
    basic_auth("foo", "bar", "baz"),
    Auth("scheme", "principal", "credentials", "realm", para="meter"),
)


@mark_async_test
@pytest.mark.parametrize("auth", SAMPLE_AUTHS)
async def test_static_manager(
    auth
) -> None:
    manager: AsyncAuthManager = AsyncAuthManagers.static(auth)
    assert await manager.get_auth() is auth

    await manager.on_auth_expired(("something", "else"))
    assert await manager.get_auth() is auth

    await manager.on_auth_expired(auth)
    assert await manager.get_auth() is auth


@mark_async_test
@pytest.mark.parametrize(("auth1", "auth2"),
                         itertools.product(SAMPLE_AUTHS, repeat=2))
@pytest.mark.parametrize("expires_in", (None, -1, 1., 1, 1000.))
async def test_temporal_manager_manual_expiry(
    auth1: t.Union[t.Tuple[str, str], Auth, None],
    auth2: t.Union[t.Tuple[str, str], Auth, None],
    expires_in: t.Union[float, int],
    mocker
) -> None:
    if expires_in is None or expires_in >= 0:
        temporal_auth = ExpiringAuth(auth1, expires_in)
    else:
        temporal_auth = ExpiringAuth(auth1)
    provider = mocker.AsyncMock(return_value=temporal_auth)
    manager: AsyncAuthManager = AsyncAuthManagers.expiration_based(provider)

    provider.assert_not_called()
    assert await manager.get_auth() is auth1
    provider.assert_awaited_once()
    provider.reset_mock()

    provider.return_value = ExpiringAuth(auth2)

    await manager.on_auth_expired(("something", "else"))
    assert await manager.get_auth() is auth1
    provider.assert_not_called()

    await manager.on_auth_expired(auth1)
    provider.assert_awaited_once()
    provider.reset_mock()
    assert await manager.get_auth() is auth2
    provider.assert_not_called()


@mark_async_test
@pytest.mark.parametrize(("auth1", "auth2"),
                         itertools.product(SAMPLE_AUTHS, repeat=2))
@pytest.mark.parametrize("expires_in", (None, -1, 1., 1, 1000.))
async def test_temporal_manager_time_expiry(
    auth1: t.Union[t.Tuple[str, str], Auth, None],
    auth2: t.Union[t.Tuple[str, str], Auth, None],
    expires_in: t.Union[float, int, None],
    mocker
) -> None:
    with freeze_time() as frozen_time:
        assert isinstance(frozen_time, FrozenDateTimeFactory)
        if expires_in is None or expires_in >= 0:
            temporal_auth = ExpiringAuth(auth1, expires_in)
        else:
            temporal_auth = ExpiringAuth(auth1)
        provider = mocker.AsyncMock(return_value=temporal_auth)
        manager: AsyncAuthManager = AsyncAuthManagers.expiration_based(provider)

        provider.assert_not_called()
        assert await manager.get_auth() is auth1
        provider.assert_awaited_once()
        provider.reset_mock()

        provider.return_value = ExpiringAuth(auth2)

        if expires_in is None or expires_in < 0:
            frozen_time.tick(1_000_000)
            assert await manager.get_auth() is auth1
            provider.assert_not_called()
        else:
            frozen_time.tick(expires_in - 0.000001)
            assert await manager.get_auth() is auth1
            provider.assert_not_called()
            frozen_time.tick(0.000002)
            assert await manager.get_auth() is auth2
            provider.assert_awaited_once()
