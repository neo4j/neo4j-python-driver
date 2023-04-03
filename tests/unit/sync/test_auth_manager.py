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
    AuthManager,
    AuthManagers,
    ExpiringAuth,
)

from ..._async_compat import mark_sync_test


SAMPLE_AUTHS = (
    None,
    ("user", "password"),
    basic_auth("foo", "bar"),
    basic_auth("foo", "bar", "baz"),
    Auth("scheme", "principal", "credentials", "realm", para="meter"),
)


@mark_sync_test
@pytest.mark.parametrize("auth", SAMPLE_AUTHS)
def test_static_manager(
    auth
) -> None:
    manager: AuthManager = AuthManagers.static(auth)
    assert manager.get_auth() is auth

    manager.on_auth_expired(("something", "else"))
    assert manager.get_auth() is auth

    manager.on_auth_expired(auth)
    assert manager.get_auth() is auth


@mark_sync_test
@pytest.mark.parametrize(("auth1", "auth2"),
                         itertools.product(SAMPLE_AUTHS, repeat=2))
@pytest.mark.parametrize("expires_in", (None, -1, 1., 1, 1000.))
def test_temporal_manager_manual_expiry(
    auth1: t.Union[t.Tuple[str, str], Auth, None],
    auth2: t.Union[t.Tuple[str, str], Auth, None],
    expires_in: t.Union[float, int],
    mocker
) -> None:
    if expires_in is None or expires_in >= 0:
        temporal_auth = ExpiringAuth(auth1, expires_in)
    else:
        temporal_auth = ExpiringAuth(auth1)
    provider = mocker.MagicMock(return_value=temporal_auth)
    manager: AuthManager = AuthManagers.expiration_based(provider)

    provider.assert_not_called()
    assert manager.get_auth() is auth1
    provider.assert_called_once()
    provider.reset_mock()

    provider.return_value = ExpiringAuth(auth2)

    manager.on_auth_expired(("something", "else"))
    assert manager.get_auth() is auth1
    provider.assert_not_called()

    manager.on_auth_expired(auth1)
    provider.assert_called_once()
    provider.reset_mock()
    assert manager.get_auth() is auth2
    provider.assert_not_called()


@mark_sync_test
@pytest.mark.parametrize(("auth1", "auth2"),
                         itertools.product(SAMPLE_AUTHS, repeat=2))
@pytest.mark.parametrize("expires_in", (None, -1, 1., 1, 1000.))
def test_temporal_manager_time_expiry(
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
        provider = mocker.MagicMock(return_value=temporal_auth)
        manager: AuthManager = AuthManagers.expiration_based(provider)

        provider.assert_not_called()
        assert manager.get_auth() is auth1
        provider.assert_called_once()
        provider.reset_mock()

        provider.return_value = ExpiringAuth(auth2)

        if expires_in is None or expires_in < 0:
            frozen_time.tick(1_000_000)
            assert manager.get_auth() is auth1
            provider.assert_not_called()
        else:
            frozen_time.tick(expires_in - 0.000001)
            assert manager.get_auth() is auth1
            provider.assert_not_called()
            frozen_time.tick(0.000002)
            assert manager.get_auth() is auth2
            provider.assert_called_once()
