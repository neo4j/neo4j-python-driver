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


@copy_signature(AuthManagers.static)
def static_auth_manager(*args, **kwargs):
    with pytest.warns(PreviewWarning, match="Auth managers"):
        return AuthManagers.static(*args, **kwargs)

@copy_signature(AuthManagers.expiration_based)
def expiration_based_auth_manager(*args, **kwargs):
    with pytest.warns(PreviewWarning, match="Auth managers"):
        return AuthManagers.expiration_based(*args, **kwargs)


@copy_signature(ExpiringAuth)
def expiring_auth(*args, **kwargs):
    with pytest.warns(PreviewWarning, match="Auth managers"):
        return ExpiringAuth(*args, **kwargs)


@mark_sync_test
@pytest.mark.parametrize("auth", SAMPLE_AUTHS)
def test_static_manager(
    auth
) -> None:
    manager: AuthManager = static_auth_manager(auth)
    assert manager.get_auth() is auth

    manager.on_auth_expired(("something", "else"))
    assert manager.get_auth() is auth

    manager.on_auth_expired(auth)
    assert manager.get_auth() is auth


@mark_sync_test
@pytest.mark.parametrize(("auth1", "auth2"),
                         itertools.product(SAMPLE_AUTHS, repeat=2))
@pytest.mark.parametrize("expires_in", (None, -1, 1., 1, 1000.))
def test_expiration_based_manager_manual_expiry(
    auth1: t.Union[t.Tuple[str, str], Auth, None],
    auth2: t.Union[t.Tuple[str, str], Auth, None],
    expires_in: t.Union[float, int],
    mocker
) -> None:
    if expires_in is None or expires_in >= 0:
        temporal_auth = expiring_auth(auth1, expires_in)
    else:
        temporal_auth = expiring_auth(auth1)
    provider = mocker.MagicMock(return_value=temporal_auth)
    manager: AuthManager = expiration_based_auth_manager(provider)

    provider.assert_not_called()
    assert manager.get_auth() is auth1
    provider.assert_called_once()
    provider.reset_mock()

    provider.return_value = expiring_auth(auth2)

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
def test_expiration_based_manager_time_expiry(
    auth1: t.Union[t.Tuple[str, str], Auth, None],
    auth2: t.Union[t.Tuple[str, str], Auth, None],
    expires_in: t.Union[float, int, None],
    mocker
) -> None:
    with freeze_time() as frozen_time:
        assert isinstance(frozen_time, FrozenDateTimeFactory)
        if expires_in is None or expires_in >= 0:
            temporal_auth = expiring_auth(auth1, expires_in)
        else:
            temporal_auth = expiring_auth(auth1)
        provider = mocker.MagicMock(return_value=temporal_auth)
        manager: AuthManager = expiration_based_auth_manager(provider)

        provider.assert_not_called()
        assert manager.get_auth() is auth1
        provider.assert_called_once()
        provider.reset_mock()

        provider.return_value = expiring_auth(auth2)

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