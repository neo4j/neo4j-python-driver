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


@copy_signature(AsyncAuthManagers.static)
def static_auth_manager(*args, **kwargs):
    with pytest.warns(PreviewWarning, match="Auth managers"):
        return AsyncAuthManagers.static(*args, **kwargs)

@copy_signature(AsyncAuthManagers.expiration_based)
def expiration_based_auth_manager(*args, **kwargs):
    with pytest.warns(PreviewWarning, match="Auth managers"):
        return AsyncAuthManagers.expiration_based(*args, **kwargs)


@copy_signature(ExpiringAuth)
def expiring_auth(*args, **kwargs):
    with pytest.warns(PreviewWarning, match="Auth managers"):
        return ExpiringAuth(*args, **kwargs)


@mark_async_test
@pytest.mark.parametrize("auth", SAMPLE_AUTHS)
async def test_static_manager(
    auth
) -> None:
    manager: AsyncAuthManager = static_auth_manager(auth)
    assert await manager.get_auth() is auth

    await manager.on_auth_expired(("something", "else"))
    assert await manager.get_auth() is auth

    await manager.on_auth_expired(auth)
    assert await manager.get_auth() is auth


@mark_async_test
@pytest.mark.parametrize(("auth1", "auth2"),
                         itertools.product(SAMPLE_AUTHS, repeat=2))
@pytest.mark.parametrize("expires_at", (None, .001, 1, 1000.))
async def test_expiration_based_manager_manual_expiry(
    auth1: t.Union[t.Tuple[str, str], Auth, None],
    auth2: t.Union[t.Tuple[str, str], Auth, None],
    expires_at: t.Optional[float],
    mocker
) -> None:
    with freeze_time("1970-01-01 00:00:00") as frozen_time:
        assert isinstance(frozen_time, FrozenDateTimeFactory)
        temporal_auth = expiring_auth(auth1, expires_at)
        provider = mocker.AsyncMock(return_value=temporal_auth)
        manager: AsyncAuthManager = expiration_based_auth_manager(provider)

        provider.assert_not_called()
        assert await manager.get_auth() is auth1
        provider.assert_awaited_once()
        provider.reset_mock()

        provider.return_value = expiring_auth(auth2)

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
@pytest.mark.parametrize("expires_at", (None, -1, 1., 1, 1000.))
async def test_expiration_based_manager_time_expiry(
    auth1: t.Union[t.Tuple[str, str], Auth, None],
    auth2: t.Union[t.Tuple[str, str], Auth, None],
    expires_at: t.Optional[float],
    mocker
) -> None:
    with freeze_time("1970-01-01 00:00:00") as frozen_time:
        assert isinstance(frozen_time, FrozenDateTimeFactory)
        if expires_at is None or expires_at >= 0:
            temporal_auth = expiring_auth(auth1, expires_at)
        else:
            temporal_auth = expiring_auth(auth1)
        provider = mocker.AsyncMock(return_value=temporal_auth)
        manager: AsyncAuthManager = expiration_based_auth_manager(provider)

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
