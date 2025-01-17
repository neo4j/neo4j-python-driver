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

import math
import time
import typing as t
from datetime import (
    datetime,
    timedelta,
)

import freezegun
import pytest
import pytz

from neo4j._async.config import AsyncPoolConfig
from neo4j._async.home_db_cache import AsyncHomeDbCache
from neo4j._async.io._pool import AsyncNeo4jPool
from neo4j._conf import WorkspaceConfig
from neo4j.time import DateTime


if t.TYPE_CHECKING:
    from neo4j._async.home_db_cache import TKey


@pytest.mark.parametrize("enabled", (True, False))
def test_key_none_is_none(enabled: bool) -> None:
    assert AsyncHomeDbCache(enabled=enabled).compute_key(None, None) == (None,)


@pytest.mark.parametrize(
    "auth",
    (
        None,
        {},
        {"scheme": "basic", "principal": "neo4j", "credentials": "password"},
        {"scheme": "custom", "principal": "neo4j", "credentials": "password"},
        {"scheme": "custom", "credentials": "nice token"},
        {"foo": "bar"},
    ),
)
@pytest.mark.parametrize("enabled", (True, False))
def test_key_imp_precedence_over_auth(
    auth: dict | None,
    enabled: bool,
) -> None:
    cache = AsyncHomeDbCache(enabled=enabled)
    assert cache.compute_key("bob", auth) == ("bob" if enabled else (None,))


@pytest.mark.parametrize(
    "auth",
    (
        {},
        {"scheme": "basic", "principal": "neo4j", "credentials": "password"},
        {"scheme": "basic", "principal": "this is wrong, no password?"},
        {"scheme": "basic", "credentials": "this is wrong, no user?"},
        {"scheme": "none"},
        {"scheme": "none", "principal": "even though the scheme is none"},
        {"scheme": "kerberos", "principal": "", "credentials": "ticket"},
        {"scheme": "bearer", "credentials": "nice SSO token"},
        {"scheme": "custom", "principal": "neo4j", "credentials": "password"},
        {"scheme": "custom", "credentials": "bar", "parameters": {"oh": "hi"}},
        {"foo": "bar"},
    ),
)
def test_key_reduces_basic_auth_to_principal(auth: dict) -> None:
    key = AsyncHomeDbCache().compute_key(None, auth)
    if auth.get("scheme") == "basic" and "principal" in auth:
        assert isinstance(key, str)
        assert key == auth["principal"]
    else:
        assert isinstance(key, tuple)
        for e in key:
            assert isinstance(e, tuple) and len(e) == 2
            assert isinstance(e[0], str)


_NAN = float("nan")
_NOW = pytz.timezone("Europe/Stockholm").localize(
    DateTime(2021, 8, 12, 12, 34, 57, 123456789)
)


@pytest.mark.parametrize(
    ("auth1", "auth2"),
    (
        ({}, {}),
        ({"foo": "bar"}, {"foo": "bar"}),
        ({"a": 1, "b": 2}, {"b": 2, "a": 1}),
        (
            {
                "scheme": "funky",
                "credentials": "t0pS3cr3t!!11",
                "parameters": {
                    "how much": 1.5,
                    # Note: for special values (NaN, temporal types, etc.),
                    #       equality may rely on object identity.
                    "why": "because",
                    "difficult": _NAN,
                    "also difficult 🔥": _NOW,
                },
            },
            {
                "parameters": {
                    "also difficult 🔥": _NOW,
                    "difficult": _NAN,
                    "why": "because",
                    "how much": 1.5,
                },
                "credentials": "t0pS3cr3t!!11",
                "scheme": "funky",
            },
        ),
    ),
)
def test_key_auth_equality(auth1: dict, auth2: dict) -> None:
    cache = AsyncHomeDbCache()
    key1 = cache.compute_key(None, auth1)
    key2 = cache.compute_key(None, auth2)

    assert len(cache) == 0

    cache.set(key1, "value")
    assert len(cache) == 1
    assert cache.get(key1) == "value"

    cache.set(key2, "value2")
    assert len(cache) == 1
    assert cache.get(key1) == "value2"
    assert cache.get(key2) == "value2"

    assert key1 == key2


def _assert_entries(
    cache: AsyncHomeDbCache,
    expected_entries: t.Collection[tuple[TKey, str]],
    allow_subset: bool = False,
) -> None:
    __tracebackhide__ = True
    if not allow_subset:
        assert len(cache) == len(expected_entries)
        for key, value in expected_entries:
            assert cache.get(key) == value
    else:
        hits = sum(cache.get(key) == value for key, value in expected_entries)
        assert hits == len(cache)


def _force_cache_clean(
    cache: AsyncHomeDbCache,
    now: float | None = None,
) -> None:
    cache._clean(now)


def test_cache_ttl() -> None:
    t0 = datetime(1970, 1, 1)
    with freezegun.freeze_time(t0) as time:
        cache = AsyncHomeDbCache(ttl=1)

        entries = []
        for i in range(1, 11):
            time.move_to(t0 + timedelta(seconds=0.25) * (i - 1))

            entries.append((cache.compute_key(f"{i}", None), f"value{i}"))
            key, value = entries[-1]
            cache.set(key, value)

            _force_cache_clean(cache)
            _assert_entries(cache, entries)

            time.move_to(
                t0 + timedelta(seconds=0.25) * i - timedelta(milliseconds=1)
            )
            _force_cache_clean(cache)
            _assert_entries(cache, entries)

            time.move_to(
                t0 + timedelta(seconds=0.25) * i + timedelta(milliseconds=1)
            )
            _force_cache_clean(cache)
            entries = entries[-3:]
            _assert_entries(cache, entries)


def test_cache_ttl_empty_cache() -> None:
    t0 = datetime(1970, 1, 1)
    with freezegun.freeze_time(t0) as time:
        cache = AsyncHomeDbCache(ttl=1)
        assert len(cache) == 0
        _force_cache_clean(cache)
        assert len(cache) == 0

        time.move_to(t0 + timedelta(seconds=1, milliseconds=1))
        _force_cache_clean(cache)
        assert len(cache) == 0


def test_does_not_return_expired_entries() -> None:
    t0 = datetime(1970, 1, 1)
    with freezegun.freeze_time(t0) as time:
        cache = AsyncHomeDbCache(ttl=1)
        key = cache.compute_key("key", None)
        value = "value"

        cache.set(cache.compute_key("key", None), value)
        assert cache.get(key) == value

        time.move_to(t0 + timedelta(seconds=1, milliseconds=1))
        assert cache.get(key) is None


def test_cache_max_size() -> None:
    cache = AsyncHomeDbCache(max_size=4)

    entries = []
    for i in range(1, 11):
        entries.append((cache.compute_key(f"{i}", None), f"value{i}"))
        entries = entries[-4:]
        key, value = entries[-1]
        cache.set(key, value)

        _force_cache_clean(cache)
        _assert_entries(cache, entries, allow_subset=True)


def test_cache_max_size_empty_cache() -> None:
    cache = AsyncHomeDbCache(max_size=1)
    assert len(cache) == 0
    _force_cache_clean(cache)
    assert len(cache) == 0


def test_clean_up_time() -> None:
    def get_default_cache():
        pool = AsyncNeo4jPool(
            lambda: None, AsyncPoolConfig(), WorkspaceConfig(), None
        )
        return pool.home_db_cache

    repetitions = 5
    scenario_timings = []

    # Test assumes that by default the driver uses a home db cache only limited
    # by its size.
    default_cache = get_default_cache()
    default_max_size = default_cache._max_size
    assert isinstance(default_max_size, int)
    # If ttl ever get used, this test needs to be updated to also test pruning
    # by TTL.
    assert math.isinf(default_cache._ttl) and default_cache._ttl > 0

    for max_size, count in (
        # no pruning needed
        (default_max_size * 10, default_max_size * 10),
        # pruning needed
        (default_max_size, default_max_size * 10),
    ):
        cache = AsyncHomeDbCache(max_size=max_size)
        keys = [cache.compute_key(f"key{i}", None) for i in range(count)]
        rep_timings = []
        for _ in range(repetitions):
            t0 = time.perf_counter()
            for key in keys:
                cache.set(key, "value")
            t1 = time.perf_counter()
            rep_timings.append(t1 - t0)
        scenario_timings.append(sum(rep_timings) / len(rep_timings))

    # pruning shouldn't take more than 20 times the time of no pruning
    # N.B., the pruning takes O(n * log(n)) where n is max_size. By only
    # pruning O(n * log(n)) elements, we get an amortized pruning overhead of
    # O(1) (as long as max_size is small enough to be able to choose a positive
    # pruning size).
    assert scenario_timings[1] <= 20 * scenario_timings[0]
