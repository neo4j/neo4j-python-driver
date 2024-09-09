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

import asyncio
import sys
from functools import wraps

import pytest
import pytest_asyncio

from neo4j import (
    AsyncGraphDatabase,
    GraphDatabase,
)
from neo4j.debug import watch

from . import env


# from neo4j.debug import watch
#
# watch("neo4j")


@pytest.fixture(scope="session")
def uri():
    return env.NEO4J_SERVER_URI


@pytest.fixture(scope="session")
def bolt_uri(uri):
    if env.NEO4J_SCHEME != "bolt":
        pytest.skip("Test requires bolt scheme")
    return uri


@pytest.fixture(scope="session")
def _forced_bolt_uri():
    return f"bolt://{env.NEO4J_HOST}:{env.NEO4J_PORT}"


@pytest.fixture(scope="session")
def neo4j_uri():
    if env.NEO4J_SCHEME != "neo4j":
        pytest.skip("Test requires neo4j scheme")
    return uri


@pytest.fixture(scope="session")
def _forced_neo4j_uri():
    return f"neo4j://{env.NEO4J_HOST}:{env.NEO4J_PORT}"


@pytest.fixture(scope="session")
def auth():
    return env.NEO4J_USER, env.NEO4J_PASS


@pytest.fixture
def driver(uri, auth):
    with GraphDatabase.driver(uri, auth=auth) as driver:
        yield driver


@pytest.fixture
def bolt_driver(bolt_uri, auth):
    with GraphDatabase.driver(bolt_uri, auth=auth) as driver:
        yield driver


@pytest.fixture
def neo4j_driver(neo4j_uri, auth):
    with GraphDatabase.driver(neo4j_uri, auth=auth) as driver:
        yield driver


@wraps(AsyncGraphDatabase.driver)
def get_async_driver(*args, **kwargs):
    return AsyncGraphDatabase.driver(*args, **kwargs)


@pytest_asyncio.fixture
async def async_driver(uri, auth):
    async with get_async_driver(uri, auth=auth) as driver:
        yield driver


@pytest_asyncio.fixture
async def async_bolt_driver(bolt_uri, auth):
    async with get_async_driver(bolt_uri, auth=auth) as driver:
        yield driver


@pytest_asyncio.fixture
async def async_neo4j_driver(neo4j_uri, auth):
    async with get_async_driver(neo4j_uri, auth=auth) as driver:
        yield driver


@pytest.fixture
def _forced_bolt_driver(_forced_bolt_uri):
    with GraphDatabase.driver(_forced_bolt_uri, auth=auth) as driver:
        yield driver


@pytest.fixture
def _forced_neo4j_driver(_forced_neo4j_uri):
    with GraphDatabase.driver(_forced_neo4j_uri, auth=auth) as driver:
        yield driver


@pytest.fixture(scope="session")
def server_info(_forced_bolt_driver):
    return _forced_bolt_driver.get_server_info()


@pytest.fixture(scope="session")
def bolt_protocol_version(server_info):
    return server_info.protocol_version


def _parse_version(version: str) -> tuple[float, ...]:
    def parse_segment(seg: str) -> float:
        if seg == "dev":
            return float("inf")
        return float(int(seg))

    return tuple(map(parse_segment, version.split(".")))


def mark_requires_min_bolt_version(version="3.5"):
    return pytest.mark.skipif(
        _parse_version(env.NEO4J_VERSION) < _parse_version(version),
        reason=f"requires server version '{version}' or higher, "
        f"found '{env.NEO4J_VERSION}'",
    )


def mark_requires_edition(edition):
    return pytest.mark.skipif(
        edition != env.NEO4J_EDITION,
        reason=f"requires server edition '{edition}', "
        f"found '{env.NEO4J_EDITION}'",
    )


@pytest.fixture
def session(driver):
    with driver.session() as session:
        yield session


@pytest.fixture
def bolt_session(bolt_driver):
    with bolt_driver.session() as session:
        yield session


@pytest.fixture
def neo4j_session(neo4j_driver):
    with neo4j_driver.session() as session:
        yield session


# async support for pytest-benchmark
# https://github.com/ionelmc/pytest-benchmark/issues/66
@pytest_asyncio.fixture
def aio_benchmark(benchmark, event_loop):
    def _wrapper(func, *args, **kwargs):
        if asyncio.iscoroutinefunction(func):

            @benchmark
            def _():
                return event_loop.run_until_complete(func(*args, **kwargs))
        else:
            benchmark(func, *args, **kwargs)

    return _wrapper


@pytest.fixture
def watcher():
    with watch("neo4j", out=sys.stdout, colour=True):
        yield
