#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2019, Nigel Small
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from os import getenv
from threading import RLock

from boltkit.server import Neo4jService
from pytest import fixture

from neo4j import GraphDatabase


NEO4J_RELEASES = getenv("NEO4J_RELEASES", "snapshot-enterprise 3.5-enterprise").split()
NEO4J_HOST = "localhost"
NEO4J_PORTS = {
    "bolt": 17601,
    "http": 17401,
    "https": 17301,
}
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"
NEO4J_AUTH = (NEO4J_USER, NEO4J_PASSWORD)
NEO4J_LOCK = RLock()
NEO4J_SERVICE = None
NEO4J_DEBUG = getenv("NEO4J_DEBUG", "")


@fixture(scope="session", params=NEO4J_RELEASES)
def service(request):
    global NEO4J_SERVICE
    if NEO4J_DEBUG:
        from neo4j.debug import watch
        watch("neo4j", "boltkit")
    with NEO4J_LOCK:
        assert NEO4J_SERVICE is None
        NEO4J_SERVICE = Neo4jService(auth=NEO4J_AUTH, image=request.param, n_cores=3, n_replicas=2)
        NEO4J_SERVICE.start(timeout=300)
        yield NEO4J_SERVICE
        if NEO4J_SERVICE is not None:
            NEO4J_SERVICE.stop(timeout=300)
            NEO4J_SERVICE = None


@fixture(scope="session")
def addresses(service):
    try:
        machines = service.cores()
    except AttributeError:
        machines = list(service.machines.values())
    return [machine.address for machine in machines]


@fixture(scope="session")
def readonly_addresses(service):
    try:
        machines = service.replicas()
    except AttributeError:
        machines = []
    return [machine.address for machine in machines]


@fixture(scope="session")
def address(addresses):
    try:
        return addresses[0]
    except IndexError:
        return None


@fixture(scope="session")
def readonly_address(readonly_addresses):
    try:
        return readonly_addresses[0]
    except IndexError:
        return None


@fixture(scope="session")
def targets(addresses):
    return " ".join("{}:{}".format(address[0], address[1]) for address in addresses)


@fixture(scope="session")
def readonly_targets(addresses):
    return " ".join("{}:{}".format(address[0], address[1]) for address in readonly_addresses)


@fixture(scope="session")
def target(address):
    return "{}:{}".format(address[0], address[1])


@fixture(scope="session")
def readonly_target(readonly_address):
    return "{}:{}".format(readonly_address[0], readonly_address[1])


@fixture(scope="session")
def uri(service, target):
    return "bolt://" + target


@fixture(scope="session")
def bolt_uri(service, target):
    return "bolt://" + target


@fixture(scope="session")
def readonly_bolt_uri(service, readonly_target):
    return "bolt://" + readonly_target


@fixture(scope="session")
def neo4j_uri(service, target):
    return "neo4j://" + target


@fixture(scope="session")
def auth():
    return NEO4J_AUTH


@fixture(scope="session")
def bolt_driver(target, auth):
    driver = GraphDatabase.bolt_driver(target, auth=auth)
    try:
        yield driver
    finally:
        driver.close()


@fixture(scope="session")
async def async_bolt_driver(target, auth):
    driver = await GraphDatabase.async_bolt_driver(target, auth=auth)
    try:
        yield driver
    finally:
        driver.close()


@fixture()
def session(bolt_driver):
    session = bolt_driver.session()
    try:
        yield session
    finally:
        session.close()


@fixture
def cypher_eval(session):

    def run_and_rollback(tx, cypher, **parameters):
        result = tx.run(cypher, **parameters)
        value = result.single().value()
        tx.success = False
        return value

    def f(cypher, **parameters):
        return session.write_transaction(run_and_rollback, cypher, **parameters)

    return f


def pytest_sessionfinish(session, exitstatus):
    """ Called after the entire session to ensure Neo4j is shut down.
    """
    global NEO4J_SERVICE
    with NEO4J_LOCK:
        if NEO4J_SERVICE is not None:
            NEO4J_SERVICE.stop(timeout=300)
            NEO4J_SERVICE = None
