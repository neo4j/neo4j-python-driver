# Copyright (c) "Neo4j"
# Neo4j Sweden AB [http://neo4j.com]
#
# This file is part of Neo4j.
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


import json
from os import path

import neo4j
from neo4j._async_compat.util import AsyncUtil

from .. import (
    fromtestkit,
    totestkit,
)


class FrontendError(Exception):
    pass


def load_config():
    config_path = path.join(path.dirname(__file__), "..", "test_config.json")
    with open(config_path, "r") as fd:
        config = json.load(fd)
    skips = config["skips"]
    features = [k for k, v in config["features"].items() if v is True]
    import ssl
    if ssl.HAS_TLSv1_3:
        features += ["Feature:TLS:1.3"]
    return skips, features


SKIPPED_TESTS, FEATURES = load_config()


async def StartTest(backend, data):
    if data["testName"] in SKIPPED_TESTS:
        await backend.send_response("SkipTest", {
            "reason": SKIPPED_TESTS[data["testName"]]
        })
    else:
        await backend.send_response("RunTest", {})


async def GetFeatures(backend, data):
    await backend.send_response("FeatureList", {"features": FEATURES})


async def NewDriver(backend, data):
    auth_token = data["authorizationToken"]["data"]
    data["authorizationToken"].mark_item_as_read_if_equals(
        "name", "AuthorizationToken"
    )
    scheme = auth_token["scheme"]
    if scheme == "basic":
        auth = neo4j.basic_auth(
            auth_token["principal"], auth_token["credentials"],
            realm=auth_token.get("realm", None)
        )
    elif scheme == "kerberos":
        auth = neo4j.kerberos_auth(auth_token["credentials"])
    elif scheme == "bearer":
        auth = neo4j.bearer_auth(auth_token["credentials"])
    else:
        auth = neo4j.custom_auth(
            auth_token["principal"], auth_token["credentials"],
            auth_token["realm"], auth_token["scheme"],
            **auth_token.get("parameters", {})
        )
        auth_token.mark_item_as_read("parameters", recursive=True)
    resolver = None
    if data["resolverRegistered"] or data["domainNameResolverRegistered"]:
        resolver = resolution_func(backend, data["resolverRegistered"],
                                   data["domainNameResolverRegistered"])
    connection_timeout = data.get("connectionTimeoutMs")
    if connection_timeout is not None:
        connection_timeout /= 1000
    max_transaction_retry_time = data.get("maxTxRetryTimeMs")
    if max_transaction_retry_time is not None:
        max_transaction_retry_time /= 1000
    data.mark_item_as_read("domainNameResolverRegistered")
    driver = neo4j.AsyncGraphDatabase.driver(
        data["uri"], auth=auth, user_agent=data["userAgent"],
        resolver=resolver, connection_timeout=connection_timeout,
        fetch_size=data.get("fetchSize"),
        max_transaction_retry_time=max_transaction_retry_time,
    )
    key = backend.next_key()
    backend.drivers[key] = driver
    await backend.send_response("Driver", {"id": key})


async def VerifyConnectivity(backend, data):
    driver_id = data["driverId"]
    driver = backend.drivers[driver_id]
    await driver.verify_connectivity()
    await backend.send_response("Driver", {"id": driver_id})


async def CheckMultiDBSupport(backend, data):
    driver_id = data["driverId"]
    driver = backend.drivers[driver_id]
    await backend.send_response("MultiDBSupport", {
        "id": backend.next_key(), "available": await driver.supports_multi_db()
    })


def resolution_func(backend, custom_resolver=False, custom_dns_resolver=False):
    # This solution (putting custom resolution together with DNS resolution
    # into one function only works because the Python driver calls the custom
    # resolver function for every connection, which is not true for all
    # drivers. Properly exposing a way to change the DNS lookup behavior is not
    # possible without changing the driver's code.
    assert custom_resolver or custom_dns_resolver

    async def resolve(address):
        addresses = [":".join(map(str, address))]
        if custom_resolver:
            key = backend.next_key()
            await backend.send_response("ResolverResolutionRequired", {
                "id": key,
                "address": addresses[0]
            })
            if not await backend.process_request():
                # connection was closed before end of next message
                return []
            if key not in backend.custom_resolutions:
                raise RuntimeError(
                    "Backend did not receive expected "
                    "ResolverResolutionCompleted message for id %s" % key
                )
            addresses = backend.custom_resolutions.pop(key)
        if custom_dns_resolver:
            dns_resolved_addresses = []
            for address in addresses:
                key = backend.next_key()
                address = address.rsplit(":", 1)
                await backend.send_response("DomainNameResolutionRequired", {
                    "id": key,
                    "name": address[0]
                })
                if not await backend.process_request():
                    # connection was closed before end of next message
                    return []
                if key not in backend.dns_resolutions:
                    raise RuntimeError(
                        "Backend did not receive expected "
                        "DomainNameResolutionCompleted message for id %s" % key
                    )
                dns_resolved_addresses += list(map(
                    lambda a: ":".join((a, *address[1:])),
                    backend.dns_resolutions.pop(key)
                ))

            addresses = dns_resolved_addresses

        return list(map(neo4j.Address.parse, addresses))

    return resolve


async def ResolverResolutionCompleted(backend, data):
    backend.custom_resolutions[data["requestId"]] = data["addresses"]


async def DomainNameResolutionCompleted(backend, data):
    backend.dns_resolutions[data["requestId"]] = data["addresses"]


async def DriverClose(backend, data):
    key = data["driverId"]
    driver = backend.drivers[key]
    await driver.close()
    await backend.send_response("Driver", {"id": key})


class SessionTracker:
    """ Keeps some extra state about the tracked session
    """

    def __init__(self, session):
        self.session = session
        self.state = ""
        self.error_id = ""


async def NewSession(backend, data):
    driver = backend.drivers[data["driverId"]]
    access_mode = data["accessMode"]
    if access_mode == "r":
        access_mode = neo4j.READ_ACCESS
    elif access_mode == "w":
        access_mode = neo4j.WRITE_ACCESS
    else:
        raise ValueError("Unknown access mode:" + access_mode)
    config = {
            "default_access_mode": access_mode,
            "bookmarks": data["bookmarks"],
            "database": data["database"],
            "fetch_size": data.get("fetchSize", None),
            "impersonated_user": data.get("impersonatedUser", None),

    }
    session = driver.session(**config)
    key = backend.next_key()
    backend.sessions[key] = SessionTracker(session)
    await backend.send_response("Session", {"id": key})


async def SessionRun(backend, data):
    session = backend.sessions[data["sessionId"]].session
    query, params = fromtestkit.to_query_and_params(data)
    result = await session.run(query, parameters=params)
    key = backend.next_key()
    backend.results[key] = result
    await backend.send_response("Result", {"id": key, "keys": result.keys()})


async def SessionClose(backend, data):
    key = data["sessionId"]
    session = backend.sessions[key].session
    await session.close()
    del backend.sessions[key]
    await backend.send_response("Session", {"id": key})


async def SessionBeginTransaction(backend, data):
    key = data["sessionId"]
    session = backend.sessions[key].session
    metadata, timeout = fromtestkit.to_meta_and_timeout(data)
    tx = await session.begin_transaction(metadata=metadata, timeout=timeout)
    key = backend.next_key()
    backend.transactions[key] = tx
    await backend.send_response("Transaction", {"id": key})


async def SessionReadTransaction(backend, data):
    await transactionFunc(backend, data, True)


async def SessionWriteTransaction(backend, data):
    await transactionFunc(backend, data, False)


async def transactionFunc(backend, data, is_read):
    key = data["sessionId"]
    session_tracker = backend.sessions[key]
    session = session_tracker.session
    metadata, timeout = fromtestkit.to_meta_and_timeout(data)

    @neo4j.unit_of_work(metadata=metadata, timeout=timeout)
    async def func(tx):
        txkey = backend.next_key()
        backend.transactions[txkey] = tx
        session_tracker.state = ''
        await backend.send_response("RetryableTry", {"id": txkey})

        cont = True
        while cont:
            cont = await backend.process_request()
            if session_tracker.state == '+':
                cont = False
            elif session_tracker.state == '-':
                if session_tracker.error_id:
                    raise backend.errors[session_tracker.error_id]
                else:
                    raise FrontendError("Client said no")

    if is_read:
        await session.read_transaction(func)
    else:
        await session.write_transaction(func)
    await backend.send_response("RetryableDone", {})


async def RetryablePositive(backend, data):
    key = data["sessionId"]
    session_tracker = backend.sessions[key]
    session_tracker.state = '+'


async def RetryableNegative(backend, data):
    key = data["sessionId"]
    session_tracker = backend.sessions[key]
    session_tracker.state = '-'
    session_tracker.error_id = data.get('errorId', '')


async def SessionLastBookmarks(backend, data):
    key = data["sessionId"]
    session = backend.sessions[key].session
    bookmark = await session.last_bookmark()
    bookmarks = []
    if bookmark:
        bookmarks.append(bookmark)
    await backend.send_response("Bookmarks", {"bookmarks": bookmarks})


async def TransactionRun(backend, data):
    key = data["txId"]
    tx = backend.transactions[key]
    cypher, params = fromtestkit.to_cypher_and_params(data)
    result = await tx.run(cypher, parameters=params)
    key = backend.next_key()
    backend.results[key] = result
    await backend.send_response("Result", {"id": key, "keys": result.keys()})


async def TransactionCommit(backend, data):
    key = data["txId"]
    tx = backend.transactions[key]
    await tx.commit()
    await backend.send_response("Transaction", {"id": key})


async def TransactionRollback(backend, data):
    key = data["txId"]
    tx = backend.transactions[key]
    await tx.rollback()
    await backend.send_response("Transaction", {"id": key})


async def TransactionClose(backend, data):
    key = data["txId"]
    tx = backend.transactions[key]
    await tx.close()
    await backend.send_response("Transaction", {"id": key})


async def ResultNext(backend, data):
    result = backend.results[data["resultId"]]

    try:
        record = await AsyncUtil.next(AsyncUtil.iter(result))
    except StopAsyncIteration:
        await backend.send_response("NullRecord", {})
        return
    await backend.send_response("Record", totestkit.record(record))


async def ResultSingle(backend, data):
    result = backend.results[data["resultId"]]
    await backend.send_response("Record", totestkit.record(result.single()))


async def ResultPeek(backend, data):
    result = backend.results[data["resultId"]]
    record = await result.peek()
    if record is not None:
        await backend.send_response("Record", totestkit.record(record))
    else:
        await backend.send_response("NullRecord", {})


async def ResultList(backend, data):
    result = backend.results[data["resultId"]]
    records = await AsyncUtil.list(result)
    await backend.send_response("RecordList", {
        "records": [totestkit.record(r) for r in records]
    })


async def ResultConsume(backend, data):
    result = backend.results[data["resultId"]]
    summary = await result.consume()
    from neo4j import ResultSummary
    assert isinstance(summary, ResultSummary)
    await backend.send_response("Summary", {
        "serverInfo": {
            "address": ":".join(map(str, summary.server.address)),
            "agent": summary.server.agent,
            "protocolVersion":
                ".".join(map(str, summary.server.protocol_version)),
        },
        "counters": None if not summary.counters else {
            "constraintsAdded": summary.counters.constraints_added,
            "constraintsRemoved": summary.counters.constraints_removed,
            "containsSystemUpdates": summary.counters.contains_system_updates,
            "containsUpdates": summary.counters.contains_updates,
            "indexesAdded": summary.counters.indexes_added,
            "indexesRemoved": summary.counters.indexes_removed,
            "labelsAdded": summary.counters.labels_added,
            "labelsRemoved": summary.counters.labels_removed,
            "nodesCreated": summary.counters.nodes_created,
            "nodesDeleted": summary.counters.nodes_deleted,
            "propertiesSet": summary.counters.properties_set,
            "relationshipsCreated": summary.counters.relationships_created,
            "relationshipsDeleted": summary.counters.relationships_deleted,
            "systemUpdates": summary.counters.system_updates,
        },
        "database": summary.database,
        "notifications": summary.notifications,
        "plan": summary.plan,
        "profile": summary.profile,
        "query": {
            "text": summary.query,
            "parameters": {k: totestkit.field(v)
                           for k, v in summary.parameters.items()},
        },
        "queryType": summary.query_type,
        "resultAvailableAfter": summary.result_available_after,
        "resultConsumedAfter": summary.result_consumed_after,
    })


async def ForcedRoutingTableUpdate(backend, data):
    driver_id = data["driverId"]
    driver = backend.drivers[driver_id]
    database = data["database"]
    bookmarks = data["bookmarks"]
    async with driver._pool.refresh_lock:
        await driver._pool.update_routing_table(
            database=database, imp_user=None, bookmarks=bookmarks
        )
    await backend.send_response("Driver", {"id": driver_id})


async def GetRoutingTable(backend, data):
    driver_id = data["driverId"]
    database = data["database"]
    driver = backend.drivers[driver_id]
    routing_table = driver._pool.routing_tables[database]
    response_data = {
        "database": routing_table.database,
        "ttl": routing_table.ttl,
    }
    for role in ("routers", "readers", "writers"):
        addresses = routing_table.__getattribute__(role)
        response_data[role] = list(map(str, addresses))
    await backend.send_response("RoutingTable", response_data)
