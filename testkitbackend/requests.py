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
import testkitbackend.fromtestkit as fromtestkit
import testkitbackend.totestkit as totestkit
from testkitbackend.fromtestkit import to_meta_and_timeout


class FrontendError(Exception):
    pass


def load_config():
    with open(path.join(path.dirname(__file__), "test_config.json"), "r") as fd:
        config = json.load(fd)
    return (config["skips"],
            [k for k, v in config["features"].items() if v is True])


SKIPPED_TESTS, FEATURES = load_config()


def StartTest(backend, data):
    if data["testName"] in SKIPPED_TESTS:
        backend.send_response("SkipTest",
                              {"reason": SKIPPED_TESTS[data["testName"]]})
    else:
        backend.send_response("RunTest", {})


def GetFeatures(backend, data):
    backend.send_response("FeatureList", {"features": FEATURES})


def NewDriver(backend, data):
    auth_token = data["authorizationToken"]["data"]
    data["authorizationToken"].mark_item_as_read_if_equals(
        "name", "AuthorizationToken"
    )
    auth = neo4j.Auth(
            auth_token["scheme"], auth_token["principal"],
            auth_token["credentials"], realm=auth_token["realm"])
    auth_token.mark_item_as_read_if_equals("ticket", "")
    resolver = None
    if data["resolverRegistered"] or data["domainNameResolverRegistered"]:
        resolver = resolution_func(backend, data["resolverRegistered"],
                                   data["domainNameResolverRegistered"])
    connection_timeout = data.get("connectionTimeoutMs", None)
    if connection_timeout is not None:
        connection_timeout /= 1000
    data.mark_item_as_read("domainNameResolverRegistered")
    driver = neo4j.GraphDatabase.driver(
        data["uri"], auth=auth, user_agent=data["userAgent"],
        resolver=resolver, connection_timeout=connection_timeout
    )
    key = backend.next_key()
    backend.drivers[key] = driver
    backend.send_response("Driver", {"id": key})


def VerifyConnectivity(backend, data):
    driver_id = data["driverId"]
    driver = backend.drivers[driver_id]
    driver.verify_connectivity()
    backend.send_response("Driver", {"id": driver_id})


def CheckMultiDBSupport(backend, data):
    driver_id = data["driverId"]
    driver = backend.drivers[driver_id]
    backend.send_response(
        "MultiDBSupport",
        {"id": backend.next_key(), "available": driver.supports_multi_db()}
    )


def resolution_func(backend, custom_resolver=False, custom_dns_resolver=False):
    # This solution (putting custom resolution together with DNS resolution into
    # one function only works because the Python driver calls the custom
    # resolver function for every connection, which is not true for all drivers.
    # Properly exposing a way to change the DNS lookup behavior is not possible
    # without changing the driver's code.
    assert custom_resolver or custom_dns_resolver

    def resolve(address):
        addresses = [":".join(map(str, address))]
        if custom_resolver:
            key = backend.next_key()
            backend.send_response("ResolverResolutionRequired", {
                "id": key,
                "address": addresses[0]
            })
            if not backend.process_request():
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
                backend.send_response("DomainNameResolutionRequired", {
                    "id": key,
                    "name": address[0]
                })
                if not backend.process_request():
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


def ResolverResolutionCompleted(backend, data):
    backend.custom_resolutions[data["requestId"]] = data["addresses"]


def DomainNameResolutionCompleted(backend, data):
    backend.dns_resolutions[data["requestId"]] = data["addresses"]


def DriverClose(backend, data):
    key = data["driverId"]
    driver = backend.drivers[key]
    driver.close()
    backend.send_response("Driver", {"id": key})


class SessionTracker:
    """ Keeps some extra state about the tracked session
    """

    def __init__(self, session):
        self.session = session
        self.state = ""
        self.error_id = ""


def NewSession(backend, data):
    driver = backend.drivers[data["driverId"]]
    access_mode = data["accessMode"]
    if access_mode == "r":
        access_mode = neo4j.READ_ACCESS
    elif access_mode == "w":
        access_mode = neo4j.WRITE_ACCESS
    else:
        raise Exception("Unknown access mode:" + access_mode)
    config = {
            "default_access_mode": access_mode,
            "bookmarks": data["bookmarks"],
            "database": data["database"],
            "fetch_size": data.get("fetchSize", None)
    }
    session = driver.session(**config)
    key = backend.next_key()
    backend.sessions[key] = SessionTracker(session)
    backend.send_response("Session", {"id": key})


def SessionRun(backend, data):
    session = backend.sessions[data["sessionId"]].session
    query, params = fromtestkit.to_query_and_params(data)
    result = session.run(query, parameters=params)
    key = backend.next_key()
    backend.results[key] = result
    backend.send_response("Result", {"id": key, "keys": result.keys()})


def SessionClose(backend, data):
    key = data["sessionId"]
    session = backend.sessions[key].session
    session.close()
    del backend.sessions[key]
    backend.send_response("Session", {"id": key})


def SessionBeginTransaction(backend, data):
    key = data["sessionId"]
    session = backend.sessions[key].session
    metadata, timeout = to_meta_and_timeout(data)
    tx = session.begin_transaction(metadata=metadata, timeout=timeout)
    key = backend.next_key()
    backend.transactions[key] = tx
    backend.send_response("Transaction", {"id": key})


def SessionReadTransaction(backend, data):
    transactionFunc(backend, data, True)


def SessionWriteTransaction(backend, data):
    transactionFunc(backend, data, False)


def transactionFunc(backend, data, is_read):
    key = data["sessionId"]
    session_tracker = backend.sessions[key]
    session = session_tracker.session
    metadata, timeout = to_meta_and_timeout(data)

    @neo4j.unit_of_work(metadata=metadata, timeout=timeout)
    def func(tx):
        txkey = backend.next_key()
        backend.transactions[txkey] = tx
        session_tracker.state = ''
        backend.send_response("RetryableTry", {"id": txkey})

        cont = True
        while cont:
            cont = backend.process_request()
            if session_tracker.state == '+':
                cont = False
            elif session_tracker.state == '-':
                if session_tracker.error_id:
                    raise backend.errors[session_tracker.error_id]
                else:
                    raise FrontendError("Client said no")

    if is_read:
        session.read_transaction(func)
    else:
        session.write_transaction(func)
    backend.send_response("RetryableDone", {})


def SessionLastBookmarks(backend, data):
    key = data["sessionId"]
    session = backend.sessions[key].session
    bookmark = session.last_bookmark()
    bookmarks = []
    if bookmark:
        bookmarks.append(bookmark)
    backend.send_response("Bookmarks", {"bookmarks": bookmarks})


def TransactionRun(backend, data):
    key = data["txId"]
    tx = backend.transactions[key]
    cypher, params = fromtestkit.to_cypher_and_params(data)
    result = tx.run(cypher, parameters=params)
    key = backend.next_key()
    backend.results[key] = result
    backend.send_response("Result", {"id": key, "keys": result.keys()})


def TransactionCommit(backend, data):
    key = data["txId"]
    tx = backend.transactions[key]
    tx.commit()
    backend.send_response("Transaction", {"id": key})


def TransactionRollback(backend, data):
    key = data["txId"]
    tx = backend.transactions[key]
    tx.rollback()
    backend.send_response("Transaction", {"id": key})


def ResultNext(backend, data):
    result = backend.results[data["resultId"]]
    try:
        record = next(iter(result))
    except StopIteration:
        backend.send_response("NullRecord", {})
        return
    backend.send_response("Record", totestkit.record(record))


def ResultConsume(backend, data):
    result = backend.results[data["resultId"]]
    summary = result.consume()
    from neo4j.work.summary import ResultSummary
    assert isinstance(summary, ResultSummary)
    backend.send_response("Summary", {
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


def RetryablePositive(backend, data):
    key = data["sessionId"]
    session_tracker = backend.sessions[key]
    session_tracker.state = '+'


def RetryableNegative(backend, data):
    key = data["sessionId"]
    session_tracker = backend.sessions[key]
    session_tracker.state = '-'
    session_tracker.error_id = data.get('errorId', '')


def ForcedRoutingTableUpdate(backend, data):
    driver_id = data["driverId"]
    driver = backend.drivers[driver_id]
    database = data["database"]
    bookmarks = data["bookmarks"]
    with driver._pool.refresh_lock:
        driver._pool.create_routing_table(database)
        driver._pool.update_routing_table(database=database,
                                          bookmarks=bookmarks)
    backend.send_response("Driver", {"id": driver_id})


def GetRoutingTable(backend, data):
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
    backend.send_response("RoutingTable", response_data)
