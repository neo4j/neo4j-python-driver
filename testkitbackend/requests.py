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


with open(path.join(path.dirname(__file__), "skipped_tests.json"), "r") as fd:
    SKIPPED_TESTS = json.load(fd)


def StartTest(backend, data):
    if data["testName"] in SKIPPED_TESTS:
        backend.send_response("SkipTest",
                              {"reason": SKIPPED_TESTS[data["testName"]]})
    else:
        backend.send_response("RunTest", {})


def NewDriver(backend, data):
    auth_token = data["authorizationToken"]["data"]
    data["authorizationToken"].mark_item_as_read_if_equals(
        "name", "AuthorizationToken"
    )
    auth = neo4j.Auth(
            auth_token["scheme"], auth_token["principal"],
            auth_token["credentials"], realm=auth_token["realm"])
    auth_token.mark_item_as_read_if_equals("ticket", "")
    resolver = resolution_func(backend) if data["resolverRegistered"] else None
    connection_timeout = data.get("connectionTimeoutMs", None)
    if connection_timeout is not None:
        connection_timeout /= 1000
    data.mark_item_as_read_if_equals("domainNameResolverRegistered", False)
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


def resolution_func(backend):
    def resolve(address):
        key = backend.next_key()
        address = ":".join(map(str, address))
        backend.send_response("ResolverResolutionRequired", {
            "id": key,
            "address": address
        })
        if not backend.process_request():
            # connection was closed before end of next message
            return []
        if key not in backend.address_resolutions:
            raise RuntimeError(
                "Backend did not receive expected ResolverResolutionCompleted "
                "message for id %s" % key
            )
        resolution = backend.address_resolutions[key]
        resolution = list(map(neo4j.Address.parse, resolution))
        # won't be needed anymore -> conserve memory
        del backend.address_resolutions[key]
        return resolution

    return resolve


def ResolverResolutionCompleted(backend, data):
    backend.address_resolutions[data["requestId"]] = data["addresses"]


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
    backend.send_response("Result", {"id": key})


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
                    raise Exception("Client said no")

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
    backend.send_response("Result", {"id": key})


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
    result.consume()
    backend.send_response("Summary", {})


def RetryablePositive(backend, data):
    key = data["sessionId"]
    session_tracker = backend.sessions[key]
    session_tracker.state = '+'


def RetryableNegative(backend, data):
    key = data["sessionId"]
    session_tracker = backend.sessions[key]
    session_tracker.state = '-'
    session_tracker.error_id = data.get('errorId', '')
