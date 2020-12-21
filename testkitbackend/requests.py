# Copyright (c) 2002-2020 "Neo4j,"
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
import neo4j
import testkitbackend.fromtestkit as fromtestkit
import testkitbackend.totestkit as totestkit


def NewDriver(backend, data):
    authToken = data["authorizationToken"]["data"]
    auth = neo4j.Auth(
            authToken["scheme"], authToken["principal"],
            authToken["credentials"], realm=authToken["realm"])
    driver = neo4j.GraphDatabase.driver(data["uri"], auth=auth)
    key = backend.next_key()
    backend.drivers[key] = driver
    backend.send_response("Driver", {"id": key})


def DriverClose(backend, data):
    key = data["driverId"]
    driver = backend.drivers[key]
    driver.close()
    backend.send_response("Driver", {"id": key})


class SessionState:
    """ Keeps some extra state about the tracked session
    """

    def __init__(self, session):
        self.session = session


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
    }
    # TODO: fetchSize, database
    session = driver.session(**config)
    key = backend.next_key()
    backend.sessions[key] = SessionState(session)
    backend.send_response("Session", {"id": key})


def SessionRun(backend, data):
    session = backend.sessions[data["sessionId"]].session
    cypher, params = fromtestkit.toCypherAndParams(data)
    # TODO: txMeta, timeout
    result = session.run(cypher, parameters=params)
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
    metadata = data.get('txMeta', None)
    timeout = data.get('timeout', None)
    if timeout:
        timeout = int(timeout)
    tx = session.begin_transaction(metadata=metadata, timeout=timeout)
    key = backend.next_key()
    backend.transactions[key] = tx
    backend.send_response("Transaction", {"id": key})


def ResultNext(backend, data):
    result = backend.results[data["resultId"]]
    try:
        record = next(iter(result))
    except StopIteration:
        backend.send_response("NullRecord", {})
    backend.send_response("Record", totestkit.record(record))


def TransactionRun(backend, data):
    key = data["txId"]
    tx = backend.transactions[key]
    cypher, params = fromtestkit.toCypherAndParams(data)
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
