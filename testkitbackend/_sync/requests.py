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


import json
import re
import warnings
from os import path

import neo4j
import neo4j.api
from neo4j._async_compat.util import Util

from .. import (
    fromtestkit,
    test_subtest_skips,
    totestkit,
)
from .._warning_check import warning_check
from ..exceptions import MarkdAsDriverException


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


def _get_skip_reason(test_name):
    for skip_pattern, reason in SKIPPED_TESTS.items():
        if skip_pattern[0] == skip_pattern[-1] == "'":
            match = skip_pattern[1:-1] == test_name
        else:
            match = re.match(skip_pattern, test_name)
        if match:
            return reason


def StartTest(backend, data):
    test_name = data["testName"]
    reason = _get_skip_reason(test_name)
    if reason is not None:
        if reason.startswith("test_subtest_skips."):
            backend.send_response("RunSubTests", {})
        else:
            backend.send_response("SkipTest", {"reason": reason})
    else:
        backend.send_response("RunTest", {})


def StartSubTest(backend, data):
    test_name = data["testName"]
    subtest_args = data["subtestArguments"]
    subtest_args.mark_all_as_read(recursive=True)
    reason = _get_skip_reason(test_name)
    assert reason and reason.startswith("test_subtest_skips.") or print(reason)
    func = getattr(test_subtest_skips, reason[19:])
    reason = func(**subtest_args)
    if reason is not None:
        backend.send_response("SkipTest", {"reason": reason})
    else:
        backend.send_response("RunTest", {})


def GetFeatures(backend, data):
    backend.send_response("FeatureList", {"features": FEATURES})


def NewDriver(backend, data):
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
    kwargs = {}
    if data["resolverRegistered"] or data["domainNameResolverRegistered"]:
        kwargs["resolver"] = resolution_func(
            backend, data["resolverRegistered"],
            data["domainNameResolverRegistered"]
        )
    for timeout_testkit, timeout_driver in (
        ("connectionTimeoutMs", "connection_timeout"),
        ("maxTxRetryTimeMs", "max_transaction_retry_time"),
        ("connectionAcquisitionTimeoutMs", "connection_acquisition_timeout"),
    ):
        if data.get(timeout_testkit) is not None:
            kwargs[timeout_driver] = data[timeout_testkit] / 1000
    for k in ("sessionConnectionTimeoutMs", "updateRoutingTableTimeoutMs"):
        if k in data:
            data.mark_item_as_read_if_equals(k, None)
    if data.get("maxConnectionPoolSize"):
        kwargs["max_connection_pool_size"] = data["maxConnectionPoolSize"]
    if data.get("fetchSize"):
        kwargs["fetch_size"] = data["fetchSize"]
    if "encrypted" in data:
        kwargs["encrypted"] = data["encrypted"]
    if "trustedCertificates" in data:
        if data["trustedCertificates"] is None:
            kwargs["trusted_certificates"] = neo4j.TrustSystemCAs()
        elif not data["trustedCertificates"]:
            kwargs["trusted_certificates"] = neo4j.TrustAll()
        else:
            cert_paths = ("/usr/local/share/custom-ca-certificates/" + cert
                          for cert in data["trustedCertificates"])
            kwargs["trusted_certificates"] = neo4j.TrustCustomCAs(*cert_paths)
    data.mark_item_as_read_if_equals("livenessCheckTimeoutMs", None)

    driver = neo4j.GraphDatabase.driver(
        data["uri"], auth=auth, user_agent=data["userAgent"], **kwargs
    )
    key = backend.next_key()
    backend.drivers[key] = driver
    backend.send_response("Driver", {"id": key})


def VerifyConnectivity(backend, data):
    driver_id = data["driverId"]
    driver = backend.drivers[driver_id]
    driver.verify_connectivity()
    backend.send_response("Driver", {"id": driver_id})


def GetServerInfo(backend, data):
    driver_id = data["driverId"]
    driver = backend.drivers[driver_id]
    server_info = driver.get_server_info()
    backend.send_response("ServerInfo", {
        "address": ":".join(map(str, server_info.address)),
        "agent": server_info.agent,
        "protocolVersion": ".".join(map(str, server_info.protocol_version)),
    })


def CheckMultiDBSupport(backend, data):
    driver_id = data["driverId"]
    driver = backend.drivers[driver_id]
    with warning_check(
        neo4j.ExperimentalWarning,
        "Feature support query, based on Bolt protocol version and Neo4j "
        "server version will change in the future."
    ):
        available = driver.supports_multi_db()
    backend.send_response("MultiDBSupport", {
        "id": backend.next_key(), "available": available
    })


def resolution_func(backend, custom_resolver=False, custom_dns_resolver=False):
    # This solution (putting custom resolution together with DNS resolution
    # into one function only works because the Python driver calls the custom
    # resolver function for every connection, which is not true for all
    # drivers. Properly exposing a way to change the DNS lookup behavior is not
    # possible without changing the driver's code.
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


def NewBookmarkManager(backend, data):
    bookmark_manager_id = backend.next_key()

    bmm_kwargs = {}
    data.mark_item_as_read("initialBookmarks", recursive=True)
    bmm_kwargs["initial_bookmarks"] = data.get("initialBookmarks")
    if data.get("bookmarksSupplierRegistered"):
        bmm_kwargs["bookmarks_supplier"] = bookmarks_supplier(
            backend, bookmark_manager_id
        )
    if data.get("bookmarksConsumerRegistered"):
        bmm_kwargs["bookmarks_consumer"] = bookmarks_consumer(
            backend, bookmark_manager_id
        )

    with warning_check(
        neo4j.ExperimentalWarning,
        "The bookmark manager feature is experimental. It might be changed or "
        "removed any time even without prior notice."
    ):
        bookmark_manager = neo4j.GraphDatabase.bookmark_manager(
            **bmm_kwargs
        )
    backend.bookmark_managers[bookmark_manager_id] = bookmark_manager
    backend.send_response("BookmarkManager", {"id": bookmark_manager_id})


def BookmarkManagerClose(backend, data):
    bookmark_manager_id = data["id"]
    del backend.bookmark_managers[bookmark_manager_id]
    backend.send_response("BookmarkManager", {"id": bookmark_manager_id})


def bookmarks_supplier(backend, bookmark_manager_id):
    def supplier(database):
        key = backend.next_key()
        backend.send_response("BookmarksSupplierRequest", {
            "id": key,
            "bookmarkManagerId": bookmark_manager_id,
            "database": database
        })
        if not backend.process_request():
            # connection was closed before end of next message
            return []
        if key not in backend.bookmarks_supplies:
            raise RuntimeError(
                "Backend did not receive expected "
                "BookmarksSupplierCompleted message for id %s" % key
            )
        return backend.bookmarks_supplies.pop(key)

    return supplier


def BookmarksSupplierCompleted(backend, data):
    backend.bookmarks_supplies[data["requestId"]] = \
        neo4j.Bookmarks.from_raw_values(data["bookmarks"])


def bookmarks_consumer(backend, bookmark_manager_id):
    def consumer(database, bookmarks):
        key = backend.next_key()
        backend.send_response("BookmarksConsumerRequest", {
            "id": key,
            "bookmarkManagerId": bookmark_manager_id,
            "database": database,
            "bookmarks": list(bookmarks.raw_values)
        })
        if not backend.process_request():
            # connection was closed before end of next message
            return []
        if key not in backend.bookmarks_consumptions:
            raise RuntimeError(
                "Backend did not receive expected "
                "BookmarksConsumerCompleted message for id %s" % key
            )
        del backend.bookmarks_consumptions[key]

    return consumer


def BookmarksConsumerCompleted(backend, data):
    backend.bookmarks_consumptions[data["requestId"]] = True


def DriverClose(backend, data):
    key = data["driverId"]
    driver = backend.drivers[key]
    driver.close()
    backend.send_response("Driver", {"id": key})


def CheckDriverIsEncrypted(backend, data):
    key = data["driverId"]
    driver = backend.drivers[key]
    backend.send_response("DriverIsEncrypted", {
        "encrypted": driver.encrypted
    })


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
        raise ValueError("Unknown access mode:" + access_mode)
    config = {
        "default_access_mode": access_mode,
        "database": data["database"],
    }
    if data.get("bookmarks") is not None:
        config["bookmarks"] = neo4j.Bookmarks.from_raw_values(
            data["bookmarks"]
        )
    if data.get("bookmarkManagerId") is not None:
        config["bookmark_manager"] = backend.bookmark_managers[
            data["bookmarkManagerId"]
        ]
    for (conf_name, data_name) in (
        ("fetch_size", "fetchSize"),
        ("impersonated_user", "impersonatedUser"),
    ):
        if data_name in data:
            config[conf_name] = data[data_name]
    if "bookmark_manager" in config:
        with warning_check(
            neo4j.ExperimentalWarning,
            "The 'bookmark_manager' config key is experimental. It might be "
            "changed or removed any time even without prior notice."
        ):
            session = driver.session(**config)
    else:
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
    tx_kwargs = fromtestkit.to_tx_kwargs(data)
    tx = session.begin_transaction(**tx_kwargs)
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
    tx_kwargs = fromtestkit.to_tx_kwargs(data)

    @neo4j.unit_of_work(**tx_kwargs)
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
        session.execute_read(func)
    else:
        session.execute_write(func)
    backend.send_response("RetryableDone", {})


def RetryablePositive(backend, data):
    key = data["sessionId"]
    session_tracker = backend.sessions[key]
    session_tracker.state = '+'


def RetryableNegative(backend, data):
    key = data["sessionId"]
    session_tracker = backend.sessions[key]
    session_tracker.state = '-'
    session_tracker.error_id = data.get('errorId', '')


def SessionLastBookmarks(backend, data):
    key = data["sessionId"]
    session = backend.sessions[key].session
    bookmarks = session.last_bookmarks()
    backend.send_response("Bookmarks",
                                {"bookmarks": list(bookmarks.raw_values)})


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
    try:
        commit = tx.commit
    except AttributeError as e:
        raise MarkdAsDriverException(e)
        # raise DriverError("Type does not support commit %s" % type(tx))
    commit()
    backend.send_response("Transaction", {"id": key})


def TransactionRollback(backend, data):
    key = data["txId"]
    tx = backend.transactions[key]
    try:
        rollback = tx.rollback
    except AttributeError as e:
        raise MarkdAsDriverException(e)
        # raise DriverError("Type does not support rollback %s" % type(tx))
    rollback()
    backend.send_response("Transaction", {"id": key})


def TransactionClose(backend, data):
    key = data["txId"]
    tx = backend.transactions[key]
    try:
        close = tx.close
    except AttributeError as e:
        raise MarkdAsDriverException(e)
        # raise DriverError("Type does not support close %s" % type(tx))
    close()
    backend.send_response("Transaction", {"id": key})


def ResultNext(backend, data):
    result = backend.results[data["resultId"]]

    try:
        record = Util.next(Util.iter(result))
    except StopIteration:
        backend.send_response("NullRecord", {})
        return
    backend.send_response("Record", totestkit.record(record))


def ResultSingle(backend, data):
    result = backend.results[data["resultId"]]
    backend.send_response("Record", totestkit.record(
        result.single(strict=True)
    ))


def ResultSingleOptional(backend, data):
    result = backend.results[data["resultId"]]
    with warnings.catch_warnings(record=True) as warning_list:
        warnings.simplefilter("always")
        record = result.single(strict=False)
    if record:
        record = totestkit.record(record)
    backend.send_response("RecordOptional", {
        "record": record, "warnings": list(map(str, warning_list))
    })


def ResultPeek(backend, data):
    result = backend.results[data["resultId"]]
    record = result.peek()
    if record is not None:
        backend.send_response("Record", totestkit.record(record))
    else:
        backend.send_response("NullRecord", {})


def ResultList(backend, data):
    result = backend.results[data["resultId"]]
    records = Util.list(result)
    backend.send_response("RecordList", {
        "records": [totestkit.record(r) for r in records]
    })


def ResultConsume(backend, data):
    result = backend.results[data["resultId"]]
    summary = result.consume()
    from neo4j import ResultSummary
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


def ForcedRoutingTableUpdate(backend, data):
    driver_id = data["driverId"]
    driver = backend.drivers[driver_id]
    database = data["database"]
    bookmarks = data["bookmarks"]
    with driver._pool.refresh_lock:
        driver._pool.update_routing_table(
            database=database, imp_user=None, bookmarks=bookmarks
        )
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
