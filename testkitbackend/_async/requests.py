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


import datetime
import json
import re
import ssl
import warnings
from os import path

from freezegun import freeze_time

import neo4j
import neo4j.api
import neo4j.auth_management
from neo4j._async_compat.util import AsyncUtil
from neo4j.auth_management import (
    AsyncAuthManager,
    AsyncAuthManagers,
    ExpiringAuth,
)

from .. import (
    fromtestkit,
    test_subtest_skips,
    totestkit,
)
from ..exceptions import MarkdAsDriverException


class FrontendError(Exception):
    pass


def load_config():
    config_path = path.join(path.dirname(__file__), "..", "test_config.json")
    with open(config_path, "r") as fd:
        config = json.load(fd)
    skips = config["skips"]
    features = [k for k, v in config["features"].items() if v is True]
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


async def StartTest(backend, data):
    test_name = data["testName"]
    reason = _get_skip_reason(test_name)
    if reason is not None:
        if reason.startswith("test_subtest_skips."):
            await backend.send_response("RunSubTests", {})
        else:
            await backend.send_response("SkipTest", {"reason": reason})
    else:
        await backend.send_response("RunTest", {})


async def StartSubTest(backend, data):
    test_name = data["testName"]
    subtest_args = data["subtestArguments"]
    subtest_args.mark_all_as_read(recursive=True)
    reason = _get_skip_reason(test_name)
    assert reason and reason.startswith("test_subtest_skips.") or print(reason)
    func = getattr(test_subtest_skips, reason[19:])
    reason = func(**subtest_args)
    if reason is not None:
        await backend.send_response("SkipTest", {"reason": reason})
    else:
        await backend.send_response("RunTest", {})


async def GetFeatures(backend, data):
    await backend.send_response("FeatureList", {"features": FEATURES})


async def NewDriver(backend, data):
    auth = fromtestkit.to_auth_token(data, "authorizationToken")
    if auth is None and data.get("authTokenManagerId") is not None:
        auth = backend.auth_token_managers[data["authTokenManagerId"]]
    else:
        data.mark_item_as_read_if_equals("authTokenManagerId", None)
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
        ("livenessCheckTimeoutMs", "liveness_check_timeout"),
    ):
        if data.get(timeout_testkit) is not None:
            kwargs[timeout_driver] = data[timeout_testkit] / 1000
    for k in ("sessionConnectionTimeoutMs", "updateRoutingTableTimeoutMs"):
        if k in data:
            data.mark_item_as_read_if_equals(k, None)
    for (conf_name, data_name) in (
        ("max_connection_pool_size", "maxConnectionPoolSize"),
        ("fetch_size", "fetchSize"),
        ("telemetry_disabled", "telemetryDisabled")
    ):
        if data.get(data_name):
            kwargs[conf_name] = data[data_name]
    for (conf_name, data_name) in (
        ("encrypted", "encrypted"),
    ):
        if data_name in data:
            kwargs[conf_name] = data[data_name]
    if "trustedCertificates" in data:
        if data["trustedCertificates"] is None:
            kwargs["trusted_certificates"] = neo4j.TrustSystemCAs()
        elif not data["trustedCertificates"]:
            kwargs["trusted_certificates"] = neo4j.TrustAll()
        else:
            cert_paths = ("/usr/local/share/custom-ca-certificates/" + cert
                          for cert in data["trustedCertificates"])
            kwargs["trusted_certificates"] = neo4j.TrustCustomCAs(*cert_paths)
    fromtestkit.set_notifications_config(kwargs, data)

    driver = neo4j.AsyncGraphDatabase.driver(
        data["uri"], auth=auth, user_agent=data["userAgent"], **kwargs,
    )
    key = backend.next_key()
    backend.drivers[key] = driver
    await backend.send_response("Driver", {"id": key})


async def NewAuthTokenManager(backend, data):
    auth_token_manager_id = backend.next_key()

    class TestKitAuthManager(AsyncAuthManager):
        async def get_auth(self):
            key = backend.next_key()
            await backend.send_response("AuthTokenManagerGetAuthRequest", {
                "id": key,
                "authTokenManagerId": auth_token_manager_id,
            })
            if not await backend.process_request():
                # connection was closed before end of next message
                return None
            if key not in backend.auth_token_supplies:
                raise RuntimeError(
                    "Backend did not receive expected "
                    f"AuthTokenManagerGetAuthCompleted message for id {key}"
                )
            return backend.auth_token_supplies.pop(key)

        async def handle_security_exception(self, auth, error):
            key = backend.next_key()
            await backend.send_response(
                "AuthTokenManagerHandleSecurityExceptionRequest", {
                    "id": key,
                    "authTokenManagerId": auth_token_manager_id,
                    "auth": totestkit.auth_token(auth),
                    "errorCode": error.code,
                }
            )
            if not await backend.process_request():
                # connection was closed before end of next message
                return None
            if key not in backend.auth_token_on_expiration_supplies:
                raise RuntimeError(
                    "Backend did not receive expected "
                    "AuthTokenManagerHandleSecurityExceptionCompleted message "
                    f"for id {key}"
                )
            handled = backend.auth_token_on_expiration_supplies.pop(key)
            return handled

    auth_manager = TestKitAuthManager()
    backend.auth_token_managers[auth_token_manager_id] = auth_manager
    await backend.send_response(
        "AuthTokenManager", {"id": auth_token_manager_id}
    )


async def AuthTokenManagerGetAuthCompleted(backend, data):
    auth_token = fromtestkit.to_auth_token(data, "auth")

    backend.auth_token_supplies[data["requestId"]] = auth_token


async def AuthTokenManagerHandleSecurityExceptionCompleted(backend, data):
    handled = data["handled"]
    backend.auth_token_on_expiration_supplies[data["requestId"]] = handled


async def AuthTokenManagerClose(backend, data):
    auth_token_manager_id = data["id"]
    del backend.auth_token_managers[auth_token_manager_id]
    await backend.send_response(
        "AuthTokenManager", {"id": auth_token_manager_id}
    )


async def NewBasicAuthTokenManager(backend, data):
    auth_token_manager_id = backend.next_key()

    async def auth_token_provider():
        key = backend.next_key()
        await backend.send_response(
            "BasicAuthTokenProviderRequest",
            {
                "id": key,
                "basicAuthTokenManagerId": auth_token_manager_id,
            }
        )
        if not await backend.process_request():
            # connection was closed before end of next message
            return None
        if key not in backend.basic_auth_token_supplies:
            raise RuntimeError(
                "Backend did not receive expected "
                "BasicAuthTokenManagerCompleted message for id "
                f"{key}"
            )
        return backend.basic_auth_token_supplies.pop(key)

    auth_manager = AsyncAuthManagers.basic(auth_token_provider)
    backend.auth_token_managers[auth_token_manager_id] = auth_manager
    await backend.send_response(
        "BasicAuthTokenManager", {"id": auth_token_manager_id}
    )


async def BasicAuthTokenProviderCompleted(backend, data):
    auth = fromtestkit.to_auth_token(data, "auth")
    backend.basic_auth_token_supplies[data["requestId"]] = auth


async def NewBearerAuthTokenManager(backend, data):
    auth_token_manager_id = backend.next_key()

    async def auth_token_provider():
        key = backend.next_key()
        await backend.send_response(
            "BearerAuthTokenProviderRequest",
            {
                "id": key,
                "bearerAuthTokenManagerId": auth_token_manager_id,
            }
        )
        if not await backend.process_request():
            # connection was closed before end of next message
            return neo4j.auth_management.ExpiringAuth(None, None)
        if key not in backend.expiring_auth_token_supplies:
            raise RuntimeError(
                "Backend did not receive expected "
                "BearerAuthTokenManagerCompleted message for id "
                f"{key}"
            )
        return backend.expiring_auth_token_supplies.pop(key)

    auth_manager = AsyncAuthManagers.bearer(auth_token_provider)
    backend.auth_token_managers[auth_token_manager_id] = auth_manager
    await backend.send_response(
        "BearerAuthTokenManager", {"id": auth_token_manager_id}
    )


async def BearerAuthTokenProviderCompleted(backend, data):
    temp_auth_data = data["auth"]
    temp_auth_data.mark_item_as_read_if_equals("name",
                                               "AuthTokenAndExpiration")
    temp_auth_data = temp_auth_data["data"]
    auth_token = fromtestkit.to_auth_token(temp_auth_data, "auth")
    expiring_auth = ExpiringAuth(auth_token)
    if temp_auth_data["expiresInMs"] is not None:
        expires_in = temp_auth_data["expiresInMs"] / 1000
        expiring_auth = expiring_auth.expires_in(expires_in)

    backend.expiring_auth_token_supplies[data["requestId"]] = expiring_auth


async def VerifyConnectivity(backend, data):
    driver_id = data["driverId"]
    driver = backend.drivers[driver_id]
    await driver.verify_connectivity()
    await backend.send_response("Driver", {"id": driver_id})


async def GetServerInfo(backend, data):
    driver_id = data["driverId"]
    driver = backend.drivers[driver_id]
    server_info = await driver.get_server_info()
    await backend.send_response("ServerInfo", {
        "address": ":".join(map(str, server_info.address)),
        "agent": server_info.agent,
        "protocolVersion": ".".join(map(str, server_info.protocol_version)),
    })


async def CheckMultiDBSupport(backend, data):
    driver_id = data["driverId"]
    driver = backend.drivers[driver_id]
    available = await driver.supports_multi_db()
    await backend.send_response("MultiDBSupport", {
        "id": backend.next_key(), "available": available
    })


async def VerifyAuthentication(backend, data):
    driver_id = data["driverId"]
    driver = backend.drivers[driver_id]
    auth = fromtestkit.to_auth_token(data, "authorizationToken")
    authenticated = await driver.verify_authentication(auth=auth)
    await backend.send_response("DriverIsAuthenticated", {
        "id": backend.next_key(), "authenticated": authenticated
    })


async def CheckSessionAuthSupport(backend, data):
    driver_id = data["driverId"]
    driver = backend.drivers[driver_id]
    available = await driver.supports_session_auth()
    await backend.send_response("SessionAuthSupport", {
        "id": backend.next_key(), "available": available
    })


async def ExecuteQuery(backend, data):
    driver = backend.drivers[data["driverId"]]
    cypher, params = fromtestkit.to_cypher_and_params(data)
    config = data.get("config", {})
    kwargs = {}
    for config_key, kwargs_key in (
        ("database", "database_"),
        ("routing", "routing_"),
        ("impersonatedUser", "impersonated_user_"),
    ):
        value = config.get(config_key, None)
        if value is not None:
            kwargs[kwargs_key] = value
    bookmark_manager_id = config.get("bookmarkManagerId")
    if bookmark_manager_id is not None:
        if bookmark_manager_id == -1:
            kwargs["bookmark_manager_"] = None
        else:
            bookmark_manager = backend.bookmark_managers[bookmark_manager_id]
            kwargs["bookmark_manager_"] = bookmark_manager
    if "authorizationToken" in config:
        kwargs["auth_"] = fromtestkit.to_auth_token(config,
                                                    "authorizationToken")

    eager_result = await driver.execute_query(cypher, params, **kwargs)
    await backend.send_response("EagerResult", {
        "keys": eager_result.keys,
        "records": list(map(totestkit.record, eager_result.records)),
        "summary": totestkit.summary(eager_result.summary),
    })


def resolution_func(backend, custom_resolver=False, custom_dns_resolver=False):
    # This solution (putting custom resolution together with DNS resolution
    # into one function) only works because the Python driver calls the custom
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


async def NewBookmarkManager(backend, data):
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

    bookmark_manager = neo4j.AsyncGraphDatabase.bookmark_manager(**bmm_kwargs)
    backend.bookmark_managers[bookmark_manager_id] = bookmark_manager
    await backend.send_response("BookmarkManager", {"id": bookmark_manager_id})


async def BookmarkManagerClose(backend, data):
    bookmark_manager_id = data["id"]
    del backend.bookmark_managers[bookmark_manager_id]
    await backend.send_response("BookmarkManager", {"id": bookmark_manager_id})


def bookmarks_supplier(backend, bookmark_manager_id):
    async def supplier():
        key = backend.next_key()
        await backend.send_response("BookmarksSupplierRequest", {
            "id": key,
            "bookmarkManagerId": bookmark_manager_id,
        })
        if not await backend.process_request():
            # connection was closed before end of next message
            return []
        if key not in backend.bookmarks_supplies:
            raise RuntimeError(
                "Backend did not receive expected "
                "BookmarksSupplierCompleted message for id %s" % key
            )
        return backend.bookmarks_supplies.pop(key)

    return supplier


async def BookmarksSupplierCompleted(backend, data):
    backend.bookmarks_supplies[data["requestId"]] = \
        neo4j.Bookmarks.from_raw_values(data["bookmarks"])


def bookmarks_consumer(backend, bookmark_manager_id):
    async def consumer(bookmarks):
        key = backend.next_key()
        await backend.send_response("BookmarksConsumerRequest", {
            "id": key,
            "bookmarkManagerId": bookmark_manager_id,
            "bookmarks": list(bookmarks.raw_values)
        })
        if not await backend.process_request():
            # connection was closed before end of next message
            return []
        if key not in backend.bookmarks_consumptions:
            raise RuntimeError(
                "Backend did not receive expected "
                "BookmarksConsumerCompleted message for id %s" % key
            )
        del backend.bookmarks_consumptions[key]

    return consumer


async def BookmarksConsumerCompleted(backend, data):
    backend.bookmarks_consumptions[data["requestId"]] = True


async def DriverClose(backend, data):
    key = data["driverId"]
    driver = backend.drivers[key]
    await driver.close()
    await backend.send_response("Driver", {"id": key})


async def CheckDriverIsEncrypted(backend, data):
    key = data["driverId"]
    driver = backend.drivers[key]
    await backend.send_response("DriverIsEncrypted", {
        "encrypted": driver.encrypted
    })


class SessionTracker:
    """ Keeps some extra state about the tracked session
    """

    def __init__(self, session):
        self.session = session
        self.state = ""
        self.error_id = ""


async def NewSession(backend, data):
    driver = backend.drivers[data["driverId"]]
    config = {
        "database": data["database"],
    }
    access_mode = data["accessMode"]
    if access_mode is not None:
        if access_mode == "r":
            config["default_access_mode"] = neo4j.READ_ACCESS
        elif access_mode == "w":
            config["default_access_mode"] = neo4j.WRITE_ACCESS
        else:
            raise ValueError("Unknown access mode:" + access_mode)
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
    if data.get("authorizationToken"):
        config["auth"] = fromtestkit.to_auth_token(data, "authorizationToken")
    fromtestkit.set_notifications_config(config, data)
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
    tx_kwargs = fromtestkit.to_tx_kwargs(data)
    tx = await session.begin_transaction(**tx_kwargs)
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
    tx_kwargs = fromtestkit.to_tx_kwargs(data)

    @neo4j.unit_of_work(**tx_kwargs)
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
        await session.execute_read(func)
    else:
        await session.execute_write(func)
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
    bookmarks = await session.last_bookmarks()
    await backend.send_response("Bookmarks",
                                {"bookmarks": list(bookmarks.raw_values)})


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
    try:
        commit = tx.commit
    except AttributeError as e:
        raise MarkdAsDriverException(e)
        # raise DriverError("Type does not support commit %s" % type(tx))
    await commit()
    await backend.send_response("Transaction", {"id": key})


async def TransactionRollback(backend, data):
    key = data["txId"]
    tx = backend.transactions[key]
    try:
        rollback = tx.rollback
    except AttributeError as e:
        raise MarkdAsDriverException(e)
        # raise DriverError("Type does not support rollback %s" % type(tx))
    await rollback()
    await backend.send_response("Transaction", {"id": key})


async def TransactionClose(backend, data):
    key = data["txId"]
    tx = backend.transactions[key]
    try:
        close = tx.close
    except AttributeError as e:
        raise MarkdAsDriverException(e)
        # raise DriverError("Type does not support close %s" % type(tx))
    await close()
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
    await backend.send_response("Record", totestkit.record(
        await result.single(strict=True)
    ))


async def ResultSingleOptional(backend, data):
    result = backend.results[data["resultId"]]
    with warnings.catch_warnings(record=True) as warning_list:
        warnings.simplefilter("always")
        record = await result.single(strict=False)
    if record:
        record = totestkit.record(record)
    await backend.send_response("RecordOptional", {
        "record": record, "warnings": list(map(str, warning_list))
    })


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
    assert isinstance(summary, neo4j.ResultSummary)
    await backend.send_response("Summary", totestkit.summary(summary))


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


async def FakeTimeInstall(backend, _data):
    assert backend.fake_time is None
    assert backend.fake_time_ticker is None

    backend.fake_time = freeze_time()
    backend.fake_time_ticker = backend.fake_time.start()
    await backend.send_response("FakeTimeAck", {})


async def FakeTimeTick(backend, data):
    assert backend.fake_time is not None
    assert backend.fake_time_ticker is not None

    increment_ms = data["incrementMs"]
    delta = datetime.timedelta(milliseconds=increment_ms)
    backend.fake_time_ticker.tick(delta=delta)
    await backend.send_response("FakeTimeAck", {})


async def FakeTimeUninstall(backend, _data):
    assert backend.fake_time is not None
    assert backend.fake_time_ticker is not None

    backend.fake_time.stop()
    backend.fake_time_ticker = None
    backend.fake_time = None
    await backend.send_response("FakeTimeAck", {})
