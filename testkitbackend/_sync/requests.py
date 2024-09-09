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


import datetime
import json
import re
import ssl
import typing as t
import warnings
from os import path

from freezegun import freeze_time

import neo4j
import neo4j.api
import neo4j.auth_management
from neo4j._async_compat.util import Util
from neo4j._auth_management import ClientCertificate
from neo4j.auth_management import (
    AuthManager,
    AuthManagers,
    ClientCertificateProvider,
    ExpiringAuth,
)

from .. import (
    fromtestkit,
    test_subtest_skips,
    totestkit,
)
from .._warning_check import warnings_check
from ..exceptions import MarkdAsDriverError


class FrontendError(Exception):
    pass


def load_config():
    config_path = path.join(path.dirname(__file__), "..", "test_config.json")
    with open(config_path, encoding="utf-8") as fd:
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
    return None


def start_test(backend, data):
    test_name = data["testName"]
    reason = _get_skip_reason(test_name)
    if reason is not None:
        if reason.startswith("test_subtest_skips."):
            backend.send_response("RunSubTests", {})
        else:
            backend.send_response("SkipTest", {"reason": reason})
    else:
        backend.send_response("RunTest", {})


def start_sub_test(backend, data):
    test_name = data["testName"]
    subtest_args = data["subtestArguments"]
    subtest_args.mark_all_as_read(recursive=True)
    reason = _get_skip_reason(test_name)
    assert (reason and reason.startswith("test_subtest_skips.")) or print(
        reason
    )
    func = getattr(test_subtest_skips, reason[19:])
    reason = func(**subtest_args)
    if reason is not None:
        backend.send_response("SkipTest", {"reason": reason})
    else:
        backend.send_response("RunTest", {})


def get_features(backend, data):
    backend.send_response("FeatureList", {"features": FEATURES})


def new_driver(backend, data):
    expected_warnings = []

    auth = fromtestkit.to_auth_token(data, "authorizationToken")
    if auth is None and data.get("authTokenManagerId") is not None:
        auth = backend.auth_token_managers[data["authTokenManagerId"]]
    else:
        data.mark_item_as_read_if_equals("authTokenManagerId", None)
    kwargs = {}
    client_cert_provider_id = data.get("clientCertificateProviderId")
    if client_cert_provider_id is not None:
        kwargs["client_certificate"] = backend.client_cert_providers[
            client_cert_provider_id
        ]
        data.mark_item_as_read_if_equals("clientCertificate", None)
        expected_warnings.append(
            (neo4j.PreviewWarning, r"Mutual TLS is a preview feature\.")
        )
    else:
        client_cert = fromtestkit.to_client_cert(data, "clientCertificate")
        if client_cert is not None:
            kwargs["client_certificate"] = client_cert
            expected_warnings.append(
                (neo4j.PreviewWarning, r"Mutual TLS is a preview feature\.")
            )
    if data["resolverRegistered"] or data["domainNameResolverRegistered"]:
        kwargs["resolver"] = resolution_func(
            backend,
            data["resolverRegistered"],
            data["domainNameResolverRegistered"],
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
    for conf_name, data_name in (
        ("max_connection_pool_size", "maxConnectionPoolSize"),
        ("fetch_size", "fetchSize"),
        ("telemetry_disabled", "telemetryDisabled"),
    ):
        if data.get(data_name):
            kwargs[conf_name] = data[data_name]
    for conf_name, data_name in (("encrypted", "encrypted"),):
        if data_name in data:
            kwargs[conf_name] = data[data_name]
    if "trustedCertificates" in data:
        if data["trustedCertificates"] is None:
            kwargs["trusted_certificates"] = neo4j.TrustSystemCAs()
        elif not data["trustedCertificates"]:
            kwargs["trusted_certificates"] = neo4j.TrustAll()
        else:
            cert_paths = (
                "/usr/local/share/custom-ca-certificates/" + cert
                for cert in data["trustedCertificates"]
            )
            kwargs["trusted_certificates"] = neo4j.TrustCustomCAs(*cert_paths)
    fromtestkit.set_notifications_config(kwargs, data)

    expected_warnings.append(
        (
            neo4j.PreviewWarning,
            r"notification warnings are a preview feature\.",
        )
    )
    with warnings_check(expected_warnings):
        driver = neo4j.GraphDatabase.driver(
            data["uri"],
            auth=auth,
            user_agent=data["userAgent"],
            warn_notification_severity=neo4j.NotificationMinimumSeverity.OFF,
            **kwargs,
        )
    key = backend.next_key()
    backend.drivers[key] = driver
    backend.send_response("Driver", {"id": key})


def new_auth_token_manager(backend, data):
    auth_token_manager_id = backend.next_key()

    class TestKitAuthManager(AuthManager):
        def get_auth(self):
            key = backend.next_key()
            backend.send_response(
                "AuthTokenManagerGetAuthRequest",
                {
                    "id": key,
                    "authTokenManagerId": auth_token_manager_id,
                },
            )
            if not backend.process_request():
                # connection was closed before end of next message
                return None
            if key not in backend.auth_token_supplies:
                raise RuntimeError(
                    "Backend did not receive expected "
                    f"AuthTokenManagerGetAuthCompleted message for id {key}"
                )
            return backend.auth_token_supplies.pop(key)

        def handle_security_exception(self, auth, error):
            key = backend.next_key()
            backend.send_response(
                "AuthTokenManagerHandleSecurityExceptionRequest",
                {
                    "id": key,
                    "authTokenManagerId": auth_token_manager_id,
                    "auth": totestkit.auth_token(auth),
                    "errorCode": error.code,
                },
            )
            if not backend.process_request():
                # connection was closed before end of next message
                return None
            if key not in backend.auth_token_on_expiration_supplies:
                raise RuntimeError(
                    "Backend did not receive expected "
                    "AuthTokenManagerHandleSecurityExceptionCompleted message "
                    f"for id {key}"
                )
            return backend.auth_token_on_expiration_supplies.pop(key)

    auth_manager = TestKitAuthManager()
    backend.auth_token_managers[auth_token_manager_id] = auth_manager
    backend.send_response(
        "AuthTokenManager", {"id": auth_token_manager_id}
    )


def auth_token_manager_get_auth_completed(backend, data):
    auth_token = fromtestkit.to_auth_token(data, "auth")

    backend.auth_token_supplies[data["requestId"]] = auth_token


def auth_token_manager_handle_security_exception_completed(
    backend, data
):
    handled = data["handled"]
    backend.auth_token_on_expiration_supplies[data["requestId"]] = handled


def auth_token_manager_close(backend, data):
    auth_token_manager_id = data["id"]
    del backend.auth_token_managers[auth_token_manager_id]
    backend.send_response(
        "AuthTokenManager", {"id": auth_token_manager_id}
    )


def new_basic_auth_token_manager(backend, data):
    auth_token_manager_id = backend.next_key()

    def auth_token_provider():
        key = backend.next_key()
        backend.send_response(
            "BasicAuthTokenProviderRequest",
            {
                "id": key,
                "basicAuthTokenManagerId": auth_token_manager_id,
            },
        )
        if not backend.process_request():
            # connection was closed before end of next message
            return None
        if key not in backend.basic_auth_token_supplies:
            raise RuntimeError(
                "Backend did not receive expected "
                "BasicAuthTokenManagerCompleted message for id "
                f"{key}"
            )
        return backend.basic_auth_token_supplies.pop(key)

    auth_manager = AuthManagers.basic(auth_token_provider)
    backend.auth_token_managers[auth_token_manager_id] = auth_manager
    backend.send_response(
        "BasicAuthTokenManager", {"id": auth_token_manager_id}
    )


def basic_auth_token_provider_completed(backend, data):
    auth = fromtestkit.to_auth_token(data, "auth")
    backend.basic_auth_token_supplies[data["requestId"]] = auth


def new_bearer_auth_token_manager(backend, data):
    auth_token_manager_id = backend.next_key()

    def auth_token_provider():
        key = backend.next_key()
        backend.send_response(
            "BearerAuthTokenProviderRequest",
            {
                "id": key,
                "bearerAuthTokenManagerId": auth_token_manager_id,
            },
        )
        if not backend.process_request():
            # connection was closed before end of next message
            return neo4j.auth_management.ExpiringAuth(None, None)
        if key not in backend.expiring_auth_token_supplies:
            raise RuntimeError(
                "Backend did not receive expected "
                "BearerAuthTokenManagerCompleted message for id "
                f"{key}"
            )
        return backend.expiring_auth_token_supplies.pop(key)

    auth_manager = AuthManagers.bearer(auth_token_provider)
    backend.auth_token_managers[auth_token_manager_id] = auth_manager
    backend.send_response(
        "BearerAuthTokenManager", {"id": auth_token_manager_id}
    )


def bearer_auth_token_provider_completed(backend, data):
    temp_auth_data = data["auth"]
    temp_auth_data.mark_item_as_read_if_equals(
        "name", "AuthTokenAndExpiration"
    )
    temp_auth_data = temp_auth_data["data"]
    auth_token = fromtestkit.to_auth_token(temp_auth_data, "auth")
    expiring_auth = ExpiringAuth(auth_token)
    if temp_auth_data["expiresInMs"] is not None:
        expires_in = temp_auth_data["expiresInMs"] / 1000
        expiring_auth = expiring_auth.expires_in(expires_in)

    backend.expiring_auth_token_supplies[data["requestId"]] = expiring_auth


class TestKitClientCertificateProvider(ClientCertificateProvider):
    def __init__(self, backend):
        self.id = backend.next_key()
        self._backend = backend

    def get_certificate(self) -> t.Optional[ClientCertificate]:
        request_id = self._backend.next_key()
        self._backend.send_response(
            "ClientCertificateProviderRequest",
            {
                "id": request_id,
                "clientCertificateProviderId": self.id,
            },
        )
        if not self._backend.process_request():
            # connection was closed before end of next message
            return None
        if request_id not in self._backend.client_cert_supplies:
            raise RuntimeError(
                "Backend did not receive expected "
                "ClientCertificateProviderCompleted message for id "
                f"{request_id}"
            )
        return self._backend.client_cert_supplies.pop(request_id)


def new_client_certificate_provider(backend, data):
    provider = TestKitClientCertificateProvider(backend)
    backend.client_cert_providers[provider.id] = provider
    backend.send_response(
        "ClientCertificateProvider", {"id": provider.id}
    )


def client_certificate_provider_close(backend, data):
    client_cert_provider_id = data["id"]
    del backend.client_cert_providers[client_cert_provider_id]
    backend.send_response(
        "ClientCertificateProvider", {"id": client_cert_provider_id}
    )


def client_certificate_provider_completed(backend, data):
    has_update = data["hasUpdate"]
    request_id = data["requestId"]
    if not has_update:
        data.mark_item_as_read("clientCertificate", recursive=True)
        backend.client_cert_supplies[request_id] = None
        return
    client_cert = fromtestkit.to_client_cert(data, "clientCertificate")
    backend.client_cert_supplies[request_id] = client_cert


def verify_connectivity(backend, data):
    driver_id = data["driverId"]
    driver = backend.drivers[driver_id]
    driver.verify_connectivity()
    backend.send_response("Driver", {"id": driver_id})


def get_server_info(backend, data):
    driver_id = data["driverId"]
    driver = backend.drivers[driver_id]
    server_info = driver.get_server_info()
    backend.send_response(
        "ServerInfo",
        {
            "address": ":".join(map(str, server_info.address)),
            "agent": server_info.agent,
            "protocolVersion": ".".join(
                map(str, server_info.protocol_version)
            ),
        },
    )


def check_multi_db_support(backend, data):
    driver_id = data["driverId"]
    driver = backend.drivers[driver_id]
    available = driver.supports_multi_db()
    backend.send_response(
        "MultiDBSupport", {"id": backend.next_key(), "available": available}
    )


def verify_authentication(backend, data):
    driver_id = data["driverId"]
    driver = backend.drivers[driver_id]
    auth = fromtestkit.to_auth_token(data, "authorizationToken")
    authenticated = driver.verify_authentication(auth=auth)
    backend.send_response(
        "DriverIsAuthenticated",
        {"id": backend.next_key(), "authenticated": authenticated},
    )


def check_session_auth_support(backend, data):
    driver_id = data["driverId"]
    driver = backend.drivers[driver_id]
    available = driver.supports_session_auth()
    backend.send_response(
        "SessionAuthSupport",
        {"id": backend.next_key(), "available": available},
    )


def execute_query(backend, data):
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
    tx_kwargs = fromtestkit.to_tx_kwargs(config)
    query = neo4j.Query(cypher, **tx_kwargs) if tx_kwargs else cypher
    bookmark_manager_id = config.get("bookmarkManagerId")
    if bookmark_manager_id is not None:
        if bookmark_manager_id == -1:
            kwargs["bookmark_manager_"] = None
        else:
            bookmark_manager = backend.bookmark_managers[bookmark_manager_id]
            kwargs["bookmark_manager_"] = bookmark_manager
    if "authorizationToken" in config:
        kwargs["auth_"] = fromtestkit.to_auth_token(
            config, "authorizationToken"
        )

    eager_result = driver.execute_query(query, params, **kwargs)
    backend.send_response(
        "EagerResult",
        {
            "keys": eager_result.keys,
            "records": list(map(totestkit.record, eager_result.records)),
            "summary": totestkit.summary(eager_result.summary),
        },
    )


def resolution_func(backend, custom_resolver=False, custom_dns_resolver=False):
    # This solution (putting custom resolution together with DNS resolution
    # into one function) only works because the Python driver calls the custom
    # resolver function for every connection, which is not true for all
    # drivers. Properly exposing a way to change the DNS lookup behavior is not
    # possible without changing the driver's code.
    assert custom_resolver or custom_dns_resolver

    def resolve(address):
        addresses = [":".join(map(str, address))]
        if custom_resolver:
            key = backend.next_key()
            backend.send_response(
                "ResolverResolutionRequired",
                {"id": key, "address": addresses[0]},
            )
            if not backend.process_request():
                # connection was closed before end of next message
                return []
            if key not in backend.custom_resolutions:
                raise RuntimeError(
                    "Backend did not receive expected "
                    f"ResolverResolutionCompleted message for id {key}"
                )
            addresses = backend.custom_resolutions.pop(key)
        if custom_dns_resolver:
            dns_resolved_addresses = []
            for address in addresses:
                key = backend.next_key()
                address = address.rsplit(":", 1)
                backend.send_response(
                    "DomainNameResolutionRequired",
                    {"id": key, "name": address[0]},
                )
                if not backend.process_request():
                    # connection was closed before end of next message
                    return []
                if key not in backend.dns_resolutions:
                    raise RuntimeError(
                        "Backend did not receive expected "
                        f"DomainNameResolutionCompleted message for id {key}"
                    )
                dns_resolved_addresses += list(
                    map(
                        lambda a: ":".join((a, *address[1:])),
                        backend.dns_resolutions.pop(key),
                    )
                )

            addresses = dns_resolved_addresses

        return list(map(neo4j.Address.parse, addresses))

    return resolve


def resolver_resolution_completed(backend, data):
    backend.custom_resolutions[data["requestId"]] = data["addresses"]


def domain_name_resolution_completed(backend, data):
    backend.dns_resolutions[data["requestId"]] = data["addresses"]


def new_bookmark_manager(backend, data):
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

    bookmark_manager = neo4j.GraphDatabase.bookmark_manager(**bmm_kwargs)
    backend.bookmark_managers[bookmark_manager_id] = bookmark_manager
    backend.send_response("BookmarkManager", {"id": bookmark_manager_id})


def bookmark_manager_close(backend, data):
    bookmark_manager_id = data["id"]
    del backend.bookmark_managers[bookmark_manager_id]
    backend.send_response("BookmarkManager", {"id": bookmark_manager_id})


def bookmarks_supplier(backend, bookmark_manager_id):
    def supplier():
        key = backend.next_key()
        backend.send_response(
            "BookmarksSupplierRequest",
            {
                "id": key,
                "bookmarkManagerId": bookmark_manager_id,
            },
        )
        if not backend.process_request():
            # connection was closed before end of next message
            return []
        if key not in backend.bookmarks_supplies:
            raise RuntimeError(
                "Backend did not receive expected "
                f"BookmarksSupplierCompleted message for id {key}"
            )
        return backend.bookmarks_supplies.pop(key)

    return supplier


def bookmarks_supplier_completed(backend, data):
    backend.bookmarks_supplies[data["requestId"]] = (
        neo4j.Bookmarks.from_raw_values(data["bookmarks"])
    )


def bookmarks_consumer(backend, bookmark_manager_id):
    def consumer(bookmarks):
        key = backend.next_key()
        backend.send_response(
            "BookmarksConsumerRequest",
            {
                "id": key,
                "bookmarkManagerId": bookmark_manager_id,
                "bookmarks": list(bookmarks.raw_values),
            },
        )
        if not backend.process_request():
            # connection was closed before end of next message
            return
        if key not in backend.bookmarks_consumptions:
            raise RuntimeError(
                "Backend did not receive expected "
                f"BookmarksConsumerCompleted message for id {key}"
            )
        del backend.bookmarks_consumptions[key]

    return consumer


def bookmarks_consumer_completed(backend, data):
    backend.bookmarks_consumptions[data["requestId"]] = True


def driver_close(backend, data):
    key = data["driverId"]
    driver = backend.drivers[key]
    driver.close()
    backend.send_response("Driver", {"id": key})


def check_driver_is_encrypted(backend, data):
    key = data["driverId"]
    driver = backend.drivers[key]
    backend.send_response(
        "DriverIsEncrypted", {"encrypted": driver.encrypted}
    )


class SessionTracker:
    """Keeps some extra state about the tracked session."""

    def __init__(self, session):
        self.session = session
        self.state = ""
        self.error_id = ""


def new_session(backend, data):
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
    for conf_name, data_name in (
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
    backend.send_response("Session", {"id": key})


def session_run(backend, data):
    session = backend.sessions[data["sessionId"]].session
    query, params = fromtestkit.to_query_and_params(data)
    result = session.run(query, parameters=params)
    key = backend.next_key()
    backend.results[key] = result
    backend.send_response("Result", {"id": key, "keys": result.keys()})


def session_close(backend, data):
    key = data["sessionId"]
    session = backend.sessions[key].session
    session.close()
    del backend.sessions[key]
    backend.send_response("Session", {"id": key})


def session_begin_transaction(backend, data):
    key = data["sessionId"]
    session = backend.sessions[key].session
    tx_kwargs = fromtestkit.to_tx_kwargs(data)
    tx = session.begin_transaction(**tx_kwargs)
    key = backend.next_key()
    backend.transactions[key] = tx
    backend.send_response("Transaction", {"id": key})


def session_read_transaction(backend, data):
    transaction_func(backend, data, True)


def session_write_transaction(backend, data):
    transaction_func(backend, data, False)


def transaction_func(backend, data, is_read):
    key = data["sessionId"]
    session_tracker = backend.sessions[key]
    session = session_tracker.session
    tx_kwargs = fromtestkit.to_tx_kwargs(data)

    @neo4j.unit_of_work(**tx_kwargs)
    def func(tx):
        txkey = backend.next_key()
        backend.transactions[txkey] = tx
        session_tracker.state = ""
        backend.send_response("RetryableTry", {"id": txkey})

        cont = True
        while cont:
            cont = backend.process_request()
            if session_tracker.state == "+":
                cont = False
            elif session_tracker.state == "-":
                if session_tracker.error_id:
                    raise backend.errors[session_tracker.error_id]
                else:
                    raise FrontendError("Client said no")

    if is_read:
        session.execute_read(func)
    else:
        session.execute_write(func)
    backend.send_response("RetryableDone", {})


def retryable_positive(backend, data):
    key = data["sessionId"]
    session_tracker = backend.sessions[key]
    session_tracker.state = "+"


def retryable_negative(backend, data):
    key = data["sessionId"]
    session_tracker = backend.sessions[key]
    session_tracker.state = "-"
    session_tracker.error_id = data.get("errorId", "")


def session_last_bookmarks(backend, data):
    key = data["sessionId"]
    session = backend.sessions[key].session
    bookmarks = session.last_bookmarks()
    backend.send_response(
        "Bookmarks", {"bookmarks": list(bookmarks.raw_values)}
    )


def transaction_run(backend, data):
    key = data["txId"]
    tx = backend.transactions[key]
    cypher, params = fromtestkit.to_cypher_and_params(data)
    result = tx.run(cypher, parameters=params)
    key = backend.next_key()
    backend.results[key] = result
    backend.send_response("Result", {"id": key, "keys": result.keys()})


def transaction_commit(backend, data):
    key = data["txId"]
    tx = backend.transactions[key]
    try:
        commit = tx.commit
    except AttributeError as e:
        raise MarkdAsDriverError(e) from None
        # raise DriverError("Type does not support commit %s" % type(tx))
    commit()
    backend.send_response("Transaction", {"id": key})


def transaction_rollback(backend, data):
    key = data["txId"]
    tx = backend.transactions[key]
    try:
        rollback = tx.rollback
    except AttributeError as e:
        raise MarkdAsDriverError(e) from None
        # raise DriverError("Type does not support rollback %s" % type(tx))
    rollback()
    backend.send_response("Transaction", {"id": key})


def transaction_close(backend, data):
    key = data["txId"]
    tx = backend.transactions[key]
    try:
        close = tx.close
    except AttributeError as e:
        raise MarkdAsDriverError(e) from None
        # raise DriverError("Type does not support close %s" % type(tx))
    close()
    backend.send_response("Transaction", {"id": key})


def result_next(backend, data):
    result = backend.results[data["resultId"]]

    try:
        record = Util.next(Util.iter(result))
    except StopIteration:
        backend.send_response("NullRecord", {})
        return
    backend.send_response("Record", totestkit.record(record))


def result_single(backend, data):
    result = backend.results[data["resultId"]]
    backend.send_response(
        "Record", totestkit.record(result.single(strict=True))
    )


def result_single_optional(backend, data):
    result = backend.results[data["resultId"]]
    with warnings.catch_warnings(record=True) as warning_list:
        warnings.simplefilter("always")
        record = result.single(strict=False)
    if record:
        record = totestkit.record(record)
    backend.send_response(
        "RecordOptional",
        {"record": record, "warnings": list(map(str, warning_list))},
    )


def result_peek(backend, data):
    result = backend.results[data["resultId"]]
    record = result.peek()
    if record is not None:
        backend.send_response("Record", totestkit.record(record))
    else:
        backend.send_response("NullRecord", {})


def result_list(backend, data):
    result = backend.results[data["resultId"]]
    records = Util.list(result)
    backend.send_response(
        "RecordList", {"records": [totestkit.record(r) for r in records]}
    )


def result_consume(backend, data):
    result = backend.results[data["resultId"]]
    summary = result.consume()
    assert isinstance(summary, neo4j.ResultSummary)
    backend.send_response("Summary", totestkit.summary(summary))


def forced_routing_table_update(backend, data):
    driver_id = data["driverId"]
    driver = backend.drivers[driver_id]
    database = data["database"]
    bookmarks = data["bookmarks"]
    with driver._pool.refresh_lock:
        driver._pool.update_routing_table(
            database=database, imp_user=None, bookmarks=bookmarks
        )
    backend.send_response("Driver", {"id": driver_id})


def get_routing_table(backend, data):
    driver_id = data["driverId"]
    database = data["database"]
    driver = backend.drivers[driver_id]
    routing_table = driver._pool.routing_tables[database]
    response_data = {
        "database": routing_table.database,
        "ttl": routing_table.ttl,
    }
    for role in ("routers", "readers", "writers"):
        addresses = getattr(routing_table, role)
        response_data[role] = list(map(str, addresses))
    backend.send_response("RoutingTable", response_data)


def get_connection_pool_metrics(backend, data):
    driver_id = data["driverId"]
    address = neo4j.Address.parse(data["address"])
    driver = backend.drivers[driver_id]
    connections = driver._pool.connections.get(address, ())
    in_use = (
        sum(c.in_use for c in connections)
        + driver._pool.connections_reservations[address]
    )
    idle = len(connections) - in_use
    backend.send_response(
        "ConnectionPoolMetrics",
        {
            "inUse": in_use,
            "idle": idle,
        },
    )


def fake_time_install(backend, _data):
    assert backend.fake_time is None
    assert backend.fake_time_ticker is None

    backend.fake_time = freeze_time()
    backend.fake_time_ticker = backend.fake_time.start()
    backend.send_response("FakeTimeAck", {})


def fake_time_tick(backend, data):
    assert backend.fake_time is not None
    assert backend.fake_time_ticker is not None

    increment_ms = data["incrementMs"]
    delta = datetime.timedelta(milliseconds=increment_ms)
    backend.fake_time_ticker.tick(delta=delta)
    backend.send_response("FakeTimeAck", {})


def fake_time_uninstall(backend, _data):
    assert backend.fake_time is not None
    assert backend.fake_time_ticker is not None

    backend.fake_time.stop()
    backend.fake_time_ticker = None
    backend.fake_time = None
    backend.send_response("FakeTimeAck", {})
