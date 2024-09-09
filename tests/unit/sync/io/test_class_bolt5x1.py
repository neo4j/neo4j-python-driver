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


import itertools
import logging

import pytest

import neo4j
import neo4j.exceptions
from neo4j._api import TelemetryAPI
from neo4j._meta import USER_AGENT
from neo4j._sync.config import PoolConfig
from neo4j._sync.io._bolt5 import Bolt5x1
from neo4j.exceptions import ConfigurationError

from ...._async_compat import mark_sync_test
from ....iter_util import powerset


@pytest.mark.parametrize("set_stale", (True, False))
def test_conn_is_stale(fake_socket, set_stale):
    address = neo4j.Address(("127.0.0.1", 7687))
    max_connection_lifetime = 0
    connection = Bolt5x1(
        address, fake_socket(address), max_connection_lifetime
    )
    if set_stale:
        connection.set_stale()
    assert connection.stale() is True


@pytest.mark.parametrize("set_stale", (True, False))
def test_conn_is_not_stale_if_not_enabled(fake_socket, set_stale):
    address = neo4j.Address(("127.0.0.1", 7687))
    max_connection_lifetime = -1
    connection = Bolt5x1(
        address, fake_socket(address), max_connection_lifetime
    )
    if set_stale:
        connection.set_stale()
    assert connection.stale() is set_stale


@pytest.mark.parametrize("set_stale", (True, False))
def test_conn_is_not_stale(fake_socket, set_stale):
    address = neo4j.Address(("127.0.0.1", 7687))
    max_connection_lifetime = 999999999
    connection = Bolt5x1(
        address, fake_socket(address), max_connection_lifetime
    )
    if set_stale:
        connection.set_stale()
    assert connection.stale() is set_stale


@pytest.mark.parametrize(
    ("args", "kwargs", "expected_fields"),
    (
        (("", {}), {"db": "something"}, ({"db": "something"},)),
        (("", {}), {"imp_user": "imposter"}, ({"imp_user": "imposter"},)),
        (
            ("", {}),
            {"db": "something", "imp_user": "imposter"},
            ({"db": "something", "imp_user": "imposter"},),
        ),
    ),
)
@mark_sync_test
def test_extra_in_begin(fake_socket, args, kwargs, expected_fields):
    address = neo4j.Address(("127.0.0.1", 7687))
    socket = fake_socket(address, Bolt5x1.UNPACKER_CLS)
    connection = Bolt5x1(
        address, socket, PoolConfig.max_connection_lifetime
    )
    connection.begin(*args, **kwargs)
    connection.send_all()
    tag, is_fields = socket.pop_message()
    assert tag == b"\x11"
    assert tuple(is_fields) == expected_fields


@pytest.mark.parametrize(
    ("args", "kwargs", "expected_fields"),
    (
        (("", {}), {"db": "something"}, ("", {}, {"db": "something"})),
        (
            ("", {}),
            {"imp_user": "imposter"},
            ("", {}, {"imp_user": "imposter"}),
        ),
        (
            ("", {}),
            {"db": "something", "imp_user": "imposter"},
            ("", {}, {"db": "something", "imp_user": "imposter"}),
        ),
    ),
)
@mark_sync_test
def test_extra_in_run(fake_socket, args, kwargs, expected_fields):
    address = neo4j.Address(("127.0.0.1", 7687))
    socket = fake_socket(address, Bolt5x1.UNPACKER_CLS)
    connection = Bolt5x1(
        address, socket, PoolConfig.max_connection_lifetime
    )
    connection.run(*args, **kwargs)
    connection.send_all()
    tag, is_fields = socket.pop_message()
    assert tag == b"\x10"
    assert tuple(is_fields) == expected_fields


@mark_sync_test
def test_n_extra_in_discard(fake_socket):
    address = neo4j.Address(("127.0.0.1", 7687))
    socket = fake_socket(address, Bolt5x1.UNPACKER_CLS)
    connection = Bolt5x1(
        address, socket, PoolConfig.max_connection_lifetime
    )
    connection.discard(n=666)
    connection.send_all()
    tag, fields = socket.pop_message()
    assert tag == b"\x2f"
    assert len(fields) == 1
    assert fields[0] == {"n": 666}


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        (666, {"n": -1, "qid": 666}),
        (-1, {"n": -1}),
    ],
)
@mark_sync_test
def test_qid_extra_in_discard(fake_socket, test_input, expected):
    address = neo4j.Address(("127.0.0.1", 7687))
    socket = fake_socket(address, Bolt5x1.UNPACKER_CLS)
    connection = Bolt5x1(
        address, socket, PoolConfig.max_connection_lifetime
    )
    connection.discard(qid=test_input)
    connection.send_all()
    tag, fields = socket.pop_message()
    assert tag == b"\x2f"
    assert len(fields) == 1
    assert fields[0] == expected


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        (777, {"n": 666, "qid": 777}),
        (-1, {"n": 666}),
    ],
)
@mark_sync_test
def test_n_and_qid_extras_in_discard(fake_socket, test_input, expected):
    address = neo4j.Address(("127.0.0.1", 7687))
    socket = fake_socket(address, Bolt5x1.UNPACKER_CLS)
    connection = Bolt5x1(
        address, socket, PoolConfig.max_connection_lifetime
    )
    connection.discard(n=666, qid=test_input)
    connection.send_all()
    tag, fields = socket.pop_message()
    assert tag == b"\x2f"
    assert len(fields) == 1
    assert fields[0] == expected


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        (666, {"n": 666}),
        (-1, {"n": -1}),
    ],
)
@mark_sync_test
def test_n_extra_in_pull(fake_socket, test_input, expected):
    address = neo4j.Address(("127.0.0.1", 7687))
    socket = fake_socket(address, Bolt5x1.UNPACKER_CLS)
    connection = Bolt5x1(
        address, socket, PoolConfig.max_connection_lifetime
    )
    connection.pull(n=test_input)
    connection.send_all()
    tag, fields = socket.pop_message()
    assert tag == b"\x3f"
    assert len(fields) == 1
    assert fields[0] == expected


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        (777, {"n": -1, "qid": 777}),
        (-1, {"n": -1}),
    ],
)
@mark_sync_test
def test_qid_extra_in_pull(fake_socket, test_input, expected):
    address = neo4j.Address(("127.0.0.1", 7687))
    socket = fake_socket(address, Bolt5x1.UNPACKER_CLS)
    connection = Bolt5x1(
        address, socket, PoolConfig.max_connection_lifetime
    )
    connection.pull(qid=test_input)
    connection.send_all()
    tag, fields = socket.pop_message()
    assert tag == b"\x3f"
    assert len(fields) == 1
    assert fields[0] == expected


@mark_sync_test
def test_n_and_qid_extras_in_pull(fake_socket):
    address = neo4j.Address(("127.0.0.1", 7687))
    socket = fake_socket(address, Bolt5x1.UNPACKER_CLS)
    connection = Bolt5x1(
        address, socket, PoolConfig.max_connection_lifetime
    )
    connection.pull(n=666, qid=777)
    connection.send_all()
    tag, fields = socket.pop_message()
    assert tag == b"\x3f"
    assert len(fields) == 1
    assert fields[0] == {"n": 666, "qid": 777}


@mark_sync_test
def test_hello_passes_routing_metadata(fake_socket_pair):
    address = neo4j.Address(("127.0.0.1", 7687))
    sockets = fake_socket_pair(
        address,
        packer_cls=Bolt5x1.PACKER_CLS,
        unpacker_cls=Bolt5x1.UNPACKER_CLS,
    )
    sockets.server.send_message(b"\x70", {"server": "Neo4j/4.4.0"})
    sockets.server.send_message(b"\x70", {})
    connection = Bolt5x1(
        address,
        sockets.client,
        PoolConfig.max_connection_lifetime,
        routing_context={"foo": "bar"},
    )
    connection.hello()
    tag, fields = sockets.server.pop_message()
    assert tag == b"\x01"
    assert len(fields) == 1
    assert fields[0]["routing"] == {"foo": "bar"}


@pytest.mark.parametrize("api", TelemetryAPI)
@pytest.mark.parametrize("serv_enabled", (True, False))
@pytest.mark.parametrize("driver_disabled", (True, False))
@mark_sync_test
def test_telemetry_message(
    fake_socket, api, serv_enabled, driver_disabled
):
    address = neo4j.Address(("127.0.0.1", 7687))
    socket = fake_socket(address, Bolt5x1.UNPACKER_CLS)
    connection = Bolt5x1(
        address,
        socket,
        PoolConfig.max_connection_lifetime,
        telemetry_disabled=driver_disabled,
    )
    if serv_enabled:
        connection.configuration_hints["telemetry.enabled"] = True
    connection.telemetry(api)
    connection.send_all()

    with pytest.raises(OSError):
        socket.pop_message()


def _assert_logon_message(sockets, auth):
    tag, fields = sockets.server.pop_message()
    assert tag == b"\x6a"  # LOGON
    assert len(fields) == 1
    keys = ["scheme", "principal", "credentials"]
    assert list(fields[0].keys()) == keys
    for key in keys:
        assert fields[0][key] == getattr(auth, key)


@mark_sync_test
def test_hello_pipelines_logon(fake_socket_pair):
    auth = neo4j.Auth("basic", "alice123", "supersecret123")
    address = neo4j.Address(("127.0.0.1", 7687))
    sockets = fake_socket_pair(
        address,
        packer_cls=Bolt5x1.PACKER_CLS,
        unpacker_cls=Bolt5x1.UNPACKER_CLS,
    )
    sockets.server.send_message(
        b"\x7f",
        {
            "code": "Neo.DatabaseError.General.MadeUpError",
            "message": "kthxbye",
        },
    )
    connection = Bolt5x1(
        address,
        sockets.client,
        PoolConfig.max_connection_lifetime,
        auth=auth,
    )
    with pytest.raises(neo4j.exceptions.Neo4jError):
        connection.hello()
    tag, fields = sockets.server.pop_message()
    assert tag == b"\x01"  # HELLO
    assert len(fields) == 1
    assert list(fields[0].keys()) == ["user_agent"]
    assert auth.credentials not in repr(fields)
    _assert_logon_message(sockets, auth)


@mark_sync_test
def test_logon(fake_socket_pair):
    auth = neo4j.Auth("basic", "alice123", "supersecret123")
    address = neo4j.Address(("127.0.0.1", 7687))
    sockets = fake_socket_pair(
        address,
        packer_cls=Bolt5x1.PACKER_CLS,
        unpacker_cls=Bolt5x1.UNPACKER_CLS,
    )
    connection = Bolt5x1(
        address,
        sockets.client,
        PoolConfig.max_connection_lifetime,
        auth=auth,
    )
    connection.logon()
    connection.send_all()
    _assert_logon_message(sockets, auth)


@mark_sync_test
def test_re_auth(fake_socket_pair, mocker, static_auth):
    auth = neo4j.Auth("basic", "alice123", "supersecret123")
    auth_manager = static_auth(auth)
    address = neo4j.Address(("127.0.0.1", 7687))
    sockets = fake_socket_pair(
        address,
        packer_cls=Bolt5x1.PACKER_CLS,
        unpacker_cls=Bolt5x1.UNPACKER_CLS,
    )
    sockets.server.send_message(
        b"\x7f",
        {
            "code": "Neo.DatabaseError.General.MadeUpError",
            "message": "kthxbye",
        },
    )
    connection = Bolt5x1(
        address, sockets.client, PoolConfig.max_connection_lifetime
    )
    connection.pool = mocker.MagicMock()
    connection.re_auth(auth, auth_manager)
    connection.send_all()
    with pytest.raises(neo4j.exceptions.Neo4jError):
        connection.fetch_all()
    tag, fields = sockets.server.pop_message()
    assert tag == b"\x6b"  # LOGOFF
    assert len(fields) == 0
    _assert_logon_message(sockets, auth)
    assert connection.auth is auth
    assert connection.auth_manager is auth_manager


@mark_sync_test
def test_logoff(fake_socket_pair):
    address = neo4j.Address(("127.0.0.1", 7687))
    sockets = fake_socket_pair(
        address,
        packer_cls=Bolt5x1.PACKER_CLS,
        unpacker_cls=Bolt5x1.UNPACKER_CLS,
    )
    sockets.server.send_message(b"\x70", {})
    connection = Bolt5x1(
        address, sockets.client, PoolConfig.max_connection_lifetime
    )
    connection.logoff()
    assert not sockets.server.recv_buffer  # pipelined, so no response yet
    connection.send_all()
    assert sockets.server.recv_buffer  # now!
    tag, fields = sockets.server.pop_message()
    assert tag == b"\x6b"  # LOGOFF
    assert len(fields) == 0


@pytest.mark.parametrize(
    ("hints", "valid"),
    (
        ({"connection.recv_timeout_seconds": 1}, True),
        ({"connection.recv_timeout_seconds": 42}, True),
        ({}, True),
        ({"whatever_this_is": "ignore me!"}, True),
        ({"connection.recv_timeout_seconds": -1}, False),
        ({"connection.recv_timeout_seconds": 0}, False),
        ({"connection.recv_timeout_seconds": 2.5}, False),
        ({"connection.recv_timeout_seconds": None}, False),
        ({"connection.recv_timeout_seconds": False}, False),
        ({"connection.recv_timeout_seconds": "1"}, False),
    ),
)
@mark_sync_test
def test_hint_recv_timeout_seconds(
    fake_socket_pair, hints, valid, caplog, mocker
):
    address = neo4j.Address(("127.0.0.1", 7687))
    sockets = fake_socket_pair(
        address,
        packer_cls=Bolt5x1.PACKER_CLS,
        unpacker_cls=Bolt5x1.UNPACKER_CLS,
    )
    sockets.client.settimeout = mocker.Mock()
    sockets.server.send_message(
        b"\x70", {"server": "Neo4j/4.3.4", "hints": hints}
    )
    sockets.server.send_message(b"\x70", {})
    connection = Bolt5x1(
        address, sockets.client, PoolConfig.max_connection_lifetime
    )
    with caplog.at_level(logging.INFO):
        connection.hello()
    if valid:
        if "connection.recv_timeout_seconds" in hints:
            sockets.client.settimeout.assert_called_once_with(
                hints["connection.recv_timeout_seconds"]
            )
        else:
            sockets.client.settimeout.assert_not_called()
        assert not any(
            "recv_timeout_seconds" in msg and "invalid" in msg
            for msg in caplog.messages
        )
    else:
        sockets.client.settimeout.assert_not_called()
        assert any(
            repr(hints["connection.recv_timeout_seconds"]) in msg
            and "recv_timeout_seconds" in msg
            and "invalid" in msg
            for msg in caplog.messages
        )


CREDENTIALS = "+++super-secret-sauce+++"


@pytest.mark.parametrize(
    "auth",
    (
        ("user", CREDENTIALS),
        neo4j.basic_auth("user", CREDENTIALS),
        neo4j.kerberos_auth(CREDENTIALS),
        neo4j.bearer_auth(CREDENTIALS),
        neo4j.custom_auth("user", CREDENTIALS, "realm", "scheme"),
        neo4j.Auth("scheme", "principal", CREDENTIALS, "realm", foo="bar"),
    ),
)
@mark_sync_test
def test_credentials_are_not_logged(auth, fake_socket_pair, caplog):
    address = neo4j.Address(("127.0.0.1", 7687))
    sockets = fake_socket_pair(
        address,
        packer_cls=Bolt5x1.PACKER_CLS,
        unpacker_cls=Bolt5x1.UNPACKER_CLS,
    )
    sockets.server.send_message(b"\x70", {"server": "Neo4j/4.3.4"})
    sockets.server.send_message(b"\x70", {})
    connection = Bolt5x1(
        address,
        sockets.client,
        PoolConfig.max_connection_lifetime,
        auth=auth,
    )
    with caplog.at_level(logging.DEBUG):
        connection.hello()

    if isinstance(auth, tuple):
        auth = neo4j.basic_auth(*auth)
    for field in ("scheme", "principal", "realm", "parameters"):
        value = getattr(auth, field, None)
        if value:
            assert repr(value) in caplog.text
    assert CREDENTIALS not in caplog.text


@pytest.mark.parametrize(
    ("method", "args"),
    (
        ("run", ("RETURN 1",)),
        ("begin", ()),
    ),
)
@pytest.mark.parametrize(
    "kwargs",
    (
        {"notifications_min_severity": "WARNING"},
        {"notifications_disabled_classifications": ["HINT"]},
        {"notifications_disabled_classifications": []},
        {
            "notifications_min_severity": "WARNING",
            "notifications_disabled_classifications": ["HINT"],
        },
    ),
)
def test_does_not_support_notification_filters(
    fake_socket, method, args, kwargs
):
    address = neo4j.Address(("127.0.0.1", 7687))
    socket = fake_socket(address, Bolt5x1.UNPACKER_CLS)
    connection = Bolt5x1(
        address, socket, PoolConfig.max_connection_lifetime
    )
    method = getattr(connection, method)
    with pytest.raises(ConfigurationError, match="Notification filtering"):
        method(*args, **kwargs)


@mark_sync_test
@pytest.mark.parametrize(
    "kwargs",
    (
        {"notifications_min_severity": "WARNING"},
        {"notifications_disabled_classifications": ["HINT"]},
        {"notifications_disabled_classifications": []},
        {
            "notifications_min_severity": "WARNING",
            "notifications_disabled_classifications": ["HINT"],
        },
    ),
)
def test_hello_does_not_support_notification_filters(
    fake_socket, kwargs
):
    address = neo4j.Address(("127.0.0.1", 7687))
    socket = fake_socket(address, Bolt5x1.UNPACKER_CLS)
    connection = Bolt5x1(
        address, socket, PoolConfig.max_connection_lifetime, **kwargs
    )
    with pytest.raises(ConfigurationError, match="Notification filtering"):
        connection.hello()


@mark_sync_test
@pytest.mark.parametrize(
    "user_agent", (None, "test user agent", "", USER_AGENT)
)
def test_user_agent(fake_socket_pair, user_agent):
    address = neo4j.Address(("127.0.0.1", 7687))
    sockets = fake_socket_pair(
        address,
        packer_cls=Bolt5x1.PACKER_CLS,
        unpacker_cls=Bolt5x1.UNPACKER_CLS,
    )
    sockets.server.send_message(b"\x70", {"server": "Neo4j/1.2.3"})
    sockets.server.send_message(b"\x70", {})
    max_connection_lifetime = 0
    connection = Bolt5x1(
        address, sockets.client, max_connection_lifetime, user_agent=user_agent
    )
    connection.hello()

    _tag, fields = sockets.server.pop_message()
    extra = fields[0]
    if not user_agent:
        assert extra["user_agent"] == USER_AGENT
    else:
        assert extra["user_agent"] == user_agent


@mark_sync_test
@pytest.mark.parametrize(
    "user_agent", (None, "test user agent", "", USER_AGENT)
)
def test_does_not_send_bolt_agent(fake_socket_pair, user_agent):
    address = neo4j.Address(("127.0.0.1", 7687))
    sockets = fake_socket_pair(
        address,
        packer_cls=Bolt5x1.PACKER_CLS,
        unpacker_cls=Bolt5x1.UNPACKER_CLS,
    )
    sockets.server.send_message(b"\x70", {"server": "Neo4j/1.2.3"})
    sockets.server.send_message(b"\x70", {})
    max_connection_lifetime = 0
    connection = Bolt5x1(
        address, sockets.client, max_connection_lifetime, user_agent=user_agent
    )
    connection.hello()

    _tag, fields = sockets.server.pop_message()
    extra = fields[0]
    assert "bolt_agent" not in extra


@mark_sync_test
@pytest.mark.parametrize(
    ("func", "args", "extra_idx"),
    (
        ("run", ("RETURN 1",), 2),
        ("begin", (), 0),
    ),
)
@pytest.mark.parametrize(
    ("timeout", "res"),
    (
        (None, None),
        (0, 0),
        (0.1, 100),
        (0.001, 1),
        (1e-15, 1),
        (0.0005, 1),
        (0.0001, 1),
        (1.0015, 1002),
        (1.000499, 1000),
        (1.0025, 1002),
        (3.0005, 3000),
        (3.456, 3456),
        (1, 1000),
        (-1e-15, ValueError("Timeout must be a positive number or 0")),
        (
            "foo",
            ValueError("Timeout must be specified as a number of seconds"),
        ),
        (
            [1, 2],
            TypeError("Timeout must be specified as a number of seconds"),
        ),
    ),
)
def test_tx_timeout(
    fake_socket_pair, func, args, extra_idx, timeout, res
):
    address = neo4j.Address(("127.0.0.1", 7687))
    sockets = fake_socket_pair(
        address,
        packer_cls=Bolt5x1.PACKER_CLS,
        unpacker_cls=Bolt5x1.UNPACKER_CLS,
    )
    sockets.server.send_message(b"\x70", {})
    connection = Bolt5x1(address, sockets.client, 0)
    func = getattr(connection, func)
    if isinstance(res, Exception):
        with pytest.raises(type(res), match=str(res)):
            func(*args, timeout=timeout)
    else:
        func(*args, timeout=timeout)
        connection.send_all()
        _tag, fields = sockets.server.pop_message()
        extra = fields[extra_idx]
        if timeout is None:
            assert "tx_timeout" not in extra
        else:
            assert extra["tx_timeout"] == res


@pytest.mark.parametrize(
    "actions",
    itertools.combinations_with_replacement(
        itertools.product(
            ("run", "begin", "begin_run"),
            ("reset", "commit", "rollback"),
            (None, "some_db", "another_db"),
        ),
        2,
    ),
)
@mark_sync_test
def test_tracks_last_database(fake_socket_pair, actions):
    address = neo4j.Address(("127.0.0.1", 7687))
    sockets = fake_socket_pair(
        address,
        packer_cls=Bolt5x1.PACKER_CLS,
        unpacker_cls=Bolt5x1.UNPACKER_CLS,
    )
    connection = Bolt5x1(address, sockets.client, 0)
    sockets.server.send_message(b"\x70", {"server": "Neo4j/1.2.3"})
    sockets.server.send_message(b"\x70", {})
    connection.hello()
    assert connection.last_database is None
    for action, finish, db in actions:
        sockets.server.send_message(b"\x70", {})
        if action == "run":
            connection.run("RETURN 1", db=db)
        elif action == "begin":
            connection.begin(db=db)
        elif action == "begin_run":
            connection.begin(db=db)
            assert connection.last_database == db
            sockets.server.send_message(b"\x70", {})
            connection.run("RETURN 1")
        else:
            raise ValueError(action)

        assert connection.last_database == db
        connection.send_all()
        connection.fetch_all()
        assert connection.last_database == db

        sockets.server.send_message(b"\x70", {})
        if finish == "reset":
            connection.reset()
        elif finish == "commit":
            if action == "run":
                connection.pull()
            else:
                connection.commit()
        elif finish == "rollback":
            if action == "run":
                connection.pull()
            else:
                connection.rollback()
        else:
            raise ValueError(finish)

        connection.send_all()
        connection.fetch_all()

        assert connection.last_database == db


@pytest.mark.parametrize(
    "sent_diag_records",
    powerset(
        (
            ...,
            None,
            {},
            [],
            "1",
            1,
            {"OPERATION_CODE": "0"},
            {"OPERATION": "", "OPERATION_CODE": "0", "CURRENT_SCHEMA": "/"},
        ),
        limit=3,
    ),
)
@pytest.mark.parametrize("method", ("pull", "discard"))
@mark_sync_test
def test_does_not_enrich_diagnostic_record(
    sent_diag_records,
    method,
    fake_socket_pair,
):
    address = neo4j.Address(("127.0.0.1", 7687))
    sockets = fake_socket_pair(
        address,
        packer_cls=Bolt5x1.PACKER_CLS,
        unpacker_cls=Bolt5x1.UNPACKER_CLS,
    )
    connection = Bolt5x1(address, sockets.client, 0)

    sent_metadata = {
        "statuses": [
            {"diagnostic_record": r} if r is not ... else {}
            for r in sent_diag_records
        ]
    }
    sockets.server.send_message(b"\x70", sent_metadata)

    received_metadata = None

    def on_success(metadata):
        nonlocal received_metadata
        received_metadata = metadata

    getattr(connection, method)(on_success=on_success)
    connection.send_all()
    connection.fetch_all()

    assert received_metadata == sent_metadata
