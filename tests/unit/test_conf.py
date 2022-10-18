#!/usr/bin/env python
# -*- encoding: utf-8 -*-

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


from contextlib import contextmanager
import warnings

import ssl
import sys
import pytest

from neo4j.exceptions import (
    ConfigurationError,
)
from neo4j.conf import (
    Config,
    PoolConfig,
    WorkspaceConfig,
    SessionConfig,
)
from neo4j.api import (
    TRUST_SYSTEM_CA_SIGNED_CERTIFICATES,
    TRUST_ALL_CERTIFICATES,
    WRITE_ACCESS,
    READ_ACCESS,
)

# python -m pytest tests/unit/test_conf.py -s -v

from neo4j.debug import watch
watch("neo4j")

test_pool_config = {
    "connection_timeout": 30.0,
    "update_routing_table_timeout": 90.0,
    "init_size": 1,
    "keep_alive": True,
    "max_connection_lifetime": 3600,
    "max_connection_pool_size": 100,
    "protocol_version": None,
    "resolver": None,
    "encrypted": False,
    "user_agent": "test",
    "trust": TRUST_SYSTEM_CA_SIGNED_CERTIFICATES,
}

test_session_config = {
    "session_connection_timeout": 180.0,
    "connection_acquisition_timeout": 60.0,
    "max_transaction_retry_time": 30.0,
    "initial_retry_delay": 1.0,
    "retry_delay_multiplier": 2.0,
    "retry_delay_jitter_factor": 0.2,
    "bookmarks": (),
    "default_access_mode": WRITE_ACCESS,
    "database": None,
    "impersonated_user": None,
    "fetch_size": 100,
}

config_function_names = ["consume_chain", "consume"]


def test_pool_config_consume():

    test_config = dict(test_pool_config)

    consumed_pool_config = PoolConfig.consume(test_config)

    assert isinstance(consumed_pool_config, PoolConfig)

    assert len(test_config) == 0

    for key in test_pool_config.keys():
        assert consumed_pool_config[key] == test_pool_config[key]

    for key, val in consumed_pool_config.items():
        assert test_pool_config[key] == val

    assert len(consumed_pool_config) == len(test_pool_config)


def test_pool_config_consume_default_values():

    test_config = {}

    consumed_pool_config = PoolConfig.consume(test_config)

    assert isinstance(consumed_pool_config, PoolConfig)

    assert len(test_config) == 0

    consumed_pool_config.keep_alive = "changed"

    assert PoolConfig.keep_alive != consumed_pool_config.keep_alive


def test_pool_config_consume_key_not_valid():

    test_config = dict(test_pool_config)

    test_config["not_valid_key"] = "test"

    with pytest.raises(ConfigurationError) as error:
        consumed_pool_config = PoolConfig.consume(test_config)

    error.match("Unexpected config keys: not_valid_key")


def test_pool_config_set_value():

    test_config = dict(test_pool_config)

    consumed_pool_config = PoolConfig.consume(test_config)

    assert consumed_pool_config.get("encrypted") is False
    assert consumed_pool_config["encrypted"] is False
    assert consumed_pool_config.encrypted is False

    consumed_pool_config.encrypted = "test"

    assert consumed_pool_config.get("encrypted") == "test"
    assert consumed_pool_config["encrypted"] == "test"
    assert consumed_pool_config.encrypted == "test"

    consumed_pool_config.not_valid_key = "test"  # Use consume functions


def test_pool_config_consume_and_then_consume_again():

    test_config = dict(test_pool_config)
    consumed_pool_config = PoolConfig.consume(test_config)
    assert consumed_pool_config.encrypted is False
    consumed_pool_config.encrypted = "test"

    with pytest.raises(AttributeError):
        consumed_pool_config = PoolConfig.consume(consumed_pool_config)

    consumed_pool_config = PoolConfig.consume(dict(consumed_pool_config.items()))
    consumed_pool_config = PoolConfig.consume(dict(consumed_pool_config.items()))

    assert consumed_pool_config.encrypted == "test"


def test_config_consume_chain():

    test_config = {}

    test_config.update(test_pool_config)

    test_config.update(test_session_config)

    consumed_pool_config, consumed_session_config = Config.consume_chain(test_config, PoolConfig, SessionConfig)

    assert isinstance(consumed_pool_config, PoolConfig)
    assert isinstance(consumed_session_config, SessionConfig)

    assert len(test_config) == 0

    for key, val in test_pool_config.items():
        assert consumed_pool_config[key] == val

    for key, val in consumed_pool_config.items():
        assert test_pool_config[key] == val

    assert len(consumed_pool_config) == len(test_pool_config)

    assert len(consumed_session_config) == len(test_session_config)


def test_init_session_config_merge():
    # python -m pytest tests/unit/test_conf.py -s -v -k test_init_session_config

    test_config_a = {"connection_acquisition_timeout": 111}
    test_config_c = {"max_transaction_retry_time": 222}

    workspace_config = WorkspaceConfig(test_config_a, WorkspaceConfig.consume(test_config_c))
    assert len(test_config_a) == 1
    assert len(test_config_c) == 0
    assert isinstance(workspace_config, WorkspaceConfig)
    assert workspace_config.connection_acquisition_timeout == WorkspaceConfig.connection_acquisition_timeout
    assert workspace_config.max_transaction_retry_time == 222

    workspace_config = WorkspaceConfig(test_config_c, test_config_a)
    assert isinstance(workspace_config, WorkspaceConfig)
    assert workspace_config.connection_acquisition_timeout == 111
    assert workspace_config.max_transaction_retry_time == WorkspaceConfig.max_transaction_retry_time

    test_config_b = {"default_access_mode": READ_ACCESS, "connection_acquisition_timeout": 333}

    session_config = SessionConfig(workspace_config, test_config_b)
    assert session_config.connection_acquisition_timeout == 333
    assert session_config.default_access_mode == READ_ACCESS

    session_config = SessionConfig(test_config_b, workspace_config)
    assert session_config.connection_acquisition_timeout == 111
    assert session_config.default_access_mode == READ_ACCESS


def test_init_session_config_with_not_valid_key():
    # python -m pytest tests/unit/test_conf.py -s -v -k test_init_session_config_with_not_valid_key

    test_config_a = {"connection_acquisition_timeout": 111}
    workspace_config = WorkspaceConfig.consume(test_config_a)

    test_config_b = {"default_access_mode": READ_ACCESS, "connection_acquisition_timeout": 333, "not_valid_key": None}
    session_config = SessionConfig(workspace_config, test_config_b)

    with pytest.raises(AttributeError):
        assert session_config.not_valid_key is None

    with pytest.raises(ConfigurationError):
        _ = SessionConfig.consume(test_config_b)

    assert session_config.connection_acquisition_timeout == 333


@pytest.mark.parametrize("config", (
    {},
    {"encrypted": False},
    {"trust": TRUST_SYSTEM_CA_SIGNED_CERTIFICATES},
    {"trust": TRUST_ALL_CERTIFICATES},
))
def test_no_ssl_mock(config, mocker):
    ssl_context_mock = mocker.patch("ssl.SSLContext", autospec=True)
    pool_config = PoolConfig.consume(config)
    assert pool_config.encrypted is False
    assert pool_config.get_ssl_context() is None
    ssl_context_mock.assert_not_called()


@pytest.mark.parametrize("config", (
    {"encrypted": True},
    {"encrypted": True, "trust": TRUST_SYSTEM_CA_SIGNED_CERTIFICATES},
))
def test_trust_system_cas_mock(config, mocker):
    ssl_context_mock = mocker.patch("ssl.SSLContext", autospec=True)
    pool_config = PoolConfig.consume(config)
    assert pool_config.encrypted is True
    ssl_context = pool_config.get_ssl_context()
    _assert_mock_tls_1_2(ssl_context_mock)
    ssl_context_mock.return_value.load_default_certs.assert_called_once_with()
    ssl_context_mock.return_value.load_verify_locations.assert_not_called()
    assert ssl_context.check_hostname
    assert ssl_context.verify_mode


@pytest.mark.parametrize("config", (
    {"encrypted": True, "trust": TRUST_ALL_CERTIFICATES},
))
def test_trust_all_mock(config, mocker):
    ssl_context_mock = mocker.patch("ssl.SSLContext", autospec=True)
    pool_config = PoolConfig.consume(config)
    assert pool_config.encrypted is True
    ssl_context = pool_config.get_ssl_context()
    _assert_mock_tls_1_2(ssl_context_mock)
    ssl_context_mock.return_value.load_verify_locations.assert_not_called()
    assert ssl_context.check_hostname is False
    assert ssl_context.verify_mode is ssl.CERT_NONE


def _assert_mock_tls_1_2(mock):
    mock.assert_called_once_with(ssl.PROTOCOL_TLS_CLIENT)
    if sys.version_info >= (3, 7):
        assert mock.return_value.minimum_version == ssl.TLSVersion.TLSv1_2
    else:
        assert mock.mock_calls[1][1][0] == ssl.OP_NO_TLSv1


@pytest.mark.parametrize("config", (
    {},
    {"encrypted": False},
    {"trust": TRUST_SYSTEM_CA_SIGNED_CERTIFICATES},
    {"trust": TRUST_ALL_CERTIFICATES},
))
def test_no_ssl(config):
    pool_config = PoolConfig.consume(config)
    assert pool_config.encrypted is False
    assert pool_config.get_ssl_context() is None


@pytest.mark.parametrize("config", (
    {"encrypted": True},
    {"encrypted": True, "trust": TRUST_SYSTEM_CA_SIGNED_CERTIFICATES},
))
def test_trust_system_cas(config):
    pool_config = PoolConfig.consume(config)
    assert pool_config.encrypted is True
    ssl_context = pool_config.get_ssl_context()
    assert isinstance(ssl_context, ssl.SSLContext)
    _assert_context_tls_1_2(ssl_context)
    assert ssl_context.check_hostname is True
    assert ssl_context.verify_mode == ssl.CERT_REQUIRED


@pytest.mark.parametrize("config", (
    {"encrypted": True, "trust": TRUST_ALL_CERTIFICATES},
))
def test_trust_all(config):
    pool_config = PoolConfig.consume(config)
    assert pool_config.encrypted is True
    ssl_context = pool_config.get_ssl_context()
    assert isinstance(ssl_context, ssl.SSLContext)
    _assert_context_tls_1_2(ssl_context)
    assert ssl_context.check_hostname is False
    assert ssl_context.verify_mode is ssl.CERT_NONE


def _assert_context_tls_1_2(ctx):
    if sys.version_info >= (3, 7):
        assert ctx.protocol == ssl.PROTOCOL_TLS_CLIENT
        assert ctx.minimum_version == ssl.TLSVersion.TLSv1_2
