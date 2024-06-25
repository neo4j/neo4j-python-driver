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


import ssl

import pytest

from neo4j import (
    PreviewWarning,
    TrustAll,
    TrustCustomCAs,
    TrustSystemCAs,
)
from neo4j._conf import (
    Config,
    SessionConfig,
)
from neo4j._sync.config import PoolConfig
from neo4j.api import (
    TRUST_ALL_CERTIFICATES,
    TRUST_SYSTEM_CA_SIGNED_CERTIFICATES,
)
from neo4j.auth_management import (
    ClientCertificate,
    ClientCertificateProviders,
)
from neo4j.debug import watch
from neo4j.exceptions import ConfigurationError

from ..._async_compat import mark_sync_test
from ..common.test_conf import test_session_config


# python -m pytest tests/unit/test_conf.py -s -v

watch("neo4j")

test_pool_config = {
    "connection_timeout": 30.0,
    "keep_alive": True,
    "max_connection_lifetime": 3600,
    "liveness_check_timeout": None,
    "max_connection_pool_size": 100,
    "resolver": None,
    "encrypted": False,
    "user_agent": "test",
    "trusted_certificates": TrustSystemCAs(),
    "client_certificate": None,
    "ssl_context": None,
    "auth": None,
    "notifications_min_severity": None,
    "notifications_disabled_classifications": None,
    "telemetry_disabled": False,
}


def test_pool_config_consume():

    test_config = dict(test_pool_config)

    consumed_pool_config = PoolConfig.consume(test_config)

    assert isinstance(consumed_pool_config, PoolConfig)

    assert len(test_config) == 0

    for key in test_pool_config.keys():
        assert consumed_pool_config[key] == test_pool_config[key]

    for key in consumed_pool_config.keys():
        assert test_pool_config[key] == consumed_pool_config[key]

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
        PoolConfig.consume(test_config)

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


@pytest.mark.parametrize(
    ("value_trust", "expected_trusted_certificates_cls"),
    (
        (TRUST_ALL_CERTIFICATES, TrustAll),
        (TRUST_SYSTEM_CA_SIGNED_CERTIFICATES, TrustSystemCAs),
    )
)
def test_pool_config_deprecated_trust_config(
    value_trust, expected_trusted_certificates_cls
):
    with pytest.warns(DeprecationWarning, match="trust.*trusted_certificates"):
        consumed_pool_config = PoolConfig.consume({"trust": value_trust})
    assert isinstance(consumed_pool_config.trusted_certificates,
                      expected_trusted_certificates_cls)
    assert not hasattr(consumed_pool_config, "trust")


@pytest.mark.parametrize("value_trust", (
    TRUST_ALL_CERTIFICATES, TRUST_SYSTEM_CA_SIGNED_CERTIFICATES
))
@pytest.mark.parametrize("trusted_certificates", (
    TrustSystemCAs(), TrustAll(), TrustCustomCAs("foo"),
    TrustCustomCAs("foo", "bar")
))
def test_pool_config_deprecated_and_new_trust_config(value_trust,
                                                     trusted_certificates):
    with pytest.raises(ConfigurationError,
                       match="trusted_certificates.*trust"):
        PoolConfig.consume({
            "trust": value_trust,
            "trusted_certificates": trusted_certificates}
        )


def test_config_consume_chain():
    test_config = {}

    test_config.update(test_pool_config)

    test_config.update(test_session_config)

    consumed_pool_config, consumed_session_config = Config.consume_chain(
        test_config, PoolConfig, SessionConfig
    )

    assert isinstance(consumed_pool_config, PoolConfig)
    assert isinstance(consumed_session_config, SessionConfig)

    assert len(test_config) == 0

    for key, val in test_pool_config.items():
        assert consumed_pool_config[key] == val

    for key, val in consumed_pool_config.items():
        assert test_pool_config[key] == val

    assert len(consumed_pool_config) == len(test_pool_config)

    assert len(consumed_session_config) == len(test_session_config)


@pytest.mark.parametrize("config", (
    {},
    {"encrypted": False},
    {"trusted_certificates": TrustSystemCAs()},
    {"trusted_certificates": TrustAll()},
    {"trusted_certificates": TrustCustomCAs("foo", "bar")},
))
@mark_sync_test
def test_no_ssl_mock(config, mocker):
    ssl_context_mock = mocker.patch("ssl.SSLContext", autospec=True)
    pool_config = PoolConfig.consume(config)
    assert pool_config.encrypted is False
    assert pool_config.get_ssl_context() is None
    ssl_context_mock.assert_not_called()
    # test caching
    assert pool_config.get_ssl_context() is None
    ssl_context_mock.assert_not_called()


@pytest.mark.parametrize("config", (
    {"encrypted": True},
    {"encrypted": True, "trusted_certificates": TrustSystemCAs()},
))
@mark_sync_test
def test_trust_system_cas_mock(config, mocker):
    ssl_context_mock = mocker.patch("ssl.SSLContext", autospec=True)
    pool_config = PoolConfig.consume(config)
    assert pool_config.encrypted is True
    ssl_context = pool_config.get_ssl_context()
    _assert_mock_tls_1_2(ssl_context_mock)
    assert ssl_context.minimum_version == ssl.TLSVersion.TLSv1_2
    ssl_context_mock.return_value.load_default_certs.assert_called_once_with()
    ssl_context_mock.return_value.load_verify_locations.assert_not_called()
    assert ssl_context.check_hostname is True
    assert ssl_context.verify_mode == ssl.CERT_REQUIRED
    # test caching
    ssl_context_mock.reset_mock()
    assert pool_config.get_ssl_context() is ssl_context
    ssl_context_mock.assert_not_called()


@pytest.mark.parametrize("config", (
    {"encrypted": True, "trusted_certificates": TrustCustomCAs("foo", "bar")},
    {"encrypted": True, "trusted_certificates": TrustCustomCAs()},
))
@mark_sync_test
def test_trust_custom_cas_mock(config, mocker):
    ssl_context_mock = mocker.patch("ssl.SSLContext", autospec=True)
    certs = config["trusted_certificates"].certs
    pool_config = PoolConfig.consume(config)
    assert pool_config.encrypted is True
    ssl_context = pool_config.get_ssl_context()
    _assert_mock_tls_1_2(ssl_context_mock)
    assert ssl_context.minimum_version == ssl.TLSVersion.TLSv1_2
    ssl_context_mock.return_value.load_default_certs.assert_not_called()
    assert (
        ssl_context_mock.return_value.load_verify_locations.call_args_list
        == [((cert,), {}) for cert in certs]
    )
    assert ssl_context.check_hostname is True
    assert ssl_context.verify_mode == ssl.CERT_REQUIRED
    # test caching
    assert pool_config.get_ssl_context() is ssl_context


@pytest.mark.parametrize("config", (
    {"encrypted": True, "trusted_certificates": TrustAll()},
))
@mark_sync_test
def test_trust_all_mock(config, mocker):
    ssl_context_mock = mocker.patch("ssl.SSLContext", autospec=True)
    pool_config = PoolConfig.consume(config)
    assert pool_config.encrypted is True
    ssl_context = pool_config.get_ssl_context()
    _assert_mock_tls_1_2(ssl_context_mock)
    assert ssl_context.minimum_version == ssl.TLSVersion.TLSv1_2
    ssl_context_mock.return_value.load_default_certs.assert_not_called()
    ssl_context_mock.return_value.load_verify_locations.assert_not_called()
    assert ssl_context.check_hostname is False
    assert ssl_context.verify_mode is ssl.CERT_NONE
    # test caching
    ssl_context_mock.reset_mock()
    assert pool_config.get_ssl_context() is ssl_context
    ssl_context_mock.assert_not_called()


def _assert_mock_tls_1_2(mock):
    mock.assert_called_once_with(ssl.PROTOCOL_TLS_CLIENT)
    assert mock.return_value.minimum_version == ssl.TLSVersion.TLSv1_2


@pytest.mark.parametrize("config", (
    {},
    {"encrypted": False},
    {"trusted_certificates": TrustSystemCAs()},
    {"trusted_certificates": TrustAll()},
    {"trusted_certificates": TrustCustomCAs("foo", "bar")},
))
@mark_sync_test
def test_no_ssl(config):
    pool_config = PoolConfig.consume(config)
    assert pool_config.encrypted is False
    assert pool_config.get_ssl_context() is None
    # test caching
    assert pool_config.get_ssl_context() is None


@pytest.mark.parametrize("config", (
    {"encrypted": True},
    {"encrypted": True, "trusted_certificates": TrustSystemCAs()},
))
@mark_sync_test
def test_trust_system_cas(config):
    pool_config = PoolConfig.consume(config)
    assert pool_config.encrypted is True
    ssl_context = pool_config.get_ssl_context()
    assert isinstance(ssl_context, ssl.SSLContext)
    _assert_context_tls_1_2(ssl_context)
    assert ssl_context.check_hostname is True
    assert ssl_context.verify_mode == ssl.CERT_REQUIRED
    # test caching
    assert pool_config.get_ssl_context() is ssl_context


@pytest.mark.parametrize("config", (
    {"encrypted": True, "trusted_certificates": TrustCustomCAs()},
))
@mark_sync_test
def test_trust_custom_cas(config):
    pool_config = PoolConfig.consume(config)
    assert pool_config.encrypted is True
    ssl_context = pool_config.get_ssl_context()
    assert isinstance(ssl_context, ssl.SSLContext)
    _assert_context_tls_1_2(ssl_context)
    assert ssl_context.check_hostname is True
    assert ssl_context.verify_mode == ssl.CERT_REQUIRED
    # test caching
    assert pool_config.get_ssl_context() is ssl_context


@pytest.mark.parametrize("config", (
    {"encrypted": True, "trusted_certificates": TrustAll()},
))
@mark_sync_test
def test_trust_all(config):
    pool_config = PoolConfig.consume(config)
    assert pool_config.encrypted is True
    ssl_context = pool_config.get_ssl_context()
    assert isinstance(ssl_context, ssl.SSLContext)
    _assert_context_tls_1_2(ssl_context)
    assert ssl_context.check_hostname is False
    assert ssl_context.verify_mode is ssl.CERT_NONE
    # test caching
    assert pool_config.get_ssl_context() is ssl_context


def _assert_context_tls_1_2(ctx):
    assert ctx.protocol == ssl.PROTOCOL_TLS_CLIENT
    assert ctx.minimum_version == ssl.TLSVersion.TLSv1_2


@pytest.mark.parametrize("encrypted", (True, False))
@pytest.mark.parametrize("trusted_certificates", (
    TrustSystemCAs(), TrustAll(), TrustCustomCAs()
))
@mark_sync_test
def test_custom_ssl_context(encrypted, trusted_certificates):
    custom_ssl_context = object()
    pool_config = PoolConfig.consume({
        "encrypted": encrypted,
        "trusted_certificates": trusted_certificates,
        "ssl_context": custom_ssl_context,
    })
    assert pool_config.encrypted is encrypted
    assert pool_config.get_ssl_context() is custom_ssl_context
    # test caching
    assert pool_config.get_ssl_context() is custom_ssl_context


@pytest.mark.parametrize("trusted_certificates", (
    TrustSystemCAs(), TrustAll(), TrustCustomCAs()
))
@mark_sync_test
def test_client_certificate(trusted_certificates, mocker) -> None:
    ssl_context_mock = mocker.patch("ssl.SSLContext", autospec=True)

    with pytest.warns(PreviewWarning, match="Mutual TLS"):
        cert = ClientCertificate("certfile", "keyfile", "password")
    with pytest.warns(PreviewWarning, match="Mutual TLS"):
        provider = ClientCertificateProviders.rotating(cert)
    pool_config = PoolConfig.consume({
        "client_certificate": provider,
        "encrypted": True,
    })
    assert pool_config.client_certificate is provider

    ssl_context = pool_config.get_ssl_context()

    assert ssl_context is ssl_context_mock.return_value
    ssl_context_mock.return_value.load_cert_chain.assert_called_with(
        cert.certfile,
        keyfile=cert.keyfile,
        password=cert.password,
    )

    # test caching
    ssl_context_mock.return_value.reset_mock()
    ssl_context_mock.reset_mock()
    assert pool_config.get_ssl_context() is ssl_context
    ssl_context_mock.return_value.load_cert_chain.assert_not_called()
    ssl_context_mock.assert_not_called()

    # test cache invalidation
    with pytest.warns(PreviewWarning, match="Mutual TLS"):
        cert2 = ClientCertificate("certfile2", "keyfile2", "password2")
    provider.update_certificate(cert2)

    ssl_context = pool_config.get_ssl_context()

    assert ssl_context is ssl_context_mock.return_value
    ssl_context_mock.return_value.load_cert_chain.assert_called_with(
        cert2.certfile,
        keyfile=cert2.keyfile,
        password=cert2.password,
    )

    # test caching
    ssl_context_mock.return_value.reset_mock()
    ssl_context_mock.reset_mock()
    assert pool_config.get_ssl_context() is ssl_context
    ssl_context_mock.return_value.load_cert_chain.assert_not_called()
    ssl_context_mock.assert_not_called()
