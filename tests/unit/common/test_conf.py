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


import pytest

from neo4j._conf import (
    SessionConfig,
    WorkspaceConfig,
)
from neo4j.api import (
    READ_ACCESS,
    WRITE_ACCESS,
)
from neo4j.debug import watch
from neo4j.exceptions import ConfigurationError


watch("neo4j")

test_session_config = {
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
    "bookmark_manager": object(),
    "auth": None,
    "notifications_min_severity": None,
    "notifications_disabled_classifications": None,
    "warn_notification_severity": None,
}


def test_init_session_config_merge():
    test_config_a = {"connection_acquisition_timeout": 111}
    test_config_c = {"max_transaction_retry_time": 222}

    workspace_config = WorkspaceConfig(
        test_config_a, WorkspaceConfig.consume(test_config_c)
    )
    assert len(test_config_a) == 1
    assert len(test_config_c) == 0
    assert isinstance(workspace_config, WorkspaceConfig)
    assert (
        workspace_config.connection_acquisition_timeout
        == WorkspaceConfig.connection_acquisition_timeout
    )
    assert workspace_config.max_transaction_retry_time == 222

    workspace_config = WorkspaceConfig(test_config_c, test_config_a)
    assert isinstance(workspace_config, WorkspaceConfig)
    assert workspace_config.connection_acquisition_timeout == 111
    assert (
        workspace_config.max_transaction_retry_time
        == WorkspaceConfig.max_transaction_retry_time
    )

    test_config_b = {
        "default_access_mode": READ_ACCESS,
        "connection_acquisition_timeout": 333,
    }

    session_config = SessionConfig(workspace_config, test_config_b)
    assert session_config.connection_acquisition_timeout == 333
    assert session_config.default_access_mode == READ_ACCESS

    session_config = SessionConfig(test_config_b, workspace_config)
    assert session_config.connection_acquisition_timeout == 111
    assert session_config.default_access_mode == READ_ACCESS


def test_init_session_config_with_not_valid_key():
    test_config_a = {"connection_acquisition_timeout": 111}
    workspace_config = WorkspaceConfig.consume(test_config_a)

    test_config_b = {
        "default_access_mode": READ_ACCESS,
        "connection_acquisition_timeout": 333,
        "not_valid_key": None,
    }
    session_config = SessionConfig(workspace_config, test_config_b)

    with pytest.raises(AttributeError):
        assert session_config.not_valid_key is None

    with pytest.raises(ConfigurationError):
        _ = SessionConfig.consume(test_config_b)

    assert session_config.connection_acquisition_timeout == 333
