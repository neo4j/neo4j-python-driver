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

from unittest.mock import MagicMock

import pytest

from neo4j import (
    Transaction,
)

from ._fake_connection import fake_connection


def test_transaction_context_calls_commit(mocker, fake_connection):
    on_closed = MagicMock()
    on_network_error = MagicMock()
    tx = Transaction(fake_connection, 2, on_closed, on_network_error)
    mock_commit = mocker.patch.object(tx, "commit", wraps=tx.commit)
    mock_rollback = mocker.patch.object(tx, "rollback", wraps=tx.rollback)
    with tx as tx_:
        assert tx is tx_
        pass
    mock_commit.assert_called_once_with()
    assert mock_rollback.call_count == 0


def test_transaction_context_calls_rollback_on_error(mocker, fake_connection):
    class OopsError(RuntimeError):
        pass

    on_closed = MagicMock()
    on_network_error = MagicMock()
    tx = Transaction(fake_connection, 2, on_closed, on_network_error)
    mock_commit = mocker.patch.object(tx, "commit", wraps=tx.commit)
    mock_rollback = mocker.patch.object(tx, "rollback", wraps=tx.rollback)
    with pytest.raises(OopsError):
        with tx as tx_:
            assert tx is tx_
            raise OopsError
    assert mock_commit.call_count == 0
    mock_rollback.assert_called_once_with()
