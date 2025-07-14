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


from __future__ import annotations

import pytest

from neo4j import (
    _typing as t,
    GqlStatusObject,
)

from ..._deprecated_imports import SummaryNotification


if t.TYPE_CHECKING:

    class TNotificationData(t.TypedDict):
        code: t.NotRequired[str | None]
        severity: t.NotRequired[str | None]
        title: t.NotRequired[str | None]
        description: t.NotRequired[str | None]
        category: t.NotRequired[str | None]
        position: t.NotRequired[Position | None]

    class TStatusNotificationData(t.TypedDict):
        gql_status: t.NotRequired[str | None]
        status_description: t.NotRequired[str | None]
        neo4j_code: t.NotRequired[str | None]
        title: t.NotRequired[str | None]
        description: t.NotRequired[str | None]
        diagnostic_record: t.NotRequired[TDiagnosticRecordData | None]

    class TDiagnosticRecordData(t.TypedDict):
        OPERATION: t.NotRequired[str | None]
        OPERATION_code: t.NotRequired[str | None]
        CURRENT_SCHEMA: t.NotRequired[str | None]
        _status_parameters: t.NotRequired[dict[str, t.Any] | None]
        _severity: t.NotRequired[str | None]
        _classification: t.NotRequired[str | None]
        _position: t.NotRequired[Position | None]

    class Position(t.TypedDict):
        offset: t.NotRequired[int | None]
        line: t.NotRequired[int | None]
        column: t.NotRequired[int | None]

    class TNotificationFactory(t.Protocol):
        def __call__(
            self,
            data: TNotificationData | None = None,
            data_overwrite: TNotificationData | None = None,
        ) -> SummaryNotification: ...

    class TStatusNotificationFactory(t.Protocol):
        def __call__(
            self,
            data: TStatusNotificationData | None = None,
            data_overwrite: TStatusNotificationData | None = None,
            diag_rec_overwrite: TDiagnosticRecordData | None = None,
        ) -> GqlStatusObject: ...

    class TStatusNotificationLegacyFactory(t.Protocol):
        def __call__(
            self,
            data: TNotificationData | None = None,
            data_overwrite: TNotificationData | None = None,
        ) -> GqlStatusObject: ...

    class TRawNotificationFactory(t.Protocol):
        def __call__(
            self,
            data: TNotificationData | None = None,
            data_overwrite: TNotificationData | None = None,
        ) -> TNotificationData: ...

    class TRawStatusNotificationFactory(t.Protocol):
        def __call__(
            self,
            data: TStatusNotificationData | None = None,
            data_overwrite: TStatusNotificationData | None = None,
            diag_rec_overwrite: TDiagnosticRecordData | None = None,
        ) -> TStatusNotificationData: ...


__all__ = [
    "notification_factory",
    "raw_notification_factory",
    "raw_status_notification_factory",
    "status_notification_factory",
    "status_notification_legacy_factory",
]


@pytest.fixture
def notification_factory() -> TNotificationFactory:
    return _notification_factory


def _notification_factory(
    data: TNotificationData | None = None,
    data_overwrite: TNotificationData | None = None,
) -> SummaryNotification:
    data = _raw_notification_factory(data, data_overwrite)
    return SummaryNotification._from_metadata(data)


@pytest.fixture
def raw_notification_factory() -> TRawNotificationFactory:
    return _raw_notification_factory


def _raw_notification_factory(
    data: TNotificationData | None = None,
    data_overwrite: TNotificationData | None = None,
) -> TNotificationData:
    if data is None:
        data = _test_notification_data()
    if data_overwrite:
        data.update(data_overwrite)
    return data


@pytest.fixture
def status_notification_legacy_factory() -> TStatusNotificationLegacyFactory:
    return _status_notification_legacy_factory


def _status_notification_legacy_factory(
    data: TNotificationData | None = None,
    data_overwrite: TNotificationData | None = None,
) -> GqlStatusObject:
    data = _raw_notification_factory(data, data_overwrite)
    return GqlStatusObject._from_notification_metadata(data)


@pytest.fixture
def status_notification_factory() -> TStatusNotificationFactory:
    return _status_notification_factory


def _status_notification_factory(
    data: TStatusNotificationData | None = None,
    data_overwrite: TStatusNotificationData | None = None,
    diag_rec_overwrite: TDiagnosticRecordData | None = None,
) -> GqlStatusObject:
    data = _raw_status_notification_factory(
        data, data_overwrite, diag_rec_overwrite
    )
    return GqlStatusObject._from_status_metadata(data)


@pytest.fixture
def raw_status_notification_factory() -> TRawStatusNotificationFactory:
    return _raw_status_notification_factory


def _raw_status_notification_factory(
    data: TStatusNotificationData | None = None,
    data_overwrite: TStatusNotificationData | None = None,
    diag_rec_overwrite: TDiagnosticRecordData | None = None,
) -> TStatusNotificationData:
    if data is None:
        data = _test_status_notification_data()
    if data_overwrite:
        data.update(data_overwrite)
    if diag_rec_overwrite:
        if data.get("diagnostic_record") is None:
            data["diagnostic_record"] = {}
        assert data["diagnostic_record"] is not None
        data["diagnostic_record"].update(diag_rec_overwrite)
    return data


def _test_notification_data() -> TNotificationData:
    return {
        "title": "Some title",
        "code": "Neo.Made.Up.Code",
        "description": "Some description",
        "severity": "INFORMATION",
        "category": "HINT",
        "position": {"offset": 0, "line": 1, "column": 1},
    }


def _test_status_notification_data() -> TStatusNotificationData:
    return {
        "gql_status": "03N42",
        "status_description": "Some status description",
        "neo4j_code": "Neo.Made.Up.Code",
        "title": "Some status title",
        "description": "Some notification description",
        "diagnostic_record": {
            "OPERATION": "",
            "OPERATION_code": "0",
            "CURRENT_SCHEMA": "/",
            "_status_parameters": {},
            "_severity": "INFORMATION",
            "_classification": "HINT",
            "_position": {"offset": 0, "line": 1, "column": 1},
        },
    }
