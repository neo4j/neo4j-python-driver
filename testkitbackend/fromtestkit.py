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

from datetime import timedelta

import pytz

import neo4j
from neo4j import (
    NotificationDisabledCategory,
    NotificationMinimumSeverity,
    Query,
)
from neo4j.auth_management import ClientCertificate
from neo4j.spatial import (
    CartesianPoint,
    WGS84Point,
)
from neo4j.time import (
    Date,
    DateTime,
    Duration,
    Time,
)

from ._preview_imports import NotificationDisabledClassification
from ._warning_check import warnings_check


def to_cypher_and_params(data):
    from .backend import Request

    params = data.get("params")
    # Optional
    if params is None:
        return data["cypher"], None
    # Transform the params to Python native
    params_dict = {k: to_param(v) for k, v in params.items()}
    if isinstance(params, Request):
        params.mark_all_as_read()
    return data["cypher"], params_dict


def to_tx_kwargs(data):
    from .backend import Request

    kwargs = {}
    if "txMeta" in data:
        metadata = data["txMeta"]
        kwargs["metadata"] = metadata
        if metadata is not None:
            kwargs["metadata"] = {k: to_param(v) for k, v in metadata.items()}
        if isinstance(metadata, Request):
            metadata.mark_all_as_read()
    if "timeout" in data:
        kwargs["timeout"] = data["timeout"]
        if kwargs["timeout"] is not None:
            kwargs["timeout"] /= 1000
    return kwargs


def to_query_and_params(data):
    cypher, param = to_cypher_and_params(data)
    tx_kwargs = to_tx_kwargs(data)
    query = Query(cypher, **tx_kwargs)
    return query, param


def to_param(m):
    # Convert testkit parameter format to driver (python) parameter.
    data = m["data"]
    name = m["name"]
    if name == "CypherNull":
        if data["value"] is not None:
            raise ValueError("CypherNull should be None")
        return None
    if name == "CypherString":
        return str(data["value"])
    if name == "CypherBool":
        return bool(data["value"])
    if name == "CypherInt":
        return int(data["value"])
    if name == "CypherFloat":
        return float(data["value"])
    if name == "CypherString":
        return str(data["value"])
    if name == "CypherBytes":
        return bytearray([int(byte, 16) for byte in data["value"].split()])
    if name == "CypherList":
        return [to_param(v) for v in data["value"]]
    if name == "CypherMap":
        return {k: to_param(data["value"][k]) for k in data["value"]}
    if name == "CypherPoint":
        coords = [data["x"], data["y"]]
        if data.get("z") is not None:
            coords.append(data["z"])
        if data["system"] == "cartesian":
            return CartesianPoint(coords)
        if data["system"] == "wgs84":
            return WGS84Point(coords)
        raise ValueError("Unknown point system: {}".format(data["system"]))
    if name == "CypherDate":
        return Date(data["year"], data["month"], data["day"])
    if name == "CypherTime":
        tz = None
        utc_offset_s = data.get("utc_offset_s")
        if utc_offset_s is not None:
            utc_offset_m = utc_offset_s // 60
            if utc_offset_m * 60 != utc_offset_s:
                raise ValueError(
                    "the used timezone library only supports "
                    "UTC offsets by minutes"
                )
            tz = pytz.FixedOffset(utc_offset_m)
        return Time(
            data["hour"],
            data["minute"],
            data["second"],
            data["nanosecond"],
            tzinfo=tz,
        )
    if name == "CypherDateTime":
        datetime = DateTime(
            data["year"],
            data["month"],
            data["day"],
            data["hour"],
            data["minute"],
            data["second"],
            data["nanosecond"],
        )
        utc_offset_s = data["utc_offset_s"]
        timezone_id = data["timezone_id"]
        if timezone_id is not None:
            utc_offset = timedelta(seconds=utc_offset_s)
            tz = pytz.timezone(timezone_id)
            localized_datetime = tz.localize(datetime, is_dst=False)
            if localized_datetime.utcoffset() == utc_offset:
                return localized_datetime
            localized_datetime = tz.localize(datetime, is_dst=True)
            if localized_datetime.utcoffset() == utc_offset:
                return localized_datetime
            raise ValueError(
                f"cannot localize datetime {datetime} to timezone "
                f"{timezone_id} with UTC offset {utc_offset}"
            )
        elif utc_offset_s is not None:
            utc_offset_m = utc_offset_s // 60
            if utc_offset_m * 60 != utc_offset_s:
                raise ValueError(
                    "the used timezone library only supports "
                    "UTC offsets by minutes"
                )
            tz = pytz.FixedOffset(utc_offset_m)
            return tz.localize(datetime)
        return datetime
    if name == "CypherDuration":
        return Duration(
            months=data["months"],
            days=data["days"],
            seconds=data["seconds"],
            nanoseconds=data["nanoseconds"],
        )
    raise ValueError("Unknown param type " + name)


def to_auth_token(data, key):
    if data[key] is None:
        return None
    auth_token = data[key]["data"]
    data[key].mark_item_as_read_if_equals("name", "AuthorizationToken")
    scheme = auth_token["scheme"]
    if scheme == "basic":
        auth = neo4j.basic_auth(
            auth_token["principal"],
            auth_token["credentials"],
            realm=auth_token.get("realm", None),
        )
    elif scheme == "kerberos":
        auth = neo4j.kerberos_auth(auth_token["credentials"])
    elif scheme == "bearer":
        auth = neo4j.bearer_auth(auth_token["credentials"])
    else:
        auth = neo4j.custom_auth(
            auth_token["principal"],
            auth_token["credentials"],
            auth_token["realm"],
            auth_token["scheme"],
            **auth_token.get("parameters", {}),
        )
        auth_token.mark_item_as_read("parameters", recursive=True)
    return auth


def to_client_cert(data, key) -> ClientCertificate | None:
    if data[key] is None:
        return None
    data[key].mark_item_as_read_if_equals("name", "ClientCertificate")
    cert_data = data[key]["data"]
    with warnings_check(
        ((neo4j.PreviewWarning, r"Mutual TLS is a preview feature\."),)
    ):
        return ClientCertificate(
            cert_data["certfile"], cert_data["keyfile"], cert_data["password"]
        )


def set_notifications_config(config, data):
    if "notificationsMinSeverity" in data:
        config["notifications_min_severity"] = NotificationMinimumSeverity[
            data["notificationsMinSeverity"]
        ]
    if "notificationsDisabledCategories" in data:
        config["notifications_disabled_categories"] = [
            NotificationDisabledCategory[c]
            for c in data["notificationsDisabledCategories"]
        ]
    if "notificationsDisabledClassifications" in data:
        config["notifications_disabled_classifications"] = [
            NotificationDisabledClassification[c]
            for c in data["notificationsDisabledClassifications"]
        ]
