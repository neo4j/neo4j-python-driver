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


from datetime import timedelta

import pytz

from neo4j import Query
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


def to_cypher_and_params(data):
    from .backend import Request
    params = data["params"]
    # Optional
    if params is None:
        return data["cypher"], None
    # Transform the params to Python native
    params_dict = {p: to_param(params[p]) for p in params}
    return data["cypher"], params_dict


def to_tx_kwargs(data):
    from .backend import Request
    kwargs = {}
    if "txMeta" in data:
        kwargs["metadata"] = data["txMeta"]
        if isinstance(kwargs["metadata"], Request):
            kwargs["metadata"].mark_all_as_read()
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
    """ Converts testkit parameter format to driver (python) parameter
    """
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
                raise ValueError("the used timezone library only supports "
                                 "UTC offsets by minutes")
            tz = pytz.FixedOffset(utc_offset_m)
        return Time(data["hour"], data["minute"], data["second"],
                    data["nanosecond"], tzinfo=tz)
    if name == "CypherDateTime":
        datetime = DateTime(
            data["year"], data["month"], data["day"],
            data["hour"], data["minute"], data["second"], data["nanosecond"]
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
                "cannot localize datetime %s to timezone %s with UTC "
                "offset %s" % (datetime, timezone_id, utc_offset)
            )
        elif utc_offset_s is not None:
            utc_offset_m = utc_offset_s // 60
            if utc_offset_m * 60 != utc_offset_s:
                raise ValueError("the used timezone library only supports "
                                 "UTC offsets by minutes")
            tz = pytz.FixedOffset(utc_offset_m)
            return tz.localize(datetime)
        return datetime
    if name == "CypherDuration":
        return Duration(
            months=data["months"], days=data["days"],
            seconds=data["seconds"], nanoseconds=data["nanoseconds"]
        )
    raise ValueError("Unknown param type " + name)
