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

import re

from ..... import _typing as t
from ....._exceptions import QueryApiHttpError
from .....spatial import (
    _srid_table,
    Point,
)
from .._common import (
    make_value_dict,
    value_as_str,
)


if t.TYPE_CHECKING:
    from .._common import ValueDict


FLOAT_RE = re.compile(
    r"[-+]?(?:\d*\.\d+|\d+\.?\d*)(?:[eE][-+]?\d+)?"
    r"|[-+]?(?i:nan)"
    r"|[-+]?(?i:inf(?:inity)?)"
)
POINT_RE = re.compile(
    r"^SRID=(\d+);"
    r"\s*POINT(?:\s+Z)?\s*\("
    rf"\s*({FLOAT_RE.pattern})"
    rf"\s+({FLOAT_RE.pattern})"
    rf"(?:\s+({FLOAT_RE.pattern}))?"
    rf"\)$"
)


def hydrate_point(value: object) -> Point:
    value = value_as_str(value)

    match = POINT_RE.match(value)
    if not match:
        raise QueryApiHttpError(
            f"expected spatial point string, got: {value!r}"
        )
    srid = int(match.group(1))
    coordinates = [float(match.group(2)), float(match.group(3))]
    z_str = match.group(4)
    if z_str is not None:
        coordinates.append(float(z_str))

    try:
        point_class, dim = _srid_table[srid]
    except KeyError:
        point = Point(coordinates)
        point.srid = srid
        return point
    else:
        if len(coordinates) != dim:
            raise QueryApiHttpError(
                f"SRID {srid} requires {dim} coordinates "
                f"({len(coordinates)} provided)"
            )
        return point_class(coordinates)


def dehydrate_point(value: Point) -> ValueDict[str]:
    if len(value) not in {2, 3}:
        raise ValueError(
            f"Cannot dehydrate Point with {len(value)} dimensions"
        )
    coordinates_str = " ".join(_dehydrate_coordinate(c) for c in value)
    point_type = "POINT" if len(value) == 2 else "POINT Z"
    value_str = f"SRID={value.srid};{point_type} ({coordinates_str})"
    return make_value_dict("Point", value_str)


def _dehydrate_coordinate(coord: float) -> str:
    return str(coord).replace("nan", "NaN").replace("inf", "Infinity")


__all__ = [
    "dehydrate_point",
    "hydrate_point",
]
