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

from datetime import (
    date,
    datetime,
    time,
    timedelta,
)
from logging import getLogger

from ..... import _typing as t
from ....._exceptions import QueryApiHttpError
from ....._optional_deps import (
    np,
    pd,
)
from .....graph import (
    Graph,
    Node,
    Path,
    Relationship,
)
from .....spatial import (
    CartesianPoint,
    Point,
    WGS84Point,
)
from .....time import (
    Date,
    DateTime,
    Duration,
    Time,
)
from .....vector import Vector
from ...._types import (
    BYTES_TYPES,
    FALSE_VALUES,
    FLOAT_TYPES,
    FLOAT_TYPES_EXACT,
    INT_TYPES,
    INT_TYPES_EXACT,
    MAPPING_TYPES,
    NON_PD_SEQUENCE_TYPES,
    NONE_VALUES,
    PD_SEQUENCE_TYPES,
    TRUE_VALUES,
)
from ..._common import ExactValueKey
from .._common import (
    GraphHydratorHttp,
    HydrationHandlerHttpBase,
    HydrationScopeHttp,
    LiteralJson,
    LiteralJsonRecursive,
    make_value_dict,
    value_as_dict,
    value_as_list,
    value_as_list_str,
    value_as_str,
)
from . import (
    base_types,
    spatial,
    temporal,
    vector,
)


if t.TYPE_CHECKING:
    from .._common import (
        T_JSON_BOUND,
        ValueDict,
    )


log = getLogger("neo4j")


class _GraphHydrator(GraphHydratorHttp):
    def __init__(self) -> None:
        super().__init__()
        self.struct_hydration_functions = {
            **self.struct_hydration_functions,
            "Node": self.hydrate_node,
            "Relationship": self.hydrate_relationship,
            "Path": self.hydrate_path,
        }

    def hydrate_node(self, value: object) -> Node:
        assert isinstance(self.graph, Graph)
        value = value_as_dict(value)

        element_id = value.get("_element_id")
        labels = value.get("_labels")
        properties = value.get("_properties")

        element_id = value_as_str(element_id)
        labels = value_as_list_str(labels)
        properties = value_as_dict(properties)

        return self._hydrate_node(element_id, labels, properties)

    def _hydrate_node(
        self,
        element_id: str,
        labels: list[str] | None = None,
        properties: dict | None = None,
    ) -> Node:
        assert isinstance(self.graph, Graph)
        id_ = 0  # Query API/HTTP provides no id

        try:
            inst = self.graph._nodes[element_id]
        except KeyError:
            inst = Node(self.graph, element_id, id_, labels, properties)
            self.graph._nodes[element_id] = inst
        else:
            if labels:
                inst._labels = inst._labels.union(labels)  # frozen_set
            if properties:
                inst._properties.update(properties)
        return inst

    def hydrate_relationship(self, value: object) -> Relationship:
        assert isinstance(self.graph, Graph)
        value = value_as_dict(value)

        element_id = value.get("_element_id")
        n0_element_id = value.get("_start_node_element_id")
        n1_element_id = value.get("_end_node_element_id")
        type_ = value.get("_type")
        properties = value.get("_properties")

        element_id = value_as_str(element_id)
        n0_element_id = value_as_str(n0_element_id)
        n1_element_id = value_as_str(n1_element_id)
        type_ = value_as_str(type_)
        properties = value_as_dict(properties)

        return self._hydrate_relationship(
            element_id, n0_element_id, n1_element_id, type_, properties
        )

    def _hydrate_relationship(
        self,
        element_id: str,
        n0_element_id: str,
        n1_element_id: str,
        type_: str,
        properties: dict,
    ) -> Relationship:
        assert isinstance(self.graph, Graph)
        id_ = 0  # Query API/HTTP provides no id

        existing = self.graph._relationships.get(element_id, None)
        if existing is not None:
            existing_n0_element_id = None
            existing_n1_element_id = None
            if existing._start_node is not None:
                existing_n0_element_id = existing._start_node.element_id
            if existing._end_node is not None:
                existing_n1_element_id = existing._end_node.element_id
            if (
                existing.type == type_
                and n0_element_id == existing_n0_element_id
                and n1_element_id == existing_n1_element_id
            ):
                existing._properties.update(properties)
                return existing
            log.warning(
                "Conflicting relationship data for element id %r. "
                "Existing: %r-[:%r]->%r. New: %r-[:%r]->%r",
                element_id,
                existing_n0_element_id,
                existing.type,
                existing_n1_element_id,
                n0_element_id,
                type_,
                n1_element_id,
            )

        r = self.graph.relationship_type(type_)
        inst = r(self.graph, element_id, id_, properties)
        self.graph._relationships[element_id] = inst
        inst._start_node = self._hydrate_node(n0_element_id)
        inst._end_node = self._hydrate_node(n1_element_id)
        return inst

    def hydrate_path(self, value: object) -> Path:
        assert isinstance(self.graph, Graph)
        value = value_as_list(value)

        if len(value) % 2 != 1:
            raise QueryApiHttpError(
                f"expected path value to have odd length, got: {value!r}"
            )

        nodes = value[::2]
        for v in nodes:
            if not isinstance(v, Node):
                raise QueryApiHttpError(
                    f"expected path node at even index, got: {v!r}"
                )
        relationships = value[1::2]
        for i, v in enumerate(relationships):
            i = i * 2 + 1  # map back to original index
            if not isinstance(v, Relationship):
                raise QueryApiHttpError(
                    f"expected path relationship at odd index, got: {v!r}"
                )
            n0 = value[i - 1]
            n1 = value[i + 1]
            if v.nodes not in {(n0, n1), (n1, n0)}:
                raise QueryApiHttpError(
                    "relationship does not connect surrounding nodes, got: "
                    f"{n0!r} - {v!r} - {n1!r}"
                )

        return Path(nodes[0], *relationships)


class HydrationHandler(HydrationHandlerHttpBase):
    def __init__(self) -> None:
        super().__init__()
        self.struct_hydration_functions = {
            **self.struct_hydration_functions,
            "Null": base_types.hydrate_null,
            "Boolean": base_types.hydrate_boolean,
            "Integer": base_types.hydrate_integer,
            "Float": base_types.hydrate_float,
            "String": base_types.hydrate_string,
            "Base64": base_types.hydrate_byte_array,
            "List": base_types.hydrate_list,
            "Map": base_types.hydrate_map,
            "Date": temporal.hydrate_date,
            "Time": temporal.hydrate_zoned_time,
            "LocalTime": temporal.hydrate_local_time,
            "OffsetDateTime": temporal.hydrate_offset_datetime,
            "ZonedDateTime": temporal.hydrate_zoned_datetime,
            "LocalDateTime": temporal.hydrate_local_datetime,
            "Duration": temporal.hydrate_duration,
            "Point": spatial.hydrate_point,
        }
        self.dehydration_hooks.update(
            exact_values={
                **dict.fromkeys(
                    map(ExactValueKey, NONE_VALUES), base_types.dehydrate_null
                ),
                **dict.fromkeys(
                    map(ExactValueKey, TRUE_VALUES), base_types.dehydrate_true
                ),
                **dict.fromkeys(
                    map(ExactValueKey, FALSE_VALUES),
                    base_types.dehydrate_false,
                ),
            },
            exact_types={
                **dict.fromkeys(FLOAT_TYPES_EXACT, base_types.dehydrate_float),
                **dict.fromkeys(INT_TYPES_EXACT, base_types.dehydrate_integer),
                str: base_types.dehydrate_string,
                **dict.fromkeys(BYTES_TYPES, base_types.dehydrate_byte_array),
                **dict.fromkeys(
                    PD_SEQUENCE_TYPES, self._dehydrate_pd_sequence
                ),
                **dict.fromkeys(
                    NON_PD_SEQUENCE_TYPES, self._dehydrate_sequence
                ),
                tuple: self._dehydrate_sequence,
                **dict.fromkeys(MAPPING_TYPES, self._dehydrate_mapping),
                Point: spatial.dehydrate_point,
                CartesianPoint: spatial.dehydrate_point,
                WGS84Point: spatial.dehydrate_point,
                Date: temporal.dehydrate_date,
                date: temporal.dehydrate_date,
                Time: temporal.dehydrate_time,
                time: temporal.dehydrate_time,
                DateTime: temporal.dehydrate_datetime,
                datetime: temporal.dehydrate_datetime,
                Duration: temporal.dehydrate_duration,
                timedelta: temporal.dehydrate_timedelta,
                Vector: vector.dehydrate_vector,
                LiteralJson: self._dehydrate_literal_json,
                LiteralJsonRecursive: self._dehydrate_literal_json_recursive,
            },
            subtypes={
                **dict.fromkeys(FLOAT_TYPES, base_types.dehydrate_float),
                **dict.fromkeys(INT_TYPES, base_types.dehydrate_integer),
                str: base_types.dehydrate_string,
                **dict.fromkeys(BYTES_TYPES, base_types.dehydrate_byte_array),
                **dict.fromkeys(
                    PD_SEQUENCE_TYPES, self._dehydrate_pd_sequence
                ),
                **dict.fromkeys(
                    NON_PD_SEQUENCE_TYPES, self._dehydrate_sequence
                ),
                tuple: self._dehydrate_sequence,
                **dict.fromkeys(MAPPING_TYPES, self._dehydrate_mapping),
                object: self._dehydrate_unsupported,
            },
        )
        if np is not None:
            self.dehydration_hooks.update(
                exact_types={
                    np.datetime64: temporal.dehydrate_np_datetime,
                    np.timedelta64: temporal.dehydrate_np_timedelta,
                }
            )
        if pd is not None:
            self.dehydration_hooks.update(
                exact_types={
                    pd.Timestamp: temporal.dehydrate_datetime,
                    pd.Timedelta: temporal.dehydrate_pandas_timedelta,
                    type(pd.NaT): lambda _: make_value_dict("Null", None),
                }
            )

    def _dehydrate_mapping(self, value: t.Any) -> ValueDict[dict]:
        return base_types.dehydrate_map(value, self)

    def _dehydrate_sequence(self, value: t.Any) -> ValueDict[list]:
        return base_types.dehydrate_list(value, self)

    def _dehydrate_pd_sequence(self, value: t.Any) -> ValueDict[list]:
        return base_types.dehydrate_pd_list(value, self)

    def _dehydrate_literal_json(
        self, value: LiteralJson[T_JSON_BOUND]
    ) -> T_JSON_BOUND | list | dict:
        inner_value = value.value
        if isinstance(inner_value, NON_PD_SEQUENCE_TYPES):
            return base_types.dehydrate_raw_list(inner_value, self)
        if isinstance(inner_value, MAPPING_TYPES):
            return base_types.dehydrate_raw_map(inner_value, self)
        if isinstance(inner_value, PD_SEQUENCE_TYPES):
            return base_types.dehydrate_raw_pd_list(inner_value, self)
        return inner_value

    @staticmethod
    def _dehydrate_literal_json_recursive(
        value: LiteralJsonRecursive[T_JSON_BOUND],
    ) -> T_JSON_BOUND:
        return value.value

    @staticmethod
    def _dehydrate_unsupported(value: object) -> t.Never:
        raise ValueError(f"Values of type {type(value)} are not supported")

    def new_hydration_scope(self) -> HydrationScopeHttp:
        return HydrationScopeHttp(self, _GraphHydrator())
