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

import typing as t
from abc import (
    ABCMeta,
    abstractmethod,
)
from collections.abc import (
    Mapping,
    Sequence,
    Set,
)
from functools import reduce
from operator import xor as xor_operator

from ._codec.hydration import BrokenHydrationObject
from ._conf import iter_items
from ._meta import deprecated
from .exceptions import BrokenRecordError
from .graph import (
    Node,
    Path,
    Relationship,
)


_T = t.TypeVar("_T")
_K = t.Union[int, str]


class Record(tuple, Mapping):
    """ A :class:`.Record` is an immutable ordered collection of key-value
    pairs. It is generally closer to a :py:class:`namedtuple` than to a
    :py:class:`OrderedDict` in as much as iteration of the collection will
    yield values rather than keys.
    """

    __keys: t.Tuple[str]

    def __new__(cls, iterable=()):
        keys = []
        values = []
        for key, value in iter_items(iterable):
            keys.append(key)
            values.append(value)
        inst = tuple.__new__(cls, values)
        inst.__keys = tuple(keys)
        return inst

    def _broken_record_error(self, index):
        return BrokenRecordError(
            f"Record contains broken data at {index} ('{self.__keys[index]}')"
        )

    def _super_getitem_single(self, index):
        value = super().__getitem__(index)
        if isinstance(value, BrokenHydrationObject):
            raise self._broken_record_error(index) from value.error
        return value

    def __repr__(self) -> str:
        return "<%s %s>" % (
            self.__class__.__name__,
            " ".join("%s=%r" % (field, value)
                     for field, value in zip(self.__keys, super().__iter__()))
        )

    __str__ = __repr__

    def __eq__(self, other: object) -> bool:
        """ In order to be flexible regarding comparison, the equality rules
        for a record permit comparison with any other Sequence or Mapping.

        :param other:
        :returns:
        """
        compare_as_sequence = isinstance(other, Sequence)
        compare_as_mapping = isinstance(other, Mapping)
        if compare_as_sequence and compare_as_mapping:
            other = t.cast(t.Mapping, other)
            return list(self) == list(other) and dict(self) == dict(other)
        elif compare_as_sequence:
            other = t.cast(t.Sequence, other)
            return list(self) == list(other)
        elif compare_as_mapping:
            other = t.cast(t.Mapping, other)
            return dict(self) == dict(other)
        else:
            return False

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __hash__(self):
        return reduce(xor_operator, map(hash, self.items()))

    def __iter__(self) -> t.Iterator[t.Any]:
        for i, v in enumerate(super().__iter__()):
            if isinstance(v, BrokenHydrationObject):
                raise self._broken_record_error(i) from v.error
            yield v

    def __getitem__(  # type: ignore[override]
        self, key: t.Union[_K, slice]
    ) -> t.Any:
        if isinstance(key, slice):
            keys = self.__keys[key]
            values = super().__getitem__(key)
            return self.__class__(zip(keys, values))
        try:
            index = self.index(key)
        except IndexError:
            return None
        else:
            return self._super_getitem_single(index)

    # TODO: 6.0 - remove
    @deprecated("This method is deprecated and will be removed in the future.")
    def __getslice__(self, start, stop):
        key = slice(start, stop)
        keys = self.__keys[key]
        values = tuple(self)[key]
        return self.__class__(zip(keys, values))

    def get(self, key: str, default: t.Optional[object] = None) -> t.Any:
        """ Obtain a value from the record by key, returning a default
        value if the key does not exist.

        :param key: a key
        :param default: default value

        :returns: a value
        """
        try:
            index = self.__keys.index(str(key))
        except ValueError:
            return default
        if 0 <= index < len(self):
            return self._super_getitem_single(index)
        else:
            return default

    def index(self, key: _K) -> int:  # type: ignore[override]
        """ Return the index of the given item.

        :param key: a key

        :returns: index
        """
        if isinstance(key, int):
            if 0 <= key < len(self.__keys):
                return key
            raise IndexError(key)
        elif isinstance(key, str):
            try:
                return self.__keys.index(key)
            except ValueError as exc:
                raise KeyError(key) from exc
        else:
            raise TypeError(key)

    def value(
        self, key: _K = 0, default: t.Optional[object] = None
    ) -> t.Any:
        """ Obtain a single value from the record by index or key. If no
        index or key is specified, the first value is returned. If the
        specified item does not exist, the default value is returned.

        :param key: an index or key
        :param default: default value

        :returns: a single value
        """
        try:
            index = self.index(key)
        except (IndexError, KeyError):
            return default
        else:
            return self[index]

    def keys(self) -> t.List[str]:  # type: ignore[override]
        """ Return the keys of the record.

        :returns: list of key names
        """
        return list(self.__keys)

    def values(self, *keys: _K) -> t.List[t.Any]:  # type: ignore[override]
        """ Return the values of the record, optionally filtering to
        include only certain values by index or key.

        :param keys: indexes or keys of the items to include; if none
                     are provided, all values will be included

        :returns: list of values
        """
        if keys:
            d: t.List[t.Any] = []
            for key in keys:
                try:
                    i = self.index(key)
                except KeyError:
                    d.append(None)
                else:
                    d.append(self[i])
            return d
        return list(self)

    def items(self, *keys):
        """ Return the fields of the record as a list of key and value tuples

        :returns: a list of value tuples
        """
        if keys:
            d = []
            for key in keys:
                try:
                    i = self.index(key)
                except KeyError:
                    d.append((key, None))
                else:
                    d.append((self.__keys[i], self[i]))
            return d
        return list((self.__keys[i], self._super_getitem_single(i))
                    for i in range(len(self)))

    def data(self, *keys: _K) -> t.Dict[str, t.Any]:
        """ Return the keys and values of this record as a dictionary,
        optionally including only certain values by index or key. Keys
        provided in the items that are not in the record will be
        inserted with a value of :data:`None`; indexes provided
        that are out of bounds will trigger an :exc:`IndexError`.

        :param keys: indexes or keys of the items to include; if none
                      are provided, all values will be included

        :returns: dictionary of values, keyed by field name

        :raises: :exc:`IndexError` if an out-of-bounds index is specified
        """
        return RecordExporter().transform(dict(self.items(*keys)))


class DataTransformer(metaclass=ABCMeta):
    """ Abstract base class for transforming data from one form into
    another.
    """

    @abstractmethod
    def transform(self, x):
        """ Transform a value, or collection of values.

        :param x: input value
        :returns: output value
        """


class RecordExporter(DataTransformer):
    """ Transformer class used by the :meth:`.Record.data` method.
    """

    def transform(self, x):
        if isinstance(x, Node):
            return self.transform(dict(x))
        elif isinstance(x, Relationship):
            return (self.transform(dict(x.start_node)),
                    x.__class__.__name__,
                    self.transform(dict(x.end_node)))
        elif isinstance(x, Path):
            path = [self.transform(x.start_node)]
            for i, relationship in enumerate(x.relationships):
                path.append(self.transform(relationship.__class__.__name__))
                path.append(self.transform(x.nodes[i + 1]))
            return path
        elif isinstance(x, str):
            return x
        elif isinstance(x, Sequence):
            typ = type(x)
            return typ(map(self.transform, x))
        elif isinstance(x, Set):
            typ = type(x)
            return typ(map(self.transform, x))
        elif isinstance(x, Mapping):
            typ = type(x)
            return typ((k, self.transform(v)) for k, v in x.items())
        else:
            return x


class RecordTableRowExporter(DataTransformer):
    """Transformer class used by the :meth:`.Result.to_df` method."""

    def transform(self, x):
        assert isinstance(x, Mapping)
        typ = type(x)
        return typ(item
                   for k, v in x.items()
                   for item in self._transform(
                       v, prefix=k.replace("\\", "\\\\").replace(".", "\\.")
                   ).items())

    def _transform(self, x, prefix):
        if isinstance(x, Node):
            res = {
                "%s().element_id" % prefix: x.element_id,
                "%s().labels" % prefix: x.labels,
            }
            res.update(("%s().prop.%s" % (prefix, k), v) for k, v in x.items())
            return res
        elif isinstance(x, Relationship):
            res = {
                "%s->.element_id" % prefix: x.element_id,
                "%s->.start.element_id" % prefix: x.start_node.element_id,
                "%s->.end.element_id" % prefix: x.end_node.element_id,
                "%s->.type" % prefix: x.__class__.__name__,
            }
            res.update(("%s->.prop.%s" % (prefix, k), v) for k, v in x.items())
            return res
        elif isinstance(x, Path) or isinstance(x, str):
            return {prefix: x}
        elif isinstance(x, Sequence):
            return dict(
                item
                for i, v in enumerate(x)
                for item in self._transform(
                    v, prefix="%s[].%i" % (prefix, i)
                ).items()
            )
        elif isinstance(x, Mapping):
            typ = type(x)
            return typ(
                item
                for k, v in x.items()
                for item in self._transform(
                    v, prefix="%s{}.%s" % (prefix, k.replace("\\", "\\\\")
                                                    .replace(".", "\\."))
                ).items()
            )
        else:
            return {prefix: x}
