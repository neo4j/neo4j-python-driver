#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2018 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
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


from __future__ import absolute_import

from collections import Mapping, OrderedDict, Set
from re import compile as re_compile
from unicodedata import category

from neo4j.compat import integer, string, unichr, unicode, ustr
from neo4j.v1.types.graph import Graph, Node, Relationship, Path


ID_START = {u"_"} | {unichr(x) for x in range(0xFFFF)
                     if category(unichr(x)) in ("LC", "Ll", "Lm", "Lo", "Lt", "Lu", "Nl")}
ID_CONTINUE = ID_START | {unichr(x) for x in range(0xFFFF)
                          if category(unichr(x)) in ("Mn", "Mc", "Nd", "Pc", "Sc")}

DOUBLE_QUOTE = u'"'
SINGLE_QUOTE = u"'"

ESCAPED_DOUBLE_QUOTE = u'\\"'
ESCAPED_SINGLE_QUOTE = u"\\'"

X_ESCAPE = re_compile(r"(\\x([0-9a-f]{2}))")
DOUBLE_QUOTED_SAFE = re_compile(r"([ -!#-\[\]-~]+)")
SINGLE_QUOTED_SAFE = re_compile(r"([ -&(-\[\]-~]+)")


class LabelSetView(Set):

    def __init__(self, elements=(), selected=(), **kwargs):
        self.__elements = frozenset(elements)
        self.__selected = tuple(selected)
        self.__kwargs = kwargs

    def __repr__(self):
        if self.__selected:
            return "".join(":%s" % cypher_escape(e, **self.__kwargs) for e in self.__selected if e in self.__elements)
        else:
            return "".join(":%s" % cypher_escape(e, **self.__kwargs) for e in sorted(self.__elements))

    def __getattr__(self, element):
        if element in self.__selected:
            return self.__class__(self.__elements, self.__selected)
        else:
            return self.__class__(self.__elements, self.__selected + (element,))

    def __len__(self):
        return len(self.__elements)

    def __iter__(self):
        return iter(self.__elements)

    def __contains__(self, element):
        return element in self.__elements


class PropertyDictView(Mapping):

    def __init__(self, items=(), selected=(), **kwargs):
        self.__items = dict(items)
        self.__selected = tuple(selected)
        self.__kwargs = kwargs

    def __repr__(self):
        if self.__selected:
            properties = OrderedDict((key, self.__items[key]) for key in self.__selected if key in self.__items)
        else:
            properties = OrderedDict((key, self.__items[key]) for key in sorted(self.__items))
        return cypher_repr(properties, **self.__kwargs)

    def __getattr__(self, key):
        if key in self.__selected:
            return self.__class__(self.__items, self.__selected)
        else:
            return self.__class__(self.__items, self.__selected + (key,))

    def __getitem__(self, key):
        return self.__items[key]

    def __len__(self):
        return len(self.__items)

    def __iter__(self):
        return iter(self.__items)

    def __contains__(self, key):
        return key in self.__items


class PropertySelector(object):

    def __init__(self, items=(), default_value=None, **kwargs):
        self.__items = dict(items)
        self.__default_value = default_value
        self.__kwargs = kwargs

    def __getattr__(self, key):
        return cypher_str(self.__items.get(key, self.__default_value), **self.__kwargs)


class CypherEncoder(object):

    __default_instance = None

    def __new__(cls, *args, **kwargs):
        if not kwargs:
            if cls.__default_instance is None:
                cls.__default_instance = super(CypherEncoder, cls).__new__(cls)
            return cls.__default_instance
        return super(CypherEncoder, cls).__new__(cls)

    encoding = "utf-8"
    quote = None
    sequence_separator = u", "
    key_value_separator = u": "
    node_template = u"_{id}{labels} {properties}"
    related_node_template = u"_{id}"
    relationship_template = u"{type} {properties}"

    def __init__(self, encoding=None, quote=None, sequence_separator=None, key_value_separator=None,
                 node_template=None, related_node_template=None, relationship_template=None):
        if encoding:
            self.encoding = encoding
        if quote:
            self.quote = quote
        if sequence_separator:
            self.sequence_separator = sequence_separator
        if key_value_separator:
            self.key_value_separator = key_value_separator
        if node_template:
            self.node_template = node_template
        if related_node_template:
            self.related_node_template = related_node_template
        if relationship_template:
            self.relationship_template = relationship_template

    def encode_key(self, key):
        if isinstance(key, bytes):
            key = key.decode(self.encoding)
        assert isinstance(key, unicode)
        if not key:
            raise ValueError("Keys cannot be empty")
        if key[0] in ID_START and all(key[i] in ID_CONTINUE for i in range(1, len(key))):
            return key
        else:
            return u"`" + key.replace(u"`", u"``") + u"`"

    def encode_value(self, value):
        if value is None:
            return u"null"
        if value is True:
            return u"true"
        if value is False:
            return u"false"
        if isinstance(value, integer):
            return unicode(value)
        if isinstance(value, float):
            return unicode(value)
        if isinstance(value, string):
            return self.encode_string(value)
        if isinstance(value, bytearray):
            return self.encode_byte_array(value)
        if isinstance(value, Graph):
            return self.encode_graph(value)
        if isinstance(value, Node):
            return self.encode_node(value)
        if isinstance(value, Relationship):
            return self.encode_relationship(value)
        if isinstance(value, Path):
            return self.encode_path(value)
        if isinstance(value, list):
            return self.encode_list(value)
        if isinstance(value, dict):
            return self.encode_map(value)
        raise TypeError("Values of type %s are not supported" % value.__class__.__name__)

    def encode_byte_array(self, value):
        return u"bytearray([%s])" % self.sequence_separator.join(map(lambda x: "0x%02X" % x, value))

    def encode_string(self, value):
        if isinstance(value, bytes):
            value = value.decode(self.encoding)
        assert isinstance(value, unicode)

        quote = self.quote
        if quote is None:
            num_single = value.count(u"'")
            num_double = value.count(u'"')
            quote = SINGLE_QUOTE if num_single <= num_double else DOUBLE_QUOTE

        if quote == SINGLE_QUOTE:
            escaped_quote = ESCAPED_SINGLE_QUOTE
            safe = SINGLE_QUOTED_SAFE
        elif quote == DOUBLE_QUOTE:
            escaped_quote = ESCAPED_DOUBLE_QUOTE
            safe = DOUBLE_QUOTED_SAFE
        else:
            raise ValueError("Unsupported quote character %r" % quote)

        if not value:
            return quote + quote

        parts = safe.split(value)
        for i in range(0, len(parts), 2):
            parts[i] = (X_ESCAPE.sub(u"\\\\u00\\2", parts[i].encode("unicode-escape").decode("utf-8")).
                        replace(quote, escaped_quote).replace(u"\\u0008", u"\\b").replace(u"\\u000c", u"\\f"))
        return quote + u"".join(parts) + quote

    def encode_list(self, values):
        return u"[" + self.sequence_separator.join(map(self.encode_value, values)) + u"]"

    def encode_map(self, values):
        return u"{" + self.sequence_separator.join(self.encode_key(key) + self.key_value_separator + self.encode_value(value)
                                                   for key, value in values.items()) + u"}"

    def encode_node(self, node):
        return self._encode_node(node, self.node_template)

    def encode_relationship(self, relationship):
        nodes = relationship.nodes
        return u"{}-{}->{}".format(
            self._encode_node(nodes[0], self.related_node_template),
            self._encode_relationship_detail(relationship, self.relationship_template),
            self._encode_node(nodes[-1], self.related_node_template),
        )

    def encode_path(self, path):
        encoded = []
        append = encoded.append
        nodes = path.nodes
        for i, relationship in enumerate(path.relationships):
            append(self._encode_node(nodes[i], self.related_node_template))
            related_nodes = relationship.nodes
            if self._node_id(related_nodes[0]) == self._node_id(nodes[i]):
                append(u"-")
                append(self._encode_relationship_detail(relationship, self.relationship_template))
                append(u"->")
            else:
                append(u"<-")
                append(self._encode_relationship_detail(relationship, self.relationship_template))
                append(u"-")
        append(self._encode_node(nodes[-1], self.related_node_template))
        return u"".join(encoded)

    def encode_graph(self, graph):
        return u"<Graph order=%d size=%d>" % (len(graph.nodes), len(graph.relationships))

    @classmethod
    def _node_id(cls, node):
        return node.id if hasattr(node, "id") else node

    def _encode_node(self, node, template):
        return u"(" + template.format(
            id=node.id,
            labels=LabelSetView(node.labels, encoding=self.encoding, quote=self.quote),
            properties=PropertyDictView(node, encoding=self.encoding, quote=self.quote),
            property=PropertySelector(node, u""),
        ).strip() + u")"

    def _encode_relationship_detail(self, relationship, template):
        return u"[" + template.format(
            id=relationship.id,
            type=u":" + ustr(type(relationship).__name__),
            properties=PropertyDictView(relationship, encoding=self.encoding, quote=self.quote),
            property=PropertySelector(relationship, u""),
        ).strip() + u"]"


def cypher_escape(identifier, **kwargs):
    """ Escape a Cypher identifier in backticks.

    ::

        >>> cypher_escape("simple_identifier")
        'simple_identifier'
        >>> cypher_escape("identifier with spaces")
        '`identifier with spaces`'
        >>> cypher_escape("identifier with `backticks`")
        '`identifier with ``backticks```'

    :arg identifier: any non-empty string
    """
    if not isinstance(identifier, string):
        raise TypeError(type(identifier).__name__)
    encoder = CypherEncoder(**kwargs)
    return encoder.encode_key(identifier)


def cypher_repr(value, **kwargs):
    """ Return the Cypher representation of a value.
    """
    encoder = CypherEncoder(**kwargs)
    return encoder.encode_value(value)


def cypher_str(value, **kwargs):
    """ Convert a Cypher value to a Python Unicode string.
    """
    if isinstance(value, unicode):
        return value
    elif isinstance(value, bytes):
        return value.decode(kwargs.get("encoding", "utf-8"))
    else:
        return cypher_repr(value, **kwargs)
