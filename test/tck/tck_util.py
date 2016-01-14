#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2016 "Neo Technology,"
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

import string
import random
import json
from neo4j.v1 import compat, Relationship, Node, Path

from neo4j.v1 import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost")


def send_string(text):
    session = driver.session()
    result = session.run(text)
    session.close()
    return result


def send_parameters(statement, parameters):
    session = driver.session()
    result = session.run(statement, parameters)
    session.close()
    return result


def get_bolt_value(type, value):
    if type == 'Integer':
        return int(value)
    if type == 'Float':
        return float(value)
    if type == 'String':
        return to_unicode(value)
    if type == 'Null':
        return None
    if type == 'Boolean':
        return bool(value)
    raise ValueError('No such type : %s' % type)


def as_cypher_text(expected):
    if expected is None:
        return "Null"
    if isinstance(expected, (str, compat.string)):
            return '"' + expected + '"'
    if isinstance(expected, float):
        return repr(expected).replace('+', '')
    if isinstance(expected, list):
        l = u'['
        for i, val in enumerate(expected):
            l += as_cypher_text(val)
            if i < len(expected)-1:
                l+= u','
        l += u']'
        return l
    if isinstance(expected, dict):
        d = u'{'
        for i, (key, val) in enumerate(expected.items()):
            d += to_unicode(key) + ':'
            d += as_cypher_text(val)
            if i < len(expected.items())-1:
                d+= u','
        d += u'}'
        return d
    else:
        return to_unicode(expected)


def get_list_from_feature_file(string_list, bolt_type):
    inputs = string_list.strip('[]')
    inputs = inputs.split(',')
    list_to_return = []
    for value in inputs:
        list_to_return.append(get_bolt_value(bolt_type, value))
    return list_to_return


def get_random_string(size):
    return u''.join(
            random.SystemRandom().choice(list(string.ascii_uppercase + string.digits + string.ascii_lowercase)) for _ in
            range(size))


def get_random_bool():
    return bool(random.randint(0, 1))


def _get_random_func(type):
    def get_none():
        return None

    if type == 'Integer':
        fu = random.randint
        args = [-9223372036854775808, 9223372036854775808]
    elif type == 'Float':
        fu = random.random
        args = []
    elif type == 'String':
        fu = get_random_string
        args = [3]
    elif type == 'Null':
        fu = get_none
        args = []
    elif type == 'Boolean':
        fu = get_random_bool
        args = []
    else:
        raise ValueError('No such type : %s' % type)
    return (fu, args)


def get_list_of_random_type(size, type):
    fu, args = _get_random_func(type)
    return [fu(*args) for _ in range(size)]


def get_dict_of_random_type(size, type):
    fu, args = _get_random_func(type)
    return {'a%d' % i: fu(*args) for i in range(size)}


def to_unicode(val):
    try:
        return unicode(val)
    except NameError:
        return str(val)


def get_properties(prop):
    split_at = prop.find('{')
    if split_at != -1:
        n_start, prop = split_two(prop, split_at)
        split_at = prop.find('}')
        if split_at == -1:
            raise ValueError("Node properties not correctly representetd. Found starrting '{' but no ending '}' in : %s" % properties)
        prop, n_end = split_two(prop, split_at+1)
        n = n_start + n_end
        properties = json.loads(prop)
        return properties, n
    else:
        return {}, prop


def get_labels(labels):
    labels = labels.split(':')
    if len(labels) == 1:
        return [], labels[0]
    n = labels[0]
    labels = labels[1:]
    if labels[-1].find(' ') != -1:
        labels[-1], n_end = split_two(labels[-1], labels[-1].find(' '))
        n += n_end
    return labels, n


def get_labels_and_properties(n):
    prop, n = get_properties(n)
    labels, n = get_labels(n)
    return labels, prop, n


def split_two(string_entity, split_at):
    return string_entity[:split_at], string_entity[split_at:]


def get_node(string_entity):
    if string_entity is None:
        return None, ''
    if string_entity[0] != "(":
        raise ValueError("Excpected node which shuld start with '('. Got: %s" % string_entity[0])
    split_at = string_entity.find(')')
    if split_at == -1:
        raise ValueError("Excpected node which shuld end with ')'. Found no such character in: %s" % string_entity)
    n , string_entity = split_two(string_entity, split_at+1)
    n = n[1:-1]
    labels, properties, n = get_labels_and_properties(n)
    node = Node(labels=labels, properties=properties)
    return node, string_entity


def get_relationship(string_entity):
    point_up = None
    if string_entity[:3] == "<-[":
        point_up = False
        string_entity = string_entity[3:]
        rel = string_entity[:string_entity.find(']-')]
        string_entity = string_entity[string_entity.find(']-')+2:]
    elif string_entity[:2] == "-[":
        point_up = True
        string_entity = string_entity[2:]
        rel = string_entity[:string_entity.find(']-')]
        string_entity = string_entity[string_entity.find(']->')+3:]
    elif string_entity[0] == "[":
        string_entity = string_entity[1:]
        rel = string_entity[:string_entity.find(']')]
        string_entity = string_entity[string_entity.find(']')+1:]
    else:
        raise ValueError("Cant identify relationship from: %s" %string_entity)
    type, properties, str = get_labels_and_properties(rel)
    if len(type) > 1:
        raise ValueError("Relationship can only have one type. Got: %s" % type)
    relationship = Relationship(1, 2, type[0], properties=properties)
    return relationship, string_entity, point_up


def get_path(string_path):
    n, string_path = get_node(string_path)
    list_of_nodes_and_rel = [n]
    n.identity = 0
    while string_path != '':
        r, string_path, point_up = get_relationship(string_path)
        n, string_path = get_node(string_path)
        n.identity = len(list_of_nodes_and_rel)
        if point_up:
            r.start = list_of_nodes_and_rel[-1].identity
            r.end = n.identity
        else:
            r.start = n.identity
            r.end = list_of_nodes_and_rel[-1].identity
        list_of_nodes_and_rel.append(r)
        list_of_nodes_and_rel.append(n)
    path = Path(list_of_nodes_and_rel[0], *list_of_nodes_and_rel[1:])
    return path, string_path


def record_value_to_comparable(val, func=None, rel=False):
    def get_val(v):
        if func is not None:
            return Value(func(v)[0])
        else:
            return Value(v)

    if val == 'null':
        return Value(None)
    if isinstance(val, compat.string) and val[0] == '[':
        if not rel or (rel and val[1] == '['):
            val = val[1:-1].split(", ")
    if isinstance(val,list):
        return tuple([get_val(v) for v in val])
    else:
        return get_val(val)

def result_to_set(result, func=None, rel=False):
    records = set([])
    for record in result:
        for i,_ in enumerate(record):
            cell = record_value_to_comparable(record[i], func, rel)
            records.add(cell)
    return records


def text_path_to_set(table):
    return result_to_set(table, get_path)


def text_relationship_to_set(table):
    return result_to_set(table, get_relationship, rel=True)


def text_node_to_set(table):
    return result_to_set(table, get_node)


def text_int_to_set(table):
    def to_int(a):
        return int(a), ''
    return result_to_set(table, to_int)


def text_string_to_set(table):
    return result_to_set(table)


def single_result_to_values(result):
    l = []

    for val in result[0].values():
        if not isinstance(val,list):
            val = [val]
        val = [Value(v) for v in val]
        l.append(tuple(val))
    return tuple(l)


def node_node_int_to_values(n1, n2, len):
    n1 = tuple([Value(get_node(n1)[0])])
    n2 = tuple([Value(get_node(n2)[0])])
    len = tuple([Value(int(len))])
    return tuple([n1,n2,len])


class Value:
    content = None

    def __init__(self, entity):
        self.content = {}
        if isinstance(entity, Node):
            self.content = self.create_node(entity)
        elif isinstance(entity, Relationship):
            self.content = self.create_relationship(entity)
        elif isinstance(entity, Path):
            self.content = self.create_path(entity)
        elif isinstance(entity, int) or isinstance(entity, float) or isinstance(entity, (str, compat.string)) or entity is None:
            self.content['value'] = entity
        else:
            raise ValueError("Do not support object type: %s" %entity)

    def __hash__(self):
        return hash(repr(self))

    def __eq__(self, other):
        assert isinstance(other, Value)
        return self.content == other.content

    def __repr__(self):
        return str(self.content)

    def create_node(self,entity):
        content = {'properties': entity.properties, 'labels': entity.labels, 'obj': "node"}

        return content

    def create_path(self,entity):
        content = {}
        prev_id = entity.start.identity
        p = []
        for i, rel in enumerate(list(entity)):
            n = entity.nodes[i+1]
            current_id = n.identity
            if rel.start == prev_id and rel.end == current_id:
                rel.start = i
                rel.end = i+1
            elif rel.start == current_id and rel.end == prev_id:
                rel.start = i+1
                rel.end = i
            else:
                raise ValueError("Relationships end and start should point to surrounding nodes. Rel: %s N1id: %s N2id: %s. At entity#%s" % (rel, current_id, prev_id, i))
            p += [self.create_relationship(rel, True),self.create_node(n)]
            prev_id = current_id
        content['path'] = p
        content['obj'] = "path"
        content['start'] = self.create_node(entity.start)
        return content

    def create_relationship(self,entity, include_start_end=False):
        content = {'obj': "relationship"}
        if include_start_end:
            self.content['start'] = entity.start
            self.content['end'] = entity.end
        content['type'] = entity.type
        content['properties'] = entity.properties
        return content

