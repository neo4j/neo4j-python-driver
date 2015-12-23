import string
import random

import sys

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
        return value
    if type == 'Null':
        return None
    if type == 'Boolean':
        return bool(value)
    raise ValueError('No such type : %s', type)


def as_cypger_text(expected):
    if expected is None:
        return "Null"
    if isinstance(expected, unicode):
        return '"' + expected + '"'
    if isinstance(expected, float):
        return repr(expected).replace('+', '')
    if isinstance(expected, list):
        l = u'['
        for i, val in enumerate(expected):
            l += as_cypger_text(val)
            if i < len(expected)-1:
                l+= u','
        l += u']'
        return l
    if isinstance(expected, dict):
        d = u'{'
        for i, (key, val) in enumerate(expected.items()):
            d += unicode(key) + ':'
            d += as_cypger_text(val)
            if i < len(expected.items())-1:
                d+= u','
        d += u'}'
        return d
    else:
        return unicode(expected)


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
        args = [-sys.maxint - 1, sys.maxint]
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
        raise ValueError('No such type : %s', type)
    return (fu, args)


def get_list_of_random_type(size, type):
    fu, args = _get_random_func(type)
    return [fu(*args) for _ in range(size)]


def get_dict_of_random_type(size, type):
    fu, args = _get_random_func(type)
    map = {}
    for i in range(size):
        map['a' + str(i)] = fu(*args)
    return map
