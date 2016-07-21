import re
import base36

from .. import types as v1_types
from .relationship import Relationship


class neo4j_type(type):
    def __new__(cls, name, bases, dct):
        if bases[0] is not v1_types.Node:
            cls._collect_relationships(dct)
            cls._build_label(dct, name)
        return type.__new__(cls, name, bases, dct)

    def __init__(cls, name, bases, dct):
        if bases[0] is not v1_types.Node:
            cls._props_schema = cls.Properties()
        return type.__init__(cls, name, bases, dct)

    def _collect_relationships(dct):
        dct['relationships'] = {
                k: v for k, v in dct.items()
                if isinstance(v, Relationship)
                }

    def _build_label(dct, cls_name):
        if 'labels' in dct:
            dct['label'] = ':'.join(cls.labels) or name
        else:
            dct['labels'] = [cls_name]
            dct['label'] = cls_name
