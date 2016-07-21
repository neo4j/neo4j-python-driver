import re
import base36

from threading import Lock
from neo4j.v1 import types as v1_types

from .symbol import Symbol
from .meta import neo4j_type
from .schema import Schema
from .util import build_props_str


RE_NONWORD_CHARS = re.compile(r'\W+')


class Node(v1_types.Node, metaclass=neo4j_type):
    class Properties(Schema):
        """Override in subclass"""

    _autogen_name_id = 1
    _autogen_name_lock = Lock()
    _props_schema = None  # initialized by neo4j_type

    def __init__(self, labels=None, properties=None):
        super(Node, self).__init__(labels, properties=properties)
        self.label = ':'.join(labels) if labels else self.label

    def __repr__(self):
        return str(self)

    def __str__(self):
        label_str = ':' + self.label if self.label else ''
        props_str = ' ' + build_props_str(self.props) if self.props else ''
        return '({label}{props})'.format(
                label=label_str,
                props=props_str,
                )

    @classmethod
    def load(cls, props):
        loaded_props = cls._props_schema.load(props).data
        return cls(properties=loaded_props)

    def dump(self):
        return self._props_schema.dump(self.properties).data

    @classmethod
    def symbol(cls, name='', label='', props=None):
        label = cls.label if label == '' else label
        name = cls._autogen_symbol_name() if name == '' else name
        return Symbol(cls, name=name, label=label, props=props)

    @classmethod
    def _autogen_symbol_name(cls):
        with cls._autogen_name_lock:
            n = cls._autogen_name_id
            cls._autogen_name_id += 1
            return '{}_{}'.format(
                    RE_NONWORD_CHARS.sub('', cls.label.lower()),
                    base36.dumps(n))
