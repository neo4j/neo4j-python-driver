from collections import defaultdict
from threading import RLock

from .schema import Schema
from .relationship import Relationship
from .predicate import PredicateProperty
from .util import build_props_str


class Symbol(object):

    _visited_node_classes = set()
    _visited_node_classes_lock = RLock()

    def __init__(self, node_class, name, label, props):
        assert node_class
        assert node_class.label

        self.node_class = node_class
        self.var_name = name
        self.label = label
        self.props = props

        self._next = None
        self._path_name = None
        self._schema = node_class.Properties()

        # init dynamic "relationship" methods
        with self._visited_node_classes_lock:
            if node_class not in self._visited_node_classes:
                self._visited_node_classes.add(node_class)
                for name in self.node_class.relationships:
                    method = self._new_relationship_method(name)
                    setattr(self.__class__, method.__name__, method)

    def _new_relationship_method(self, name):
        _rel_name = name[:]
        def f(self, symbol, name=name[:], label='', props=None, path=None):
            copy = self.copy()
            copy._path_name = path
            rel = self.node_class.relationships[_rel_name]
            # TODO: Make this into a proper relationship class:
            copy._next = (rel, symbol, name, label, props)
            return copy
        f.__name__ = name
        return f

    def __str__(self):
        return self.emit()

    def __repr__(self):
        return self.emit()

    def __getattr__(self, attr):
        if attr in self._schema.fields:
            return PredicateProperty(self, attr)

    def __call__(self, name='', label='', props=''):
        name = self.var_name if name == '' else name
        label = self.label if label == '' else label
        props = self.props if props == '' else props
        return Symbol(self.node_class, name, label, props)

    @property
    def path_name(self):
        return self._path_name

    @property
    def next(self):
        return self._next

    def copy(self):
        return Symbol(self.node_class, self.var_name, self.label, self.props)

    def emit(self):
        name_str = self.var_name or ''
        label_str = ':' + self.label if self.label else ''
        props_str = ' ' + build_props_str(self.props) if self.props else ''

        next_str = ''
        if self._next is not None:
            rel, sym, rel_name, rel_label, rel_props = self._next
            next_str += '{}{}'.format(
                    rel.emit(rel_name, rel_label, rel_props), sym.emit())

        node_str = '({name}{label}{props}){next}'.format(
                name=name_str,
                label=label_str,
                props=props_str,
                next=next_str)

        if self._path_name:
            return '{} = {}'.format(self._path_name, node_str)
        else:
            return node_str
