from types import GeneratorType
from collections import defaultdict

from .symbol import Symbol
from .predicate import Predicate
from .constants import (
        KW_MATCH, KW_CREATE, KW_WHERE,
        KW_RETURN, KW_DELETE, KW_MERGE,
        KW_SKIP, KW_LIMIT, KW_WITH,
        )


class Cypher(object):

    # Each clause of the Cypher statement is emited
    # in the order listed here.
    TARGET_KEYWORD_SEQUENCE = (
            KW_MATCH,
            KW_CREATE,
            KW_MERGE,
            KW_DELETE,
            KW_WHERE,
            KW_WITH,
            KW_RETURN,
            KW_SKIP,
            KW_LIMIT,
            )

    def __init__(self, parent=None):
        self.v1_type_map = {}
        self._parent = parent
        self._targets = defaultdict(list)
        self._node_classes = set()
        self._emit_methods = {
            k: getattr(self, '_emit_{}'.format(k))
            for k in self.TARGET_KEYWORD_SEQUENCE
            }

        if parent is not None:
            assert parent._targets[KW_RETURN]
            parent._targets[KW_WITH] = parent._targets.pop(KW_RETURN)

    def __str__(self):
        return self.emit()

    def run(self, session):
        return session.run(self.emit(), cypher_obj=self)

    def new_node(self, labels, properties):
        label = ':'.join(labels)
        node_class = self.v1_type_map.get(label)
        if node_class is not None:
            return node_class(labels=labels, properties=properties)
        node = Node(labels=labels, properties=properties)
        return node

    def create(self, *targets):
        targets = self._normalize_targets(targets)
        self._targets[KW_CREATE].extend(targets)
        return self

    def merge(self, *targets):
        targets = self._normalize_targets(targets)
        self._targets[KW_MERGE].extend(targets)
        return self

    def match(self, *targets):
        targets = self._normalize_targets(targets)
        self._targets[KW_MATCH].extend(targets)
        return self

    def delete(self, *targets):
        targets = self._normalize_targets(targets)
        self._targets[KW_DELETE].extend(targets)
        return self

    def where(self, *predicates):
        self._targets[KW_WHERE].extend(predicates)
        return self

    def skip(self, value):
        self._targets[KW_SKIP] = [str(value)]
        return self

    def limit(self, value):
        self._targets[KW_LIMIT] = [str(value)]
        return self

    def ret(self, *targets):
        targets = self._normalize_targets(targets)
        self._targets[KW_RETURN].extend(targets)
        return self

    def _normalize_targets(self, sequence):
        flattened = []
        for x in sequence:
            if not x:
                continue
            elif isinstance(x, (list, tuple, set, GeneratorType)):
                flattened.extend(self._normalize_targets(x))
            else:
                if isinstance(x, Symbol):
                    self._register_types(x)
                flattened.append(x)
        return flattened

    def _register_types(self, symbol):
        while True:
            if symbol.node_class not in self._node_classes:
                self._node_classes.add(symbol.node_class)
                for label in symbol.node_class.labels:
                    if label not in self.v1_type_map:
                        self.v1_type_map[label] = symbol.node_class
            if not symbol.next:
                break
            symbol = symbol.next[1]

    def emit(self):
        lines = []
        parent_stmt = ''
        if self._parent is not None:
            parent_stmt = self._parent.emit() + '\n'
        for keyword in self.TARGET_KEYWORD_SEQUENCE:
            targets = self._targets.get(keyword)
            if targets:
                lines.extend(self._emit_methods[keyword](targets))
        return parent_stmt + '\n'.join(lines)

    def _emit_CREATE(self, targets):
        return ['{} {}'.format(KW_CREATE, ', '.join(str (t) for t in targets))]

    def _emit_MERGE(self, targets):
        return ['{} {}'.format(KW_MERGE, str(t)) for t in targets]

    def _emit_MATCH(self, targets):
        return ['{} {}'.format(KW_MATCH, ', '.join(str(t) for t in targets))]

    def _emit_DELETE(self, targets):
        names = self._extract_target_names(targets)
        return ['{} {}'.format(KW_DELETE, ', '.join(name for name in names))]

    def _emit_WITH(self, targets):
        names = self._extract_target_names(targets)
        return ['{} {}'.format(KW_WITH, ', '.join(name for name in names))]

    def _emit_RETURN(self, targets):
        names = self._extract_target_names(targets)
        return ['{} {}'.format(KW_RETURN, ', '.join(name for name in names))]

    def _emit_WHERE(self, predicates):
        predicates_str = ' AND '.join(str(p) for p in predicates)
        return ['{} {}'.format(KW_WHERE, predicates_str)]

    def _emit_SKIP(self, targets):
        return ['{} {}'.format(KW_SKIP, targets[0])]

    def _emit_LIMIT(self, targets):
        return ['{} {}'.format(KW_LIMIT, targets[0])]

    def _extract_target_names(self, targets):
        names = []
        for target in targets:
            if isinstance(target, Symbol):
                if target.path_name:
                    names.append(target.path_name)
                elif target.var_name:
                    names.append(target.var_name)
            elif isinstance(target, Predicate):
                names.append(target.emit())
            elif isinstance(target, str):
                names.append(target)
        return names


if __name__ == '__main__':
    from .symbol import Symbol
    from .node import Node
    from .schema import Schema, Str, Int
    from .relationship import Relationship

    from neo4j.v1 import GraphDatabase, basic_auth

    url = "bolt://localhost:7687"
    driver = GraphDatabase.driver(url, auth=basic_auth("neo4j", "tempus"))
    session = driver.session()

    class User(Node):
        class Properties(Schema):
            name = Str()
            age = Int()

        in_company = Relationship('IN_COMPANY', '->')

    class Company(Node):
        class Properties(Schema):
            name = Str()


    u = User.symbol(props={'name': 'Daniel', 'age': 31})
    c = Company.symbol(props={'name': 'Axial'})


    cypher = Cypher()\
        .create(u.in_company(c, name='in_company', path='p'))\
        .ret(c, u, 'in_company', 'p')

    print(cypher)

    results = cypher.run(session)
    for rec in results:
        print(rec)
