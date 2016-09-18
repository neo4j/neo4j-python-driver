import json

from .constants import (
        OP_EQ, OP_NE, OP_GT, OP_LT,
        OP_GE, OP_LE, OP_IN, OP_RE,
        )


class Predicate(object):
    def __init__(self, operator, operands, prop_name):
        self._operator = operator
        self._operands = operands
        self._prop_name = prop_name

    def __repr__(self):
        return 'Predicate({})'.format(self)

    def __str__(self):
        return self.emit()

    def emit(self):
        operand_strs = []
        for op in self._operands:
            if isinstance(op, PredicateProperty):
                operand_str = '{}.{}'.format(op.symbol.var_name, op.name)
            else:
                operand_str = json.dumps(op)   # we do this mainly for encoding strings
            operand_strs.append(operand_str)
        return '{lhs} {op} {rhs}'.format(
            lhs=operand_strs[0], op=self._operator, rhs=operand_strs[1])


class PredicateProperty(object):
    def __init__(self, symbol, name):
        self.symbol = symbol
        self.name = name

    def __str__(self):
        if self.symbol.node_class is not None:
            symbol_str = ''
            if self.symbol.var_name:
                symbol_str += self.symbol.var_name
            if self.symbol.label:
                symbol_str += ':' + self.symbol.label
            symbol_str += '.'
        else:
            symbol_str = ''
        return 'PredicateProperty({}{})'.format(symbol_str, self.name)

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        return self._build_predicate(OP_EQ, other)

    def __ne__(self, other):
        return self._build_predicate(OP_NE, other)

    def __gt__(self, other):
        return self._build_predicate(OP_GT, other)

    def __ge__(self, other):
        return self._build_predicate(OP_GE, other)

    def __lt__(self, other):
        return self._build_predicate(OP_LT, other)

    def __le__(self, other):
        return self._build_predicate(OP_LE, other)

    def is_in(self, iterable):
        if not iterable:
            return None
        return self._build_predicate(OP_IN, iterable)

    def starts_with(self, string):
        return self._build_predicate('STARTS WITH', string)

    def ends_with(self, string):
        return self._build_predicate('ENDS WITH', string)

    def contains(self, string):
        return self._build_predicate('CONTAINS', string)

    def regex(self, pattern):
        return self._build_predicate(OP_RE, pattern)

    def op(self, op_str):
        return self._build_predicate(op_str, other)

    def _build_predicate(self, operator, rhs):
        operands = (self, rhs)
        return Predicate(operator, operands, self.name)
