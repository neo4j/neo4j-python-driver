import re

# Constants for parsing and generating Cypher statements
# ---------------------------------------------------------------------------

OP_EQ = '='
OP_NE = '<>'
OP_GT = '>'
OP_LT = '<'
OP_GE = '>='
OP_LE = '<='
OP_IN = 'IN'
OP_RE = '=~'

KW_MATCH = 'MATCH'
KW_CREATE = 'CREATE'
KW_WHERE = 'WHERE'
KW_RETURN = 'RETURN'
KW_DELETE = 'DELETE'
KW_MERGE = 'MERGE'
KW_WITH = 'WITH'
KW_SKIP = 'SKIP'
KW_LIMIT = 'LIMIT'

DIR_OUT = '->'
DIR_IN = '<-'
DIR_NONE = '--'
