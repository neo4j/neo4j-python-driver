import re
import json

from .constants import DIR_IN, DIR_OUT, DIR_NONE
from .util import build_props_str


class Relationship(object):
    RE_PROP_NAME = re.compile(r'"(\w+?)":')

    def __init__(self, label=None, direction=None):
        self.label = label
        self.direction = direction or DIR_NONE

    def emit(self, name='', label='', props=None):
        # TODO: Refactor Relationship.emit method
        label = self.label if label == '' else label
        if label or name or props:
            if props:
                props_str = build_props_str(props)
            else:
                props_str = ''
            name_str = name or ''
            edge_label = '[{}{}{}]'.format(
                    name_str,
                    ':' + label if label else '',
                    ' ' + props_str if props else '')
        else:
            edge_label = ''

        if self.direction == DIR_OUT:
            return '-{}->'.format(edge_label)
        elif self.direction == DIR_IN:
            return '<-{}-'.format(edge_label)
        elif self.direction == DIR_NONE:
            return '-{}-'.format(edge_label)

        raise ValueError('invalid relationship direction')
