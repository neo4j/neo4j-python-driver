import json
import re


RE_PROP_NAME = re.compile(r'"(\w+?)":')


def build_props_str(dct):
    # TODO: Allow custom JSON Encoder to be passed in
    return RE_PROP_NAME.sub(r'\1:', json.dumps(dct))
