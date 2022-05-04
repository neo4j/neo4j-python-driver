# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


class Request(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._seen_keys = set()

    def __getitem__(self, item):
        self._seen_keys.add(item)
        return super().__getitem__(item)

    def get(self, item, default=None):
        self._seen_keys.add(item)
        return super(Request, self).get(item, default)

    def mark_all_as_read(self, recursive=False):
        self._seen_keys = set(self.keys())
        if recursive:
            for val in self.values():
                if isinstance(val, Request):
                    val.mark_all_as_read(recursive=True)

    def mark_item_as_read(self, item, recursive=False):
        self._seen_keys.add(item)
        if recursive:
            value = super().__getitem__(item)
            if isinstance(value, Request):
                value.mark_all_as_read(recursive=True)

    def mark_item_as_read_if_equals(self, item, value, recursive=False):
        if super().__getitem__(item) == value:
            self.mark_item_as_read(item, recursive=recursive)

    @property
    def unseen_keys(self):
        assert not any(isinstance(v, dict) and not isinstance(v, Request)
                       for v in self.values())
        unseen = set(self.keys()) - self._seen_keys
        for k, v in self.items():
            if isinstance(v, Request):
                unseen.update(k + "." + u for u in v.unseen_keys)
        return unseen

    @property
    def seen_all_keys(self):
        return not self.unseen_keys
