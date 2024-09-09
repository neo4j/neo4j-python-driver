# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
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


from __future__ import annotations

from sanic import Sanic
from sanic.worker.loader import AppLoader

from .app import create_app
from .env import env


if __name__ == "__main__":
    loader = AppLoader(factory=create_app)
    app = loader.load()

    # For local development:
    # app.prepare(port=env.backend_port, debug=True, workers=1, dev=True)

    # For production:
    app.prepare(host="0.0.0.0", port=env.backend_port, workers=1)

    Sanic.serve(primary=app, app_loader=loader)
