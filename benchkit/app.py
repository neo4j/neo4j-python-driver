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

import typing as t
from contextlib import contextmanager
from multiprocessing import Semaphore

import typing_extensions as te
from sanic import Sanic
from sanic.exceptions import (
    BadRequest,
    NotFound,
)
from sanic.response import (
    empty,
    HTTPResponse,
    text,
)

from .context import BenchKitContext
from .env import env


if t.TYPE_CHECKING:
    from sanic.config import Config
    from sanic.request import Request

    from .workloads import Workload


T_App: te.TypeAlias = "Sanic[Config, BenchKitContext]"


def create_app() -> T_App:
    app: T_App = Sanic("Python_BenchKit", ctx=BenchKitContext())

    @app.main_process_start
    def main_process_start(app: T_App) -> None:
        app.shared_ctx.running = Semaphore(1)

    @app.before_server_start
    def before_server_start(app: T_App) -> None:
        if env.driver_debug:
            from neo4j.debug import watch

            watch("neo4j")

        running = app.shared_ctx.running
        acquired = running.acquire(block=False)
        if not acquired:
            raise RuntimeError(
                "The server does not support multiple worker processes"
            )

    @app.after_server_stop
    async def after_server_stop(app: T_App) -> None:
        await app.ctx.shutdown()
        running = app.shared_ctx.running
        running.release()

    @contextmanager
    def _loading_workload():
        try:
            yield
        except (ValueError, TypeError) as e:
            print(e)
            raise BadRequest(str(e)) from None

    def _get_workload(app: T_App, name: str) -> Workload:
        try:
            workload = app.ctx.workloads[name]
        except KeyError:
            raise NotFound(f"Workload {name} not found") from None
        return workload

    @app.get("/ready")
    async def ready(_: Request) -> HTTPResponse:
        await app.ctx.get_db()  # check that the database is available
        return empty()

    @app.post("/workload")
    def post_workload(request: Request) -> HTTPResponse:
        data = request.json
        with _loading_workload():
            name = app.ctx.workloads.store_workload(data)
        location = f"/workload/{name}"
        return text(
            f"created at {location}",
            status=204,
            headers={"location": location},
        )

    @app.put("/workload")
    async def put_workload(request: Request) -> HTTPResponse:
        data = request.json
        with _loading_workload():
            workload = app.ctx.workloads.parse_workload(data)
        driver = await app.ctx.get_db()
        await workload(driver)
        return empty()

    @app.get("/workload/<name>")
    async def get_workload(_: Request, name: str) -> HTTPResponse:
        workload = _get_workload(app, name)
        driver = await app.ctx.get_db()
        await workload(driver)
        return empty()

    @app.patch("/workload/<name>")
    def patch_workload(request: Request, name: str) -> HTTPResponse:
        data = request.json
        workload = _get_workload(app, name)
        with _loading_workload():
            workload.patch(data)
        return empty()

    @app.delete("/workload/<name>")
    def delete_workload(_: Request, name: str) -> HTTPResponse:
        _get_workload(app, name)
        del app.ctx.workloads[name]
        return empty()

    return app
