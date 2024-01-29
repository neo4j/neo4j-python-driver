from __future__ import annotations

from contextlib import contextmanager
from multiprocessing import Semaphore

import typing_extensions as te
from sanic import Sanic
from sanic.config import Config
from sanic.exceptions import (
    BadRequest,
    NotFound,
)
from sanic.request import Request
from sanic.response import (
    empty,
    HTTPResponse,
    text,
)

from .context import BenchKitContext
from .env import env
from .workloads import Workload


T_App: te.TypeAlias = "Sanic[Config, BenchKitContext]"


def create_app() -> T_App:
    app: T_App = Sanic("Python_BenchKit", ctx=BenchKitContext())

    @app.main_process_start
    async def main_process_start(app: T_App) -> None:
        app.shared_ctx.running = Semaphore(1)

    @app.before_server_start
    async def before_server_start(app: T_App) -> None:
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
            raise BadRequest(str(e))

    def _get_workload(app: T_App, name: str) -> Workload:
        try:
            workload = app.ctx.workloads[name]
        except KeyError:
            raise NotFound(f"Workload {name} not found")
        return workload

    @app.get("/ready")
    async def ready(_: Request) -> HTTPResponse:
        await app.ctx.get_db()  # check that the database is available
        return empty()

    @app.post("/workload")
    async def post_workload(request: Request) -> HTTPResponse:
        data = request.json
        with _loading_workload():
            name = app.ctx.workloads.store_workload(data)
        location = f"/workload/{name}"
        return text(f"created at {location}",
                    status=204,
                    headers={"location": location})

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
    async def patch_workload(request: Request, name: str) -> HTTPResponse:
        data = request.json
        workload = _get_workload(app, name)
        with _loading_workload():
            workload.patch(data)
        return empty()

    @app.delete("/workload/<name>")
    async def delete_workload(_: Request, name: str) -> HTTPResponse:
        _get_workload(app, name)
        del app.ctx.workloads[name]
        return empty()

    return app
