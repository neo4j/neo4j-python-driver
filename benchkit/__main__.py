from __future__ import annotations

from sanic import Sanic
from sanic.worker.loader import AppLoader

from .app import create_app
from .env import env


if __name__ == '__main__':
    loader = AppLoader(factory=create_app)
    app = loader.load()
    # app.prepare(port=env.backend_port, debug=True, workers=1, dev=True)
    app.prepare(host="0.0.0.0", port=env.backend_port, workers=1)
    Sanic.serve(primary=app, app_loader=loader)
