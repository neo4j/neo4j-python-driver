from __future__ import annotations

import os
import typing as t


__all__ = [
    "Env",
    "env",
]


class Env(t.NamedTuple):
    backend_port: int
    neo4j_host: str
    neo4j_port: int
    neo4j_scheme: str
    neo4j_user: str
    neo4j_pass: str
    driver_debug: bool


env = Env(
    backend_port=int(os.environ.get("TEST_BACKEND_PORT", "9000")),
    neo4j_host=os.environ.get("TEST_NEO4J_HOST", "localhost"),
    neo4j_port=int(os.environ.get("TEST_NEO4J_PORT", "7687")),
    neo4j_scheme=os.environ.get("TEST_NEO4J_SCHEME", "neo4j"),
    neo4j_user=os.environ.get("TEST_NEO4J_USER", "neo4j"),
    neo4j_pass=os.environ.get("TEST_NEO4J_PASS", "password"),
    driver_debug=os.environ.get("TEST_DRIVER_DEBUG", "").lower() in (
        "y", "yes", "true", "1", "on"
    )
)
