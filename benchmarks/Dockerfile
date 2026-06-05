FROM ghcr.io/astral-sh/uv:alpine AS build-venv

WORKDIR /home/root/neo4j-python-driver/benchmarks
COPY ./benchmarks/pyproject.toml .
RUN uv venv .venv
COPY ./benchmarks/uv.lock .
RUN uv sync --no-dev --no-cache --compile-bytecode --frozen --no-install-local

FROM ghcr.io/astral-sh/uv:alpine AS build
WORKDIR /home/root
COPY . neo4j-python-driver/
WORKDIR /home/root/neo4j-python-driver/benchmarks
COPY --from=build-venv /root/.local/share/uv/python /root/.local/share/uv/python
COPY --from=build-venv /home/root/neo4j-python-driver/benchmarks/.venv .venv
COPY --from=build-venv /home/root/neo4j-python-driver/benchmarks/pyproject.toml .
COPY --from=build-venv /home/root/neo4j-python-driver/benchmarks/uv.lock .
RUN uv sync --no-dev --no-cache --compile-bytecode --frozen --no-editable

FROM ghcr.io/astral-sh/uv:alpine

WORKDIR /home/root/benchmarks
COPY --from=build /root/.local/share/uv/python /root/.local/share/uv/python
COPY --from=build /home/root/neo4j-python-driver/benchmarks/.venv .venv

ENTRYPOINT ["uv", "run", "python", "-m", "bench"]
