FROM ghcr.io/astral-sh/uv:alpine AS build

WORKDIR /home/root/
COPY . neo4j-python-driver
WORKDIR /home/root/neo4j-python-driver/benchmarks
RUN uv sync --no-dev --no-cache --compile-bytecode --frozen --no-editable --no-install-project

FROM ghcr.io/astral-sh/uv:alpine

WORKDIR /home/root/benchmarks
COPY --from=build /home/root/neo4j-python-driver/benchmarks .
COPY --from=build /root/.local/share/uv/python /root/.local/share/uv/python

ENTRYPOINT ["uv", "run", "--no-sync", "python", "-m", "bench"]
