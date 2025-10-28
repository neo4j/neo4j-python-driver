FROM ghcr.io/astral-sh/uv:alpine AS build

WORKDIR /home/root/
RUN pwd && ls -Flah
COPY . neo4j-python-driver
# RUN uv pip install --system ./neo4j-python-driver/benchmarks
WORKDIR /home/root/neo4j-python-driver/benchmarks
RUN uv sync --frozen --no-editable
RUN uv python find 3.13

FROM ghcr.io/astral-sh/uv:alpine

WORKDIR /home/root/benchmarks
COPY --from=build /home/root/neo4j-python-driver/benchmarks .
COPY --from=build /root/.local/share/uv/python /root/.local/share/uv/python

ENTRYPOINT ["uv", "run", "--no-sync", "python", "-m", "bench"]
