# Benchmarking Workloads for Internal Use

## Run Locally

```bash
# inside this folder
uv run --frozen python -m bench \
    --neo4j-uri neo4j://localhost:7687 \
    --neo4j-db-name neo4j \
    --neo4j-db-user neo4j \
    --neo4j-db-password pass \
    --iterations 25 \
    --warmup 10 \
    --output ./results/results.csv \
    --workload cold-driver
```

## Run Inside Docker

```bash
# inside the repo root
mkdir -p benchmarks/results
docker build --tag foo --progress plain --debug -f benchmarks/Dockerfile .
docker run --network host --rm -it \
    -v $(pwd)/benchmarks/results:/results \
    foo \
    --neo4j-uri neo4j://localhost:7687 \
    --neo4j-db-name neo4j \
    --neo4j-db-user neo4j \
    --neo4j-db-password pass \
    --iterations 25 \
    --warmup 10 \
    --workload cold-driver
```

## Bumping Environment

### Bumping Python Version

  * Change `requires-python` in `pyproject.toml`
  * ```bash
    # inside this folder
    uv lock --upgrade
    ```

### Bumping Lockfile

```bash
# inside this folder
uv lock --update
```
