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

import argparse
import sys
from pathlib import Path

from .workloads.util import (
    Bencher,
    BenchmarkContext,
    get_benchmark,
)


def main() -> None:
    if sys.getrecursionlimit() < 2000:
        sys.setrecursionlimit(2000)

    args = _parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    benchmark = get_benchmark(args.workload)
    context = BenchmarkContext(
        uri=args.neo4j_uri,
        db_name=args.neo4j_db_name,
        user=args.neo4j_db_user,
        password=args.neo4j_db_password,
    )
    bencher = Bencher(args.iterations, args.warmup, context)
    benchmark(bencher)
    bencher.timer.flush_csv(args.output)


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Benchmarking Neo4j Python Driver",
        prog="bench",
    )
    parser.add_argument(
        "--neo4j-uri",
        required=True,
        help="The Neo4j URI to connect to",
    )
    parser.add_argument(
        "--neo4j-db-name",
        required=True,
        help="The Neo4j database name",
    )
    parser.add_argument(
        "--neo4j-db-user",
        required=True,
        help="The Neo4j database user",
    )
    parser.add_argument(
        "--neo4j-db-password",
        required=True,
        help="The Neo4j database password",
    )
    parser.add_argument(
        "--workload",
        required=True,
        help="The workload id to execute",
    )
    parser.add_argument(
        "--iterations",
        required=True,
        type=int,
        help="Number of iterations to run and time the workload",
    )
    parser.add_argument(
        "--warmup",
        required=True,
        type=int,
        help=(
            "Number of warmup iterations before recording results "
            "(in addition to --iterations)"
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("/results/measurement.csv"),
        help="Path to output the results CSV file to",
    )

    return parser.parse_args()


if __name__ == "__main__":
    main()
