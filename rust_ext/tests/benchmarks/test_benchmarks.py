import neo4j


URL = "neo4j://localhost:7687"
AUTH = ("neo4j", "pass")


def test_little_data(benchmark):
    def work():
        driver.execute_query("RETURN 1 AS n")

    with neo4j.GraphDatabase.driver(URL, auth=AUTH) as driver:
        driver.verify_connectivity()
        benchmark.pedantic(work, rounds=5000)


def test_import(benchmark):
    def work():
        driver.execute_query("RETURN 1 AS n", param=data)

    data = [
        *range(1000),
        *(
            {
                "name": f"Person {i}",
                "age": i,
            }
            for i in range(1000)
        ),
        f"L{'o' * 10000}ng string",
    ]

    with neo4j.GraphDatabase.driver(URL, auth=AUTH) as driver:
        driver.verify_connectivity()
        benchmark.pedantic(work, rounds=1000)


def test_export_single_record(benchmark):
    def work():
        driver.execute_query("RETURN [x IN range(0, 100000)] AS x")

    with neo4j.GraphDatabase.driver(URL, auth=AUTH) as driver:
        driver.verify_connectivity()
        benchmark.pedantic(work, rounds=300)


def test_export_many_records(benchmark):
    def work():
        driver.execute_query("UNWIND range(0, 1000) AS x RETURN x")

    with neo4j.GraphDatabase.driver(URL, auth=AUTH) as driver:
        driver.verify_connectivity()
        benchmark.pedantic(work, rounds=150)
