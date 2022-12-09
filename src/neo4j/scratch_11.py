from time import time

import neo4j
from neo4j.debug import watch


# watch("neo4j")


URI = "neo4j://localhost:7687"
USER = "neo4j"
PASSWORD = "pass"



def main():
    start = time()
    with neo4j.GraphDatabase.driver(URI, auth=(USER, PASSWORD)) as driver:
        for _ in range(10000):
            with driver.session() as session:
                value = list(range(100))
                list(session.run("RETURN $value", value=value))
    end = time()
    print(f"Time taken: {end - start}s")


if __name__ == '__main__':
    main()
