#!/usr/bin/env bash

CONTAINER_ID=$(docker run -d  -e NEO4J_dbms_memory_heap_max__size=2000m --publish=7474:7474 --publish=7687:7687 --volume=$(pwd)/data/neo4j:/data  neo4j:3.3.1)
sleep 10
docker run -it --net host neo4j:3.3.1 bin/cypher-shell -u neo4j -p neo4j "CALL dbms.changePassword('local neo hates security!')"

echo "Neo4j running locally. To stop it: docker kill ${CONTAINER_ID}"