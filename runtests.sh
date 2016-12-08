#!/usr/bin/env bash

RUN=$(dirname "$0")/test/run/
VERSIONS="3.0.7:3.1.0-M13-beta3"

# Export DIST_HOST=localhost if local web server hosts server packages
if [ -z $1 ]
then
    # Full test (with coverage)
    neotest -e ${VERSIONS} ${RUN} coverage run --source neo4j -m unittest discover -vs test && coverage report --show-missing
else
    # Partial test
    neotest -e ${VERSIONS} ${RUN} coverage run --source neo4j -m unittest -v $1 && coverage report --show-missing
fi
