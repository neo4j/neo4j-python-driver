#!/usr/bin/env bash

RUN=$(dirname "$0")/test/run/
versions="${VERSIONS:-3.0.8:3.1.0}"

# Export DIST_HOST=localhost if local web server hosts server packages
if [ -z $1 ]
then
    # Full test (with coverage)
    neotest -e ${versions} ${RUN} coverage run --source neo4j -m unittest discover -vs test && coverage report --show-missing
else
    # Partial test
    neotest -e ${versions} ${RUN} coverage run --source neo4j -m unittest -v $1 && coverage report --show-missing
fi
