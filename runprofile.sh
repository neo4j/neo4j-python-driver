#!/usr/bin/env bash

HOME=$(dirname $0)

function runprofile {
    STATEMENT="$1"
    TIMES="$2"
    cd ${HOME}
    PYTHONPATH=. python -m cProfile -s cumulative neo4j/__main__.py -qx "${TIMES}" "${STATEMENT}"
}

if [ $# -lt 1 ]
then
    echo "usage: $0 <statement> [<times>]"
    exit 1
elif [ $# -eq 1 ]
then
    runprofile "$1" 10000
else
    runprofile "$1" "$2"
fi
