#!/usr/bin/env bash

HOME=$(pwd)/$(dirname $0)

function runtests {

    PYTHON_VERSION=$(python --version)
    NEO_VERSION=$1

    echo "========================================================================"
    echo "Running with ${PYTHON_VERSION} against Neo4j ${NEO_VERSION}"
    echo "------------------------------------------------------------------------"

    mkdir -p ${HOME}/.test 2> /dev/null

    cd ${HOME}/.test
    tar xf $(${HOME}/neoget.sh -ex ${NEO_VERSION})
    NEO_HOME=$(ls -1Ft | grep "/$" | head -1)      # finds the newest directory
    ${NEO_HOME}/bin/neo4j start

    cd ${HOME}
    coverage run -m unittest test

    cd ${HOME}/.test
    ${NEO_HOME}/bin/neo4j stop
    rm -rf ${NEO_HOME}

    cd ${HOME}
    coverage report --show-missing

    echo "========================================================================"
    echo ""

}

runtests "3.0.0-alpha.LATEST"
