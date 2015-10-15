#!/usr/bin/env bash

if [[ $0 == /* ]]
then
    HOME=$(dirname $0)
else
    HOME=$(pwd)/$(dirname $0)
fi
cd ${HOME}

DOT_TEST=${HOME}/.test
NEOGET=${HOME}/neoget.sh

FORCE_DOWNLOAD=0
RUNNING=0

function runserverandtests {

    PYTHON_VERSION=$(python --version)
    NEO_VERSION=$1

    echo "======================================================================"
    echo "Running with ${PYTHON_VERSION} against Neo4j ${NEO_VERSION}"
    echo "----------------------------------------------------------------------"

    cd ${HOME}

    if [ ${FORCE_DOWNLOAD} -ne 0 ]
    then
        rm -rf ${DOT_TEST}
    fi
    mkdir -p ${DOT_TEST} 2> /dev/null

    pushd ${DOT_TEST} > /dev/null
    tar xf $(${NEOGET} ${NEOGET_ARGS})
    NEO_HOME=$(ls -1Ft | grep "/$" | head -1)      # finds the newest directory relative to .test
    echo "xx.bolt.enabled=true" >> ${NEO_HOME}/conf/neo4j-server.properties
    ${NEO_HOME}/bin/neo4j start
    STATUS=$?
    if [ ${STATUS} -ne 0 ]
    then
        exit ${STATUS}
    fi
    popd > /dev/null

    echo -n "Testing"
    coverage run -m unittest test

    pushd ${DOT_TEST} > /dev/null
    ${NEO_HOME}/bin/neo4j stop
    rm -rf ${NEO_HOME}
    popd > /dev/null

    coverage report --show-missing

    echo "======================================================================"
    echo ""

}

function runtests {

    PYTHON_VERSION=$(python --version)

    echo "======================================================================"
    echo "Running with ${PYTHON_VERSION} against running Neo4j instance"
    echo "----------------------------------------------------------------------"

    cd ${HOME}

    echo -n "Testing"
    coverage run -m unittest test

    coverage report --show-missing

    echo "======================================================================"
    echo ""

}

while getopts ":dr" OPTION
do
  case ${OPTION} in
    d)
      FORCE_DOWNLOAD=1
      ;;
    r)
      RUNNING=1
      ;;
    \?)
      echo "Invalid option: -${OPTARG}" >&2
      ;;
  esac
done

if [ ${RUNNING} -eq 1 ]
then
    runtests
else
    NEOGET_ARGS="-eax"
    runserverandtests "3.0.0-M01"
fi
