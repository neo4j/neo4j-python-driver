#!/usr/bin/env bash

HOME=$(dirname $0)


function runtests {
    NEO_VERSION=$1
    PYTHON=$2
    cd ${HOME}
    ${PYTHON} -m unittest test
}

runtests "2.3.0-M01" "python2.7"
runtests "2.3.0-M01" "python3.3"
