#!/usr/bin/env bash

# Copyright (c) 2002-2016 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

DRIVER_HOME=$(dirname $0)

NEORUN_OPTIONS=""
RUNNING=0
KNOWN_HOSTS="${HOME}/.neo4j/known_hosts"
KNOWN_HOSTS_BACKUP="${KNOWN_HOSTS}.backup"

FG_BRIGHT_RED='\033[1;31m'
FG_DEFAULT='\033[0m'

# Parse options
while getopts ":dr" OPTION
do
  case ${OPTION} in
    d)
      NEORUN_OPTIONS="-f"
      ;;
    r)
      RUNNING=1
      ;;
    \?)
      echo "Invalid option: -${OPTARG}" >&2
      ;;
  esac
done
shift $(($OPTIND - 1))

if [ -z "${TEAMCITY_VERSION}" ]
then
    UNITTEST="unittest"
else
    UNITTEST="teamcity.unittestpy"
fi

if [ -z "${TEST}" ]
then
    TEST="test"
fi

VERSIONS=$*
if [ "${VERSIONS}" == "" ]
then
    VERSIONS="nightly"
fi

function check_exit_status {
    EXIT_STATUS=$1
    if [ ${EXIT_STATUS} -ne 0 ]
    then
        echo ""
        echo -e "${FG_BRIGHT_RED}Tests failed with status ${EXIT_STATUS}${FG_DEFAULT}"
        exit ${EXIT_STATUS}
    fi
}

# Run tests
echo "Using $(python --version)"
pip install --upgrade -r ${DRIVER_HOME}/test_requirements.txt
echo ""

TEST_RUNNER="coverage run -m ${UNITTEST} discover -vfs ${TEST}"
BEHAVE_RUNNER="behave --tags=-db --tags=-tls test/tck"
EXAMPLES_RUNNER="coverage run -m ${UNITTEST} discover -vfs examples"
BEHAVE_RUNNER="behave --tags=-db --tags=-in_dev test/tck"

if [ ${RUNNING} -eq 1 ]
then
    ${TEST_RUNNER}
    check_exit_status $?
else
    #echo "Updating password"
    #mv ${KNOWN_HOSTS} ${KNOWN_HOSTS_BACKUP}
    #neokit/neorun ${NEORUN_OPTIONS} "python -m test.auth password" ${VERSIONS}
    #EXIT_STATUS=$?
    #mv ${KNOWN_HOSTS_BACKUP} ${KNOWN_HOSTS}
    #check_exit_status ${EXIT_STATUS}
    export NEO4J_PASSWORD="password"

    echo "Running unit tests"
    neokit/neorun ${NEORUN_OPTIONS} "${TEST_RUNNER}" ${VERSIONS}
    check_exit_status $?

    echo "Testing example code"
    neokit/neorun ${NEORUN_OPTIONS} "${EXAMPLES_RUNNER}" ${VERSIONS}
    check_exit_status $?

    coverage report --show-missing
    python -c 'from test.tck.configure_feature_files import *; set_up()'
    echo "Feature files downloaded"
    neokit/neorun ${NEORUN_OPTIONS} "${BEHAVE_RUNNER}" ${VERSIONS}
    python -c 'from test.tck.configure_feature_files import *; clean_up()'
    echo "Feature files removed"

fi
