#!/usr/bin/env bash

# Copyright 2014-2015, Nigel Small
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

ACTION="download"
DIST="http://dist.neo4j.org"
ALPHA="http://alpha.neotechnology.com.s3-website-eu-west-1.amazonaws.com"
EDITION="community"
MODE=""
CHECK_EXISTS=0
ALL_VERSIONS="2.3.0-M02 2.2.2 2.1.8 2.0.4"

function usage {
    SCRIPT=$(basename $0)
    echo "Usage: ${SCRIPT} [<options>] [<version> [<version> ...]]"
    echo ""
    echo "Download tool for Neo4j server packages."
    echo ""
    echo "Action Options:"
    echo "  -d  download the latest or specific Neo4j packages (default)"
    echo "  -l  show a list of available Neo4j versions"
    echo "  -h  display this help text"
    echo ""
    echo "Other Options:"
    echo "  -a  select the latest alpha release for download"
    echo "  -x  only download if the file does not already exist"
    echo ""
    echo "Edition Options:"
    echo "  -c  use Neo4j Community edition (default)"
    echo "  -e  use Neo4j Enterprise edition"
    echo ""
    echo "List Options:"
    echo "  -f list only archive file names"
    echo "  -u list full download URLs (default)"
    echo "  -v list only versions"
    echo ""
    echo "Environment:"
    echo "  NEO4J_DIST - base URL for downloads (default: http://dist.neo4j.org)"
    echo ""
    echo "Report bugs to nigel@neotechnology.com"
}

function list {
    for VERSION in ${ALL_VERSIONS}
    do
        ARCHIVE="neo4j-${EDITION}-${VERSION}-unix.tar.gz"
        if [ "${MODE}" == "file" ]
        then
            echo "${ARCHIVE}"
        elif [ "${MODE}" == "version" ]
        then
            echo "${VERSION}"
        else
            echo "${DIST}/${ARCHIVE}"
        fi
    done
}

function download {
    for VERSION in ${VERSIONS}
    do
        ARCHIVE="neo4j-${EDITION}-${VERSION}-unix.tar.gz"
        DOWNLOAD=1
        if [ $CHECK_EXISTS -eq 1 ]
        then
            if [ -f $ARCHIVE ]
            then
                DOWNLOAD=0
            fi
        fi
        if [ $DOWNLOAD -eq 1 ]
        then
            if [[ "${VERSION}" == *"alpha"* ]]
            then
                URL="${ALPHA}/${ARCHIVE}"
            else
                URL="${DIST}/${ARCHIVE}"
            fi
            curl --silent --fail "${URL}" -o "${ARCHIVE}"
            RESULT=$?
            if [ $RESULT -eq 0 ]
            then
                echo "${ARCHIVE}"
            else
                echo 1>&2 "Cannot download archive ${URL}"
                exit $RESULT
            fi
        else
            echo "${ARCHIVE}"
        fi
    done
}

while getopts ":acdefhluvx" OPTION
do
  case ${OPTION} in
    a)
      ALL_VERSIONS="3.0.0-alpha.LATEST"
      ;;
    c)
      EDITION="community"
      ;;
    d)
      ACTION="download"
      ;;
    e)
      EDITION="enterprise"
      ;;
    f)
      MODE="file"
      ;;
    h)
      ACTION="help"
      ;;
    l)
      ACTION="list"
      ;;
    u)
      MODE="url"
      ;;
    v)
      MODE="version"
      ;;
    x)
      CHECK_EXISTS=1
      ;;
    \?)
      echo "Invalid option: -${OPTARG}" >&2
      ;;
  esac
done

shift $(($OPTIND - 1))
if [ "$*" != "" ]
then
    VERSIONS="$*"
else
    set -- $ALL_VERSIONS
    VERSIONS=$1
fi

if [ "${ACTION}" == "help" ]
then
    usage
elif [ "${ACTION}" == "list" ]
then
    list
else
    download
fi
