#!/usr/bin/env bash

ACTION="download"
DIST="http://dist.neo4j.org"
EDITION="community"
MODE=""
ONE=""
VERSIONS="2.3.0-M01 2.2.1 2.1.8 2.0.4"

function usage {
    SCRIPT=$(basename $0)
    echo "Usage: ${SCRIPT} [<options>]"
    echo ""
    echo "Download tool for Neo4j server packages."
    echo ""
    echo "Action Options:"
    echo "  -d  download the latest Neo4j version package (default)"
    echo "  -l  show a list of available Neo4j versions"
    echo "  -h  display this help text"
    echo ""
    echo "Edition Options:"
    echo "  -c  use Neo4j Community edition (default)"
    echo "  -e  use Neo4j Enterprise edition"
    echo ""
    echo "List Options:"
    echo "  -a list only archive file names"
    echo "  -u list full download URLs (default)"
    echo "  -v list only versions"
    echo ""
    echo "Environment:"
    echo "  NEO4J_DIST - base URL for downloads (default: http://dist.neo4j.org)"
    echo ""
    echo "Report bugs to nigel@neotechnology.com"
}

function list {
    for VERSION in ${VERSIONS}
    do
        ARCHIVE="neo4j-${EDITION}-${VERSION}-unix.tar.gz"
        if [ "${MODE}" == "archive" ]
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
        curl "${DIST}/${ARCHIVE}" -o "${ARCHIVE}"
        break
    done
}

while getopts ":acdehlv" OPTION
do
  case ${OPTION} in
    a)
      MODE="archive"
      ;;
    e)
      EDITION="community"
      ;;
    d)
      ACTION="download"
      ;;
    e)
      EDITION="enterprise"
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
    \?)
      echo "Invalid option: -${OPTARG}" >&2
      ;;
  esac
done

if [ "${ACTION}" == "help" ]
then
    usage
elif [ "${ACTION}" == "list" ]
then
    list
else
    download
fi
