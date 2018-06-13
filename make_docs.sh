#!/usr/bin/env bash

HOME=$(dirname $0)

pip install --upgrade sphinx
make -C ${HOME}/docs html

echo ""
INDEX_FILE="${HOME}/docs/build/html/index.html"
echo "Documentation index file can be found at file://$(cd "$(dirname "${INDEX_FILE}")"; pwd)/$(basename "${INDEX_FILE}")"
