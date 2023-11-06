#!/usr/bin/env bash
set -e

version="$1"; shift

sed -i "s/\"\(neo4j *== *\).*\"/\"\1$version\"/" pyproject.toml
sed -i "s/\(version *= *\)\"[0-9]\+\.[0-9]\+\.[0-9]\+\(.*\)\"/\1\"$version.0a1\"/" pyproject.toml

cd driver
git fetch origin
git checkout "$version"
git pull origin "$version"
cd ..
cp driver/tests/unit/common/codec/packstream/v1/test_packstream.py tests/v1/test_packstream.py
