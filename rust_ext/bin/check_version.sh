#!/usr/bin/env bash
set -e

version="$1"; shift

if ! grep -q --perl-regexp "(?m)(?<!.)^\s*version\s*=\s*\"$version\"\s*\$(?!.)" pyproject.toml
then
    echo "Version mismatch in pyproject.toml"
    echo "Trying to release version $version"
    foundVersion=$(sed -nr 's/ *version *= *\"(.*)\"/\1/p' pyproject.toml)
    if [ -z "$foundVersion" ]
    then
        echo "No version found in pyproject.toml"
    else
        echo "Found version $foundVersion in pyproject.toml"
    fi
    exit 1
fi
