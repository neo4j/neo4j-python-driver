#!/usr/bin/env bash

ROOT=$(dirname "$0")

rm -rf ${ROOT}/build ${ROOT}/dist ${ROOT}/*.egg-info ${ROOT}/.coverage ${ROOT}/.tox ${ROOT}/.cache ${ROOT}/.pytest_cache ${ROOT}/.benchmarks

# These are removed to avoid name collision problems with snapshot server packages
rm -rf ${ROOT}/test/integration/dist ${ROOT}/test/integration/run

find -name *.so -delete
