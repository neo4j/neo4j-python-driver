#!/usr/bin/env bash

ROOT=$(dirname "$0")

rm -rf ${ROOT}/build ${ROOT}/dist ${ROOT}/*.egg-info ${ROOT}/.coverage ${ROOT}/.tox
find -name *.so -delete
