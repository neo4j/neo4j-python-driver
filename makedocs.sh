#!/usr/bin/env bash

HOME=$(dirname $0)

make -C ${HOME}/docs html
