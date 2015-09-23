#!/usr/bin/env bash

DOCS=$(dirname $0)/docs

aws --region eu-west-1 s3 cp --acl public-read ${DOCS}/build/html/ s3://alpha.neohq.net/docs/python-driver/ --recursive
