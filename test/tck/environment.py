#!/usr/bin/env python
# -*- encoding: utf-8 -*-

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

import logging

from behave.log_capture import capture
from test.tck import tck_util

def before_all(context):
    context.config.setup_logging()

def before_scenario(context,scenario):
    #Empty database
    tck_util.send_string("MATCH (n) DETACH DELETE n")


@capture
def after_scenario(context, scenario):
    for step in scenario.steps:
        if step.status == 'failed':
            logging.error("Scenario :'%s' at step: '%s' failed! ", scenario.name, step.name)
        if step.status == 'skipped':
            logging.warn("Scenario :'%s' at step: '%s' was skipped! ", scenario.name, step.name)
        if step.status == 'passed':
            logging.debug("Scenario :'%s' at step: '%s' was passed! ", scenario.name, step.name)
