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

from tck import tck_util

failing_features = {}


def before_feature(context, feature):
    # Workaround. Behave has a different way of tagging than cucumber
    for scenario in feature.scenarios:
        scenario.tags += feature.tags


def before_scenario(context, scenario):
    context.runners = []
    if "reset_database" in scenario.tags:
        session = tck_util.driver.session()
        session.run("MATCH (n) DETACH DELETE n")
        session.close()
    if "equality" in scenario.tags:
        context.values = {}


def after_feature(context, feature):
    failed_scenarios = []
    for scenario in feature.scenarios:
        if scenario.status == "untested" or scenario.status == "failed" :
            failed_scenarios.append(scenario.name)
    if len(failed_scenarios) > 0:
        failing_features[feature.name] = failed_scenarios


def after_all(context):
    if len(failing_features) != 0:
        print("Following Features failed in TCK:")
        for feature, list_of_scenarios in failing_features.items():
            print("Feature: %s" %feature)
            for scenario in list_of_scenarios:
                print("Failing scenario: %s" % scenario)
        raise Exception("\tTCK FAILED!")


def after_scenario(context, scenario):
    for runner in tck_util.runners:
        runner.close()

