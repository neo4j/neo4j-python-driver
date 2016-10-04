#!/usr/bin/env bash

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

python -c "from tck.configure_feature_files import *; set_up()"
neotest 3.1.0-M09 $(dirname "$0")/tck/run/ behave --format=progress --tags=-db --tags=-tls --tags=-fixed_session_pool tck
python -c "from tck.configure_feature_files import *; clean_up()"
