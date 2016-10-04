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

"""
Usage:   runtests.py
         -h          : show this help message
         --test=name : run this specific test
         --tests     : run all unit tests
         --examples  : run all example tests
         --tck       : run tck tests
         --neorun.start.args : args to neorun script
example:
         python ./runtests.py --tests --examples --tck
         python ./runtests.py --tests --examples --tck --neorun.start.args="-n 3.1 -p neo4j"
"""
from sys import argv, stdout, exit, version_info
from os import name, path
from atexit import register
import subprocess
import getopt

UNITTEST_RUNNER = "coverage run -m unittest discover -vfs "
BEHAVE_RUNNER="behave --format=progress --tags=-db --tags=-tls --tags=-fixed_session_pool test/tck"

NEORUN_PATH = path.abspath('./neokit/neorun.py')
NEO4J_HOME = path.abspath('./build/neo4jhome')

is_windows = (name == 'nt')


def runpymodule(command):
    commands = command.split()
    if is_windows:
        commands = ['powershell.exe', 'python', '-m'] + commands
    return run0(commands)


def runcommand(command):
    commands = command.split()
    return runcommands(commands)


def runcommands(commands):
    if is_windows:
        commands = ["\"" + comm + "\"" if " " in comm else comm for comm in commands]
        commands = ['powershell.exe'] + commands
    return run0(commands)


def run0(commands):
    stdout.write("Running commands: %s\n" % commands)
    p = subprocess.Popen(commands, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    retcode = p.wait()
    if version_info < (3, 0):
        stdout.write(out)
        stdout.write(err)
    else:
        stdout.write(out.decode(stdout.encoding))
        stdout.write(err.decode(stdout.encoding))
    return retcode


def neorun(command):
    runcommand('python ' + NEORUN_PATH + ' ' + command)


def main():
    if len(argv) <= 1:
        print_help()
        exit(2)
    try:
        opts, args = getopt.getopt(argv[1:], "h", ["test=", "tests", "examples", "tck", "neorun.start.args="])
    except getopt.GetoptError as err:
        print(str(err))
        print_help()
        exit(2)
    else:

        stdout.write("Using python version:\n")
        runcommand('python --version')
        runpymodule('pip install --upgrade -r ./test/requirements.txt')
        retcode = 0

        register(neorun, '--stop=' + NEO4J_HOME)

        neorun_args = '-p neo4j'
        for opt, arg in opts:
            if opt == '--neorun.start.args':
                neorun_args = arg
                break
        neorun('--start=' + NEO4J_HOME + ' ' + neorun_args)

        for opt, arg in opts:
            if opt == '-h':
                print_help()
                retcode = 2

            elif opt == "--tests":
                retcode = retcode or runpymodule(UNITTEST_RUNNER + "test")
            elif opt == "--test=":
                retcode = retcode or runpymodule(UNITTEST_RUNNER + arg)
            elif opt == "--example":
                retcode = retcode or runpymodule(UNITTEST_RUNNER + "examples")
            elif opt == "--tck":
                retcode = runpymodule('coverage report --show-missing') or \
                          runcommands(["python", "-c", "from test.tck.configure_feature_files import *; set_up()"]) or \
                          runpymodule(BEHAVE_RUNNER) or \
                          runcommands(["python", "-c", "from test.tck.configure_feature_files import *; clean_up()"])

            if retcode != 0:
                break

    return retcode


def print_help():
    print(__doc__)


if __name__ == "__main__":
    main()