#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2015 "Neo Technology,"
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
Usage:   neoget.py <cmd> [arg]
         -v neo4j-version: download this specific neo4j version
         -l download-url : download neo4j provided by this url
         -h              : show this help message

Example: neoget.py -v 2.3.1
         neoget.py -h
"""
from __future__ import print_function
from urllib import urlretrieve
from sys import argv, stdout, exit
from getopt import getopt
from os import path, name
from zipfile import ZipFile
from urlparse import urlparse


DIST = "http://dist.neo4j.org"
DEFAULT_URL = "http://alpha.neohq.net/dist/neo4j-enterprise-3.0.0-M01-NIGHTLY-unix.tar.gz"
WIN_URL = "http://alpha.neohq.net/dist/neo4j-enterprise-3.0.0-M01-NIGHTLY-windows.zip"


def main():
    is_windows = (name == 'nt')
    archive_url = WIN_URL if is_windows else DEFAULT_URL
    archive_name = path.split(urlparse(archive_url).path)[-1]

    try:
        opts, args = getopt(argv[1:], "hv:l:")
    except getopt.GetoptError as err:
        print(str(err))
        print_help()
        exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print_help()
            exit()
        elif opt == '-v':
            if is_windows:
                archive_name = "neo4j-enterprise-%s-windows.zip" % arg
            else:
                archive_name = "neo4j-enterprise-%s-unix.tar.gz" % arg
            archive_url = "%s/%s" % (DIST, archive_name)
        elif opt == '-l':
            archive_url = arg
            archive_name = path.split(urlparse(archive_url).path)[-1]

    stdout.write("Downloading %s...\n" % archive_url)
    urlretrieve(archive_url, archive_name)

    if archive_name.endswith('zip') or archive_name.endswith('gz'):
        stdout.write("Unzipping %s...\n" % archive_name)
        zip_ref = ZipFile(archive_name, 'r')
        zip_ref.extractall(".")
        zip_ref.close()


def print_help():
    print(__doc__)


if __name__ == "__main__":
    main()
