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

import os
import tarfile


def clean_up():
    dir_path = (os.path.dirname(os.path.realpath(__file__)))
    files = os.listdir(dir_path)
    for f in files:
        if not os.path.isdir(f) and ".feature" in f:
            os.remove(os.path.join(dir_path, f))


def set_up():
    dir_path = (os.path.dirname(os.path.realpath(__file__)))
    url = "https://s3-eu-west-1.amazonaws.com/remoting.neotechnology.com/driver-compliance/tck.tar.gz"
    file_name = url.split('/')[-1]
    _download_tar(url,file_name)

    tar = tarfile.open(file_name)
    tar.extractall(dir_path)
    tar.close()
    os.remove(file_name)


def _download_tar(url, file_name):
    try:
        import urllib2
        tar = open(file_name, 'w')
        response = urllib2.urlopen(url)
        block_sz = 1024
        while True:
            buffer = response.read(block_sz)
            if not buffer:
                break
            tar.write(buffer)
        tar.close()
    except ImportError:
        from urllib import request
        request.urlretrieve(url, file_name)
