#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright (c) 2002-2018 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
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


from genericpath import isfile
from base64 import b64encode
from os import makedirs, open as os_open, write as os_write, close as os_close, O_CREAT, O_APPEND, O_WRONLY
from os.path import dirname, join as path_join, expanduser


KNOWN_HOSTS = path_join(expanduser("~"), ".neo4j", "known_hosts")


class CertificateStore(object):

    def match_or_trust(self, host, der_encoded_certificate):
        """ Check whether the supplied certificate matches that stored for the
        specified host. If it does, return ``True``, if it doesn't, return
        ``False``. If no entry for that host is found, add it to the store
        and return ``True``.

        :arg host:
        :arg der_encoded_certificate:
        :return:
        """
        raise NotImplementedError()


class PersonalCertificateStore(CertificateStore):

    def __init__(self, path=None):
        self.path = path or KNOWN_HOSTS

    def match_or_trust(self, host, der_encoded_certificate):
        base64_encoded_certificate = b64encode(der_encoded_certificate)
        if isfile(self.path):
            with open(self.path) as f_in:
                for line in f_in:
                    known_host, _, known_cert = line.strip().partition(":")
                    known_cert = known_cert.encode("utf-8")
                    if host == known_host:
                        return base64_encoded_certificate == known_cert
        # First use (no hosts match)
        try:
            makedirs(dirname(self.path))
        except OSError:
            pass
        f_out = os_open(self.path, O_CREAT | O_APPEND | O_WRONLY, 0o600)  # TODO: Windows
        if isinstance(host, bytes):
            os_write(f_out, host)
        else:
            os_write(f_out, host.encode("utf-8"))
        os_write(f_out, b":")
        os_write(f_out, base64_encoded_certificate)
        os_write(f_out, b"\n")
        os_close(f_out)
        return True
