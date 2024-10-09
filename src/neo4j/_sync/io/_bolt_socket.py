# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import asyncio
import logging
import struct
from contextlib import suppress

from ... import addressing
from ..._async_compat.network import (
    BoltSocketBase,
    NetworkUtil,
)
from ..._exceptions import (
    BoltError,
    BoltProtocolError,
)
from ...exceptions import (
    DriverError,
    ServiceUnavailable,
)


log = logging.getLogger("neo4j.io")


class BoltSocket(BoltSocketBase):
    def _handshake(self, resolved_address, deadline):
        """
        Perform BOLT handshake.

        :param resolved_address:
        :param deadline: Deadline for handshake

        :returns: (version, client_handshake, server_response_data)
        """
        local_port = self.getsockname()[1]

        # TODO: Optimize logging code
        handshake = self.Bolt.get_handshake()
        handshake = struct.unpack(">16B", handshake)
        handshake = [handshake[i : i + 4] for i in range(0, len(handshake), 4)]

        supported_versions = [
            f"0x{vx[0]:02X}{vx[1]:02X}{vx[2]:02X}{vx[3]:02X}"
            for vx in handshake
        ]

        log.debug(
            "[#%04X]  C: <MAGIC> 0x%08X",
            local_port,
            int.from_bytes(self.Bolt.MAGIC_PREAMBLE, byteorder="big"),
        )
        log.debug(
            "[#%04X]  C: <HANDSHAKE> %s %s %s %s",
            local_port,
            *supported_versions,
        )

        request = self.Bolt.MAGIC_PREAMBLE + self.Bolt.get_handshake()

        # Handle the handshake response
        original_timeout = self.gettimeout()
        self.settimeout(deadline.to_timeout())
        try:
            self.sendall(request)
            response = self.recv(4)
        except OSError as exc:
            raise ServiceUnavailable(
                f"Failed to read any data from server {resolved_address!r} "
                f"after connected (deadline {deadline})"
            ) from exc
        finally:
            self.settimeout(original_timeout)
        data_size = len(response)
        if data_size == 0:
            # If no data is returned after a successful select
            # response, the server has closed the connection
            log.debug("[#%04X]  S: <CLOSE>", local_port)
            self.close()
            raise ServiceUnavailable(
                f"Connection to {resolved_address} closed without handshake "
                "response"
            )
        if data_size != 4:
            # Some garbled data has been received
            log.debug("[#%04X]  S: @*#!", local_port)
            self.close()
            raise BoltProtocolError(
                "Expected four byte Bolt handshake response from "
                f"{resolved_address!r}, received {response!r} instead; "
                "check for incorrect port number",
                address=resolved_address,
            )
        elif response == b"HTTP":
            log.debug("[#%04X]  S: <CLOSE>", local_port)
            self.close()
            raise ServiceUnavailable(
                f"Cannot to connect to Bolt service on {resolved_address!r} "
                "(looks like HTTP)"
            )
        agreed_version = response[-1], response[-2]
        log.debug(
            "[#%04X]  S: <HANDSHAKE> 0x%06X%02X",
            local_port,
            agreed_version[1],
            agreed_version[0],
        )
        return agreed_version, handshake, response

    @classmethod
    def connect(
        cls,
        address,
        *,
        tcp_timeout,
        deadline,
        custom_resolver,
        ssl_context,
        keep_alive,
    ):
        """
        Connect and perform a handshake.

        Return a valid Connection object, assuming a protocol version can be
        agreed.
        """
        errors = []
        failed_addresses = []
        # Establish a connection to the host and port specified
        # Catches refused connections see:
        # https://docs.python.org/2/library/errno.html

        resolved_addresses = NetworkUtil.resolve_address(
            addressing.Address(address), resolver=custom_resolver
        )
        for resolved_address in resolved_addresses:
            deadline_timeout = deadline.to_timeout()
            if (
                deadline_timeout is not None
                and deadline_timeout <= tcp_timeout
            ):
                tcp_timeout = deadline_timeout
            s = None
            try:
                s = cls._connect_secure(
                    resolved_address, tcp_timeout, keep_alive, ssl_context
                )
                return (s, *s._handshake(resolved_address, deadline))
            except (BoltError, DriverError, OSError) as error:
                try:
                    local_port = s.getsockname()[1]
                except (OSError, AttributeError, TypeError):
                    local_port = 0
                err_str = error.__class__.__name__
                if str(error):
                    err_str += ": " + str(error)
                log.debug(
                    "[#%04X]  S: <CONNECTION FAILED> %s %s",
                    local_port,
                    resolved_address,
                    err_str,
                )
                if s:
                    cls.close_socket(s)
                errors.append(error)
                failed_addresses.append(resolved_address)
            except asyncio.CancelledError:
                try:
                    local_port = s.getsockname()[1]
                except (OSError, AttributeError, TypeError):
                    local_port = 0
                log.debug(
                    "[#%04X]  C: <CANCELED> %s", local_port, resolved_address
                )
                if s:
                    with suppress(OSError):
                        s.kill()
                raise
            except Exception:
                if s:
                    cls.close_socket(s)
                raise
        address_strs = tuple(map(str, failed_addresses))
        if not errors:
            raise ServiceUnavailable(
                f"Couldn't connect to {address} (resolved to {address_strs})"
            )
        else:
            error_strs = "\n".join(map(str, errors))
            raise ServiceUnavailable(
                f"Couldn't connect to {address} (resolved to {address_strs}):"
                f"\n{error_strs}"
            ) from errors[0]
