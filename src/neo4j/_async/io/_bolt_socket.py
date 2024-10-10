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


from __future__ import annotations

import asyncio
import dataclasses
import logging
import struct
import typing as t
from contextlib import suppress

from ... import addressing
from ..._async_compat.network import (
    AsyncBoltSocketBase,
    AsyncNetworkUtil,
)
from ..._exceptions import (
    BoltError,
    BoltProtocolError,
)
from ...exceptions import (
    DriverError,
    ServiceUnavailable,
)


if t.TYPE_CHECKING:
    from ..._deadline import Deadline


log = logging.getLogger("neo4j.io")


@dataclasses.dataclass
class HandshakeCtx:
    ctx: str
    deadline: Deadline
    local_port: int
    resolved_address: addressing.ResolvedAddress
    full_response: bytearray = dataclasses.field(default_factory=bytearray)


@dataclasses.dataclass
class BytesPrinter:
    bytes: bytes | bytearray

    def __str__(self):
        return f"0x{self.bytes.hex().upper()}"


class AsyncBoltSocket(AsyncBoltSocketBase):
    async def _parse_handshake_response_v1(self, ctx, response):
        agreed_version = response[-1], response[-2]
        log.debug(
            "[#%04X]  S: <HANDSHAKE v1> 0x%06X%02X",
            ctx.local_port,
            agreed_version[1],
            agreed_version[0],
        )
        return agreed_version

    async def _parse_handshake_response_v2(self, ctx, response):
        ctx.ctx = "handshake v2 offerings count"
        num_offerings = await self._read_varint(ctx)
        offerings = []
        for i in range(num_offerings):
            ctx.ctx = f"handshake v2 offering {i}"
            offering_response = await self._handshake_read(ctx, 4)
            offering = offering_response[-1:-4:-1]
            offerings.append(offering)
        ctx.ctx = "handshake v2 capabilities"
        _capabilities_offer = await self._read_varint(ctx)

        if log.getEffectiveLevel() >= logging.DEBUG:
            log.debug(
                "[#%04X]  S: <HANDSHAKE v2> %s [%i] %s %s",
                ctx.local_port,
                BytesPrinter(response),
                num_offerings,
                " ".join(
                    f"0x{vx[2]:04X}{vx[1]:02X}{vx[0]:02X}" for vx in offerings
                ),
                BytesPrinter(self._encode_varint(_capabilities_offer)),
            )

        supported_versions = sorted(self.Bolt.protocol_handlers().keys())
        chosen_version = 0, 0
        for v in supported_versions:
            for offer_major, offer_minor, offer_range in offerings:
                offer_max = (offer_major, offer_minor)
                offer_min = (offer_major, offer_minor - offer_range)
                if offer_min <= v <= offer_max:
                    chosen_version = v
                    break

        ctx.ctx = "handshake v2 chosen version"
        await self._handshake_send(
            ctx, bytes((0, 0, chosen_version[1], chosen_version[0]))
        )
        chosen_capabilities = 0
        capabilities = self._encode_varint(chosen_capabilities)
        ctx.ctx = "handshake v2 chosen capabilities"
        log.debug(
            "[#%04X]  C: <HANDSHAKE v2> 0x%06X%02X %s",
            ctx.local_port,
            chosen_version[1],
            chosen_version[0],
            BytesPrinter(capabilities),
        )
        await self._handshake_send(ctx, b"\x00")

        return chosen_version

    async def _read_varint(self, ctx):
        next_byte = (await self._handshake_read(ctx, 1))[0]
        res = next_byte & 0x7F
        i = 0
        while next_byte & 0x80:
            i += 1
            next_byte = (await self._handshake_read(ctx, 1))[0]
            res += (next_byte & 0x7F) << (7 * i)
        return res

    @staticmethod
    def _encode_varint(n):
        res = bytearray()
        while n >= 0x80:
            res.append(n & 0x7F | 0x80)
            n >>= 7
        res.append(n)
        return res

    async def _handshake_read(self, ctx, n):
        original_timeout = self.gettimeout()
        self.settimeout(ctx.deadline.to_timeout())
        try:
            response = await self.recv(n)
            ctx.full_response.extend(response)
        except OSError as exc:
            raise ServiceUnavailable(
                f"Failed to read {ctx.ctx} from server "
                f"{ctx.resolved_address!r} (deadline {ctx.deadline})"
            ) from exc
        finally:
            self.settimeout(original_timeout)
        data_size = len(response)
        if data_size == 0:
            # If no data is returned after a successful select
            # response, the server has closed the connection
            log.debug("[#%04X]  S: <CLOSE>", ctx.local_port)
            await self.close()
            raise ServiceUnavailable(
                f"Connection to {ctx.resolved_address} closed with incomplete "
                f"handshake response"
            )
        if data_size != n:
            # Some garbled data has been received
            log.debug("[#%04X]  S: @*#!", ctx.local_port)
            await self.close()
            raise BoltProtocolError(
                f"Expected {ctx.ctx} from {ctx.resolved_address!r}, received "
                f"{response!r} instead (so far {ctx.full_response!r}); "
                "check for incorrect port number",
                address=ctx.resolved_address,
            )

        return response

    async def _handshake_send(self, ctx, data):
        original_timeout = self.gettimeout()
        self.settimeout(ctx.deadline.to_timeout())
        try:
            await self.sendall(data)
        except OSError as exc:
            raise ServiceUnavailable(
                f"Failed to write {ctx.ctx} to server "
                f"{ctx.resolved_address!r} (deadline {ctx.deadline})"
            ) from exc
        finally:
            self.settimeout(original_timeout)

    async def _handshake(self, resolved_address, deadline):
        """
        Perform BOLT handshake.

        :param resolved_address:
        :param deadline: Deadline for handshake

        :returns: (version, client_handshake, server_response_data)
        """
        local_port = self.getsockname()[1]

        if log.getEffectiveLevel() >= logging.DEBUG:
            handshake = self.Bolt.get_handshake()
            handshake = struct.unpack(">16B", handshake)
            handshake = [
                handshake[i : i + 4] for i in range(0, len(handshake), 4)
            ]

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

        ctx = HandshakeCtx(
            ctx="handshake opening",
            deadline=deadline,
            local_port=local_port,
            resolved_address=resolved_address,
        )

        await self._handshake_send(ctx, request)

        ctx.ctx = "four byte Bolt handshake response"
        response = await self._handshake_read(ctx, 4)

        if response == b"HTTP":
            log.debug("[#%04X]  S: <CLOSE>", local_port)
            await self.close()
            raise ServiceUnavailable(
                f"Cannot to connect to Bolt service on {resolved_address!r} "
                "(looks like HTTP)"
            )
        elif response[-1] == 0xFF:
            # manifest style handshake
            manifest_version = response[-2]
            if manifest_version == 0x01:
                agreed_version = await self._parse_handshake_response_v2(
                    ctx,
                    response,
                )
            else:
                raise BoltProtocolError(
                    "Unsupported Bolt handshake manifest version "
                    f"{manifest_version} received from {resolved_address!r}.",
                    address=resolved_address,
                )
        else:
            agreed_version = await self._parse_handshake_response_v1(
                ctx,
                response,
            )

        return agreed_version, handshake, response

    @classmethod
    async def connect(
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

        resolved_addresses = AsyncNetworkUtil.resolve_address(
            addressing.Address(address), resolver=custom_resolver
        )
        async for resolved_address in resolved_addresses:
            deadline_timeout = deadline.to_timeout()
            if (
                deadline_timeout is not None
                and deadline_timeout <= tcp_timeout
            ):
                tcp_timeout = deadline_timeout
            s = None
            try:
                s = await cls._connect_secure(
                    resolved_address, tcp_timeout, keep_alive, ssl_context
                )
                agreed_version, handshake, response = await s._handshake(
                    resolved_address, deadline
                )
                return s, agreed_version, handshake, response
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
                    await cls.close_socket(s)
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
                    await cls.close_socket(s)
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
