import asyncio
import logging
import socket

from ... import addressing


log = logging.getLogger("neo4j.io")


def _resolved_addresses_from_info(info, host_name):
    resolved = []
    for fam, _, _, _, addr in info:
        if fam == socket.AF_INET6 and addr[3] != 0:
            # skip any IPv6 addresses with a non-zero scope id
            # as these appear to cause problems on some platforms
            continue
        if addr not in resolved:
            resolved.append(addr)
            yield addressing.ResolvedAddress(
                addr, host_name=host_name
            )


class AsyncNetworkUtil:
    @staticmethod
    async def get_address_info(host, port, *,
                               family=0, type=0, proto=0, flags=0):
        loop = asyncio.get_event_loop()
        return await loop.getaddrinfo(
            host, port, family=family, type=type, proto=proto, flags=flags
        )

    @staticmethod
    async def _dns_resolver(address, family=0):
        """ Regular DNS resolver. Takes an address object and optional
        address family for filtering.

        :param address:
        :param family:
        :returns:
        """
        try:
            info = await AsyncNetworkUtil.get_address_info(
                address.host, address.port, family=family,
                type=socket.SOCK_STREAM
            )
        except OSError:
            raise ValueError("Cannot resolve address {}".format(address))
        return list(_resolved_addresses_from_info(info, address._host_name))

    @staticmethod
    async def resolve_address(address, family=0, resolver=None):
        """ Carry out domain name resolution on this Address object.

        If a resolver function is supplied, and is callable, this is
        called first, with this object as its argument. This may yield
        multiple output addresses, which are chained into a subsequent
        regular DNS resolution call. If no resolver function is passed,
        the DNS resolution is carried out on the original Address
        object.

        This function returns a list of resolved Address objects.

        :param address: the Address to resolve
        :param family: optional address family to filter resolved
                       addresses by (e.g. `socket.AF_INET6`)
        :param resolver: optional customer resolver function to be
                         called before regular DNS resolution
        """
        if isinstance(address, addressing.ResolvedAddress):
            yield address
            return

        log.debug("[#0000]  _: <RESOLVE> in: %s", address)
        if resolver:
            if asyncio.iscoroutinefunction(resolver):
                resolved_addresses = await resolver(address)
            else:
                resolved_addresses = resolver(address)
            for address in map(addressing.Address, resolved_addresses):
                log.debug("[#0000]  _: <RESOLVE> custom resolver out: %s",
                          address)
                for resolved_address in await AsyncNetworkUtil._dns_resolver(
                    address, family=family
                ):
                    log.debug("[#0000]  _: <RESOLVE> dns resolver out: %s",
                              resolved_address)
                    yield resolved_address
        else:
            for resolved_address in await AsyncNetworkUtil._dns_resolver(
                address, family=family
            ):
                log.debug("[#0000]  _: <RESOLVE> dns resolver out: %s",
                          resolved_address)
                yield resolved_address


class NetworkUtil:
    @staticmethod
    def get_address_info(host, port, *, family=0, type=0, proto=0, flags=0):
        return socket.getaddrinfo(host, port, family, type, proto, flags)

    @staticmethod
    def _dns_resolver(address, family=0):
        """ Regular DNS resolver. Takes an address object and optional
        address family for filtering.

        :param address:
        :param family:
        :returns:
        """
        try:
            info = NetworkUtil.get_address_info(
                address.host, address.port, family=family,
                type=socket.SOCK_STREAM
            )
        except OSError:
            raise ValueError("Cannot resolve address {}".format(address))
        return _resolved_addresses_from_info(info, address._host_name)

    @staticmethod
    def resolve_address(address, family=0, resolver=None):
        """ Carry out domain name resolution on this Address object.

        If a resolver function is supplied, and is callable, this is
        called first, with this object as its argument. This may yield
        multiple output addresses, which are chained into a subsequent
        regular DNS resolution call. If no resolver function is passed,
        the DNS resolution is carried out on the original Address
        object.

        This function returns a list of resolved Address objects.

        :param address: the Address to resolve
        :param family: optional address family to filter resolved
                       addresses by (e.g. `socket.AF_INET6`)
        :param resolver: optional customer resolver function to be
                         called before regular DNS resolution
        """
        if isinstance(address, addressing.ResolvedAddress):
            yield address
            return

        log.debug("[#0000]  _: <RESOLVE> in: %s", address)
        if resolver:
            for address in map(addressing.Address, resolver(address)):
                log.debug("[#0000]  _: <RESOLVE> custom resolver out: %s",
                          address)
                for resolved_address in NetworkUtil._dns_resolver(
                    address, family=family
                ):
                    log.debug("[#0000]  _: <RESOLVE> dns resolver out: %s",
                              resolved_address)
                    yield resolved_address
        else:
            for resolved_address in NetworkUtil._dns_resolver(
                address, family=family
            ):
                log.debug("[#0000]  _: <RESOLVE> dns resolver out: %s",
                          resolved_address)
                yield resolved_address
