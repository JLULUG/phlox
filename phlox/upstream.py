from typing import Any
import logging as log

import os
from abc import ABC, abstractmethod

import aiohttp
from aiohttp_xmlrpc.client import ServerProxy  # type: ignore

from . import USER_AGENT
from .util import dist_rel_path

log.getLogger("aiohttp_xmlrpc.client").setLevel(log.WARNING)


class Upstream(ABC):

    @abstractmethod
    async def __aenter__(self) -> "Upstream": ...

    @abstractmethod
    async def __aexit__(self, *exc: Any) -> None: ...

    @abstractmethod
    async def list_packages(self) -> dict[str, int]: ...

    @abstractmethod
    async def query_metadata(self, package: str) -> dict[str, Any]: ...

    @abstractmethod
    async def download_dist(self, blake: str, filename: str) -> None: ...


class PyPIUpstream(Upstream):
    def __init__(self, base_url: str = "https://pypi.org") -> None:
        self.base_url = base_url

    async def __aenter__(self) -> "PyPIUpstream":
        # pylint: disable=attribute-defined-outside-init
        self.session = aiohttp.ClientSession(
            headers={"User-Agent": USER_AGENT},
            raise_for_status=True,
        )
        self.xmlrpc = ServerProxy(
            f"{self.base_url}/pypi",
            client=self.session,
            headers={"User-Agent": USER_AGENT},
        )
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.session.close()

    async def list_packages(self) -> dict[str, int]:
        try:
            return self._serial_cache
        except AttributeError:
            # pylint: disable-next=attribute-defined-outside-init
            self._serial_cache: dict[str, int] = (
                await self.xmlrpc.list_packages_with_serial()
            )
            return self._serial_cache

    async def query_metadata(self, package: str) -> dict[str, Any]:
        log.debug("accessing metadata of %s", package)
        async with self.session.get(f"{self.base_url}/pypi/{package}/json") as r:
            return await r.json()  # type: ignore

    async def download_dist(self, blake: str, filename: str) -> None:
        rel_path = dist_rel_path(blake, filename)
        log.debug("downloading %s", rel_path)
        os.makedirs(os.path.dirname(rel_path), exist_ok=True)
        async with self.session.get(f"{self.base_url}/{rel_path}") as r:
            with open(rel_path, "wb") as f:
                async for chunk in r.content.iter_chunked(1024**2):
                    f.write(chunk)
