from typing import Any
import logging as log

from abc import ABC, abstractmethod
import shutil
import asyncio

import aiohttp
import aiohttp.http_exceptions
from aiohttp_xmlrpc.client import ServerProxy  # type: ignore

from . import USER_AGENT

log.getLogger("aiohttp_xmlrpc.client").setLevel(log.WARNING)


class Upstream(ABC):
    def __init__(
        self,
        base_url: str = "https://pypi.org",
        base_file_url: str = "https://files.pythonhosted.org",
    ) -> None:
        self.base_url = base_url
        self.base_file_url = base_file_url
        self.session = aiohttp.ClientSession(
            headers={"User-Agent": USER_AGENT},
            raise_for_status=True,
        )

    def __del__(self, *exc: Any) -> None:
        asyncio.run(self.session.close())

    @abstractmethod
    async def list_packages(self) -> dict[str, int]: ...

    @abstractmethod
    async def query_metadata(self, package: str) -> dict[str, Any]: ...

    @abstractmethod
    async def fetch_dist(self, file_spec: dict[str, Any], target: str) -> None: ...


class XMLRPC(Upstream):
    def __init__(
        self,
        base_url: str = "https://pypi.org",
        base_file_url: str = "https://files.pythonhosted.org",
    ) -> None:
        super().__init__(base_url, base_file_url)
        self.rpc = ServerProxy(
            f"{self.base_url}/pypi",
            client=self.session,
            headers={"User-Agent": USER_AGENT},
        )

    async def list_packages(self) -> dict[str, int]:
        try:
            return self._serial_cache
        except AttributeError:
            # pylint: disable-next=attribute-defined-outside-init
            self._serial_cache: dict[str, int] = (
                await self.rpc.list_packages_with_serial()
            )
            return self._serial_cache


class SimpleV1JSON(Upstream):
    async def list_packages(self) -> dict[str, int]:
        async with self.session.get(
            f"{self.base_url}/simple/",
            headers={"Accept": "application/vnd.pypi.simple.v1+json"},
        ) as r:
            return {
                project["name"]: project["_last-serial"]
                for project in (await r.json())["projects"]
            }


class JSONMetadata(Upstream):
    async def query_metadata(self, package: str) -> dict[str, Any]:
        log.debug("accessing metadata of %s", package)
        async with self.session.get(f"{self.base_url}/pypi/{package}/json") as r:
            return await r.json()  # type: ignore


async def _write_response_to_file(
    response: aiohttp.client.ClientResponse, target: str
) -> None:
    with open(target, "wb") as f:
        async for chunk in response.content.iter_chunked(1024**2):
            f.write(chunk)


class DirectDownload(Upstream):
    async def fetch_dist(self, file_spec: dict[str, Any], target: str) -> None:
        async with self.session.get(file_spec["url"]) as r:
            await _write_response_to_file(r, target)


class MirrorDownload(DirectDownload):
    def __init__(
        self,
        mirror_url: str,
        base_url: str = "https://pypi.org",
        base_file_url: str = "https://files.pythonhosted.org",
    ) -> None:
        super().__init__(base_url, base_file_url)
        self.mirror_url = mirror_url

    async def fetch_dist(self, file_spec: dict[str, Any], target: str) -> None:
        try:
            async with self.session.get(
                self.mirror_url + file_spec["url"].removeprefix(self.base_file_url)
            ) as r:
                await _write_response_to_file(r, target)
        except aiohttp.ClientError as e:
            log.warning(
                "error downloading %s from mirror, falling back",
                file_spec["filename"],
                exc_info=e,
            )
            await super().fetch_dist(file_spec, target)


class CopyFromLocal(DirectDownload):
    def __init__(
        self,
        local_path: str,
        base_url: str = "https://pypi.org",
        base_file_url: str = "https://files.pythonhosted.org",
    ) -> None:
        super().__init__(base_url, base_file_url)
        self.local_path = local_path

    async def fetch_dist(self, file_spec: dict[str, Any], target: str) -> None:
        try:
            src = self.local_path + file_spec["url"].removeprefix(self.base_file_url)
            shutil.copyfile(src, target)
        except (OSError, shutil.Error) as e:
            log.warning("error copying %s, falling back", src, exc_info=e)
            await super().fetch_dist(file_spec, target)


class PyPIUpstream(SimpleV1JSON, JSONMetadata, DirectDownload):
    pass
