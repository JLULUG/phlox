import logging as log
from typing import Any

import os
from datetime import datetime

import aiohttp

from . import BadUpstream, VerificationFailed
from .db import local_state, local_dists, Distribution
from .util import dist_rel_path
from .upstream import Upstream
from .verify import verify_file
from .delete import delete_dist
from .filter import filter_metadata


async def generate_simple_page(package: str, metadata: dict[str, Any]) -> None:
    log.debug("generating simple page for %s", package)
    files = "\n".join(
        [
            f'    <a href="../../{dist_rel_path(file['digests']['blake2b_256'], file['filename'])}#sha256={file['digests']['sha256']}">{file['filename']}</a><br/>'
            for release in metadata["releases"].values()
            for file in release
        ]
    )
    html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta name="pypi:repository-version" content="1.0">
    <title>Links for {package}</title>
</head>
<body>
    <h1>Links for {package}</h1>
{files}
</body>
</html>
<!--SERIAL {metadata['last_serial']}-->
"""
    os.makedirs(f"simple/{package}", exist_ok=True)
    with open(f"simple/{package}/index.html", "w", encoding="utf-8") as f:
        f.write(html)


async def generate_global_simple_page() -> None:
    packages = "\n".join(
        [
            f'    <a href="{package}/">{package}</a><br/>'
            for package, _ in sorted(local_state)
        ]
    )
    html = f"""
<html>
  <head>
    <meta name="pypi:repository-version" content="1.0">
    <title>Simple Index</title>
  </head>
  <body>
{packages}
  </body>
</html>
"""
    os.makedirs("simple", exist_ok=True)
    with open("simple/index.html", "w", encoding="utf-8") as f:
        f.write(html)


async def sync(package: str, upstream: Upstream) -> None:
    try:
        metadata = await upstream.query_metadata(package)
    except aiohttp.ClientResponseError as e:
        if e.code == 404:
            log.error("metadata of package %s is not found", package)
            return
        raise

    if package in local_state:
        if metadata["last_serial"] < local_state[package]:
            raise BadUpstream(
                f"local serial is newer than upstream for package {package}"
            )

    filtered = filter_metadata(package, metadata)
    local_file = {dist.blake: dist for dist in local_dists.by_package(package)}
    for release in filtered["releases"].values():
        for file in release:
            rel_path = dist_rel_path(file["digests"]["blake2b_256"], file["filename"])

            try:
                if file["digests"]["blake2b_256"] in local_file:
                    verify_file(rel_path, file["size"], file["digests"]["sha256"])
                    log.debug("skipping file %s", rel_path)
                    local_file.pop(file["digests"]["blake2b_256"])
                    continue
            except VerificationFailed:
                log.warning("File {rel_path} in database but not correct!")

            log.debug("downloading %s", rel_path)
            os.makedirs(os.path.dirname(rel_path), exist_ok=True)
            await upstream.fetch_dist(file, rel_path)
            local_dists.add(
                Distribution(
                    file["digests"]["blake2b_256"],
                    file["digests"]["sha256"],
                    file["filename"],
                    package,
                    file["size"],
                    int(
                        datetime.fromisoformat(file["upload_time_iso_8601"]).timestamp()
                    ),
                )
            )

    for dist in local_file.values():
        delete_dist(dist)

    await generate_simple_page(package, metadata)
    local_state[package] = metadata["last_serial"]
