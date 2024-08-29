import logging as log

import os
import shutil

from .db import local_state, local_dists
from .util import dist_rel_path


async def delete_dist(blake: str, filename: str) -> None:
    rel_path = dist_rel_path(blake, filename)
    log.debug("deleting %s", rel_path)
    local_dists.delete(blake)
    try:
        os.unlink(rel_path)
    except FileNotFoundError:
        pass
    for _ in range(3):
        rel_path = os.path.dirname(rel_path)
        try:
            os.rmdir(rel_path)
        except OSError:
            break


async def delete(package: str) -> None:
    del local_state[package]
    dists = local_dists.by_package(package)
    for dist in dists:
        await delete_dist(dist.blake, dist.name)
    shutil.rmtree(f"simple/{package}/")
    # shutil.rmtree(f"pypi/{package}/")
