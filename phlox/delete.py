import logging as log

import os
import shutil

from . import VerificationFailed
from .db import local_state, local_dists, Distribution
from .util import dist_rel_path


def delete_dist(dist: Distribution) -> None:
    rel_path = dist_rel_path(dist.blake, dist.name)
    log.debug("deleting %s", rel_path)
    local_dists.delete(dist.blake)
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
    if package not in local_state:
        raise VerificationFailed(f"package {package} does not exist")
    del local_state[package]
    dists = local_dists.by_package(package)
    for dist in dists:
        delete_dist(dist)
    shutil.rmtree(f"simple/{package}/")
    # shutil.rmtree(f"pypi/{package}/")
