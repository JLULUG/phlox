import os
import hashlib

from . import VerificationFailed

# from .phlox import arg
from .db import local_state, local_dists
from .util import dist_rel_path


def verify_file(path: str, size: int, sha256: str) -> None:
    from .phlox import arg  # pylint: disable=cyclic-import

    if not os.path.isfile(path):
        raise VerificationFailed(f"file {path} not found")
    if os.stat(path).st_size != size:
        raise VerificationFailed(f"size of file {path} mismatch")
    if arg.hash:
        with open(path, "rb") as f:
            if hashlib.sha256(f.read()).hexdigest() != sha256:
                raise VerificationFailed(f"sha256 of file {path} mismatch")


async def verify(package: str) -> None:
    if package not in local_state:
        raise VerificationFailed(f"failed verifying package {package}")
    dists = local_dists.by_package(package)
    with open("simple/{package}/index.html", "r", encoding="utf-8") as f:
        simple_page = f.read()

    for dist in dists:
        rel_path = dist_rel_path(dist.blake, dist.name)
        verify_file(rel_path, dist.size, dist.sha256)
        if not (dist.name in simple_page and dist.sha256 in simple_page):
            raise VerificationFailed("simple page corrupted")
