from typing import Any
import logging as log

import os
import sys
import asyncio
import argparse
from functools import partial

from . import USER_AGENT
from .db import local_state
from .upstream import PyPIUpstream
from .sync import sync, generate_global_simple_page
from .verify import verify
from .delete import delete


arg: argparse.Namespace


def _parse_args() -> None:
    global arg  # pylint: disable=global-statement
    argparser = argparse.ArgumentParser(
        description="A lightweight tool to mirror Python Package Index (PyPI).",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    argparser.add_argument(
        "-V",
        "--version",
        action="version",
        version=USER_AGENT,
        help="Dispaly program version",
    )
    argparser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    argparser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Supress info logging",
    )
    argparser.add_argument(
        "-w",
        "--worker",
        default=1,
        type=int,
        metavar="N",
        help="Concurrent syncing thread\nDefaults to 4 for sync, 1 for others",
    )
    argparser.add_argument(
        "-H",
        "--hash",
        action="store_true",
        help="Calculate hash in file operations",
    )
    argparser.add_argument(
        "-d",
        "--dir",
        default=".",
        type=str,
        help="Location of local repository\nDefaults to current directory",
    )
    funcs_group = argparser.add_argument_group("command")
    funcs = funcs_group.add_mutually_exclusive_group(required=True)
    funcs.add_argument(
        "--sync",
        action="store_true",
        help="Sync packages",
    )
    funcs.add_argument(
        "--verify",
        action="store_true",
        help="Verify the integrity of local repository",
    )
    funcs.add_argument(
        "--delete",
        action="store_true",
        help="Delete specified packages",
    )
    packages = argparser.add_argument_group()
    packages.add_argument(
        "packages",
        nargs="*",
        type=str,
        help="Specify packages to sync, verify or delete\nDefaults to all if not specified",
    )
    arg = argparser.parse_args()

    arg.worker = arg.worker or (4 if arg.sync else 1)
    if arg.delete and not arg.packages:
        argparser.error("packages must be specified for --delete")


async def _worker(queue: asyncio.Queue[str], func: Any) -> None:
    while True:
        try:
            package = queue.get_nowait()
        except asyncio.QueueEmpty:
            return
        log.info("Processing package %s ...", package)
        try:
            await func(package)
        except Exception:  # pylint: disable=broad-exception-caught
            log.exception("Failed processing package %s", package)


async def main() -> None:
    _parse_args()

    log.basicConfig(
        format="%(asctime)s %(levelname)s %(name)s: %(message)s (%(filename)s:%(lineno)d)",
        level=(log.DEBUG if arg.verbose else log.WARN if arg.quiet else log.INFO),
    )

    log.debug("args: %s", arg)

    try:
        os.chdir(arg.dir)
    except OSError:
        log.critical("local repository does not exist")
        sys.exit(3)

    def _check_package_exist(repository: Any) -> None:
        if not_found := [x for x in arg.packages if x not in repository]:
            log.critical("packages not found %s", not_found)
            sys.exit(3)

    if arg.sync:
        log.info("Fetching package serials...")
        upstream = PyPIUpstream()
        remote_state = await upstream.list_packages()
        # local_state.update(remote_state)
        if arg.packages:
            _check_package_exist(remote_state)
            targets = arg.packages
        else:
            targets = [x[0] for x in set(remote_state.items()) ^ set(local_state)]
    else:
        if arg.packages:
            _check_package_exist(local_state)
            targets = arg.packages
        else:
            targets = set(package for package, _ in local_state)

    log.debug("targets: %s", targets)

    target_queue: asyncio.Queue[str] = asyncio.Queue()
    for package in sorted(set(targets)):
        target_queue.put_nowait(package)

    log.warning("%d packages to process", target_queue.qsize())

    # fmt: off
    await asyncio.gather(*[_worker(target_queue, (
        verify if arg.verify else
        delete if arg.delete else
        partial(sync, upstream=upstream)
    )) for _ in range(arg.worker)])
    # fmt: on

    log.info("Generating global simple page ...")
    await generate_global_simple_page()

    log.warning("Finished")
