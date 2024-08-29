import sys
import asyncio

from . import phlox

sys.exit(asyncio.run(phlox.main()))
