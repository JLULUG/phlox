from typing import Any

import re
from copy import deepcopy


def filter_metadata(package: str, metadata: dict[str, Any]) -> dict[str, Any]:
    d = deepcopy(metadata)
    if package in (
        "uselesscapitalquiz",
    ) or any(
        re.match(pattern, package)
        for pattern in (
            r".+-nightly(-|$)",
        )
    ):
        d["releases"] = {}
        return d

    # ...

    return d
