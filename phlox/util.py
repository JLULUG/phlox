import re


def canonicalize_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def dist_rel_path(blake: str, filename: str) -> str:
    return f"packages/{blake[0:2]}/{blake[2:4]}/{blake[4:]}/{filename}"
