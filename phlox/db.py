from collections.abc import Iterable, Iterator
import logging as log

import sqlite3
from collections import namedtuple

Distribution = namedtuple(
    "Distribution", ("blake", "sha256", "name", "package", "size", "date")
)


class SerialRegistry(Iterable[tuple[str, int]]):
    def __init__(self) -> None:
        self.con = sqlite3.connect("serial.db")
        with self.con:
            self.con.execute(
                "CREATE TABLE IF NOT EXISTS t("
                "package TEXT PRIMARY KEY ON CONFLICT REPLACE,"
                "serial INT NOT NULL)"
            )
            self.con.execute("CREATE INDEX IF NOT EXISTS i_serial ON t(serial)")

    def __contains__(self, package: str) -> bool:
        with self.con:
            return bool(
                self.con.execute(
                    "SELECT serial FROM t WHERE package = ?",
                    (package,),
                ).fetchone()
            )

    def __getitem__(self, package: str) -> int:
        with self.con:
            row = self.con.execute(
                "SELECT serial FROM t WHERE package = ?",
                (package,),
            ).fetchone()
        if not row:
            raise KeyError("package specified not found")
        return int(row[0])

    def __setitem__(self, package: str, serial: int) -> None:
        log.debug("setting serial for %s to %d", package, serial)
        with self.con:
            self.con.execute("INSERT INTO t VALUES(?,?)", (package, serial))

    def __delitem__(self, package: str) -> None:
        log.debug("deleting state of %s", package)
        with self.con:
            self.con.execute("DELETE FROM t WHERE package = ?", (package,))

    def update(self, package_serials: dict[str, int]) -> None:
        with self.con:
            self.con.executemany("INSERT INTO t VALUES(?,?)", package_serials.items())

    def __iter__(self) -> Iterator[tuple[str, int]]:
        with self.con:
            rows = self.con.execute("SELECT * FROM t").fetchall()
        yield from map(tuple, rows)

    def __len__(self) -> int:
        with self.con:
            row = self.con.execute("SELECT COUNT(*) FROM t").fetchone()
        return int(row[0])


class DistRegistry:
    def __init__(self) -> None:
        self.con = sqlite3.connect("files.db")
        with self.con:
            self.con.execute(
                "CREATE TABLE IF NOT EXISTS t("
                "blake TEXT PRIMARY KEY ON CONFLICT REPLACE,"
                "sha256 TEXT UNIQUE,"
                "name TEXT UNIQUE,"
                "package TEXT NOT NULL,"
                "size INT NOT NULL,"
                "date INT NOT NULL)",
            )
            self.con.execute("CREATE INDEX IF NOT EXISTS i_package ON t(package)")
            self.con.execute("CREATE INDEX IF NOT EXISTS i_size ON t(size)")
            self.con.execute("CREATE INDEX IF NOT EXISTS i_date ON t(date)")

    def add(self, dist: Distribution) -> None:
        log.debug("adding file %s (%s)", dist.name, dist.blake)
        with self.con:
            self.con.execute("INSERT INTO t VALUES(?,?,?,?,?,?)", dist)

    def extend(self, dists: Iterable[Distribution]) -> None:
        with self.con:
            self.con.executemany("INSERT INTO t VALUES(?,?,?,?,?,?)", dists)

    def delete(self, blake: str) -> None:
        log.debug("adding file %s", blake)
        with self.con:
            self.con.execute("DELETE FROM t WHERE blake = ?", (blake,))

    def by_blake(self, blake: str) -> Distribution | None:
        with self.con:
            row = self.con.execute(
                "SELECT * FROM t WHERE blake = ?",
                (blake,),
            ).fetchone()
        return Distribution._make(row) if row else None

    def by_package(self, package: str) -> Iterable[Distribution]:
        with self.con:
            rows = self.con.execute(
                "SELECT * FROM t WHERE package = ?", (package,)
            ).fetchall()
        yield from map(Distribution._make, rows)


local_state = SerialRegistry()
local_dists = DistRegistry()
