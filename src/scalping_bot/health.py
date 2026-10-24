from __future__ import annotations

import sqlite3
import sys
from pathlib import Path


def check(database: Path) -> None:
    connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True, timeout=1)
    try:
        if connection.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise RuntimeError("SQLite quick check failed")
        connection.execute(
            "SELECT operating_mode FROM runtime_state WHERE singleton = 1"
        ).fetchone()
    finally:
        connection.close()


if __name__ == "__main__":
    check(Path(sys.argv[1] if len(sys.argv) > 1 else "state/scalping-bot.db"))
