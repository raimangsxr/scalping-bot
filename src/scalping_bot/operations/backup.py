from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken


class EncryptedSQLiteBackup:
    RPO_SECONDS = 5 * 60
    RTO_SECONDS = 30 * 60

    def __init__(self, key: bytes) -> None:
        self.cipher = Fernet(key)

    def create(self, database: Path, *, destination: Path, created_ms: int) -> Path:
        destination.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(suffix=".sqlite") as temporary:
            source = sqlite3.connect(database)
            target = sqlite3.connect(temporary.name)
            try:
                source.backup(target)
            finally:
                target.close()
                source.close()
            plaintext = Path(temporary.name).read_bytes()
        artifact = destination / f"state-{created_ms}.sqlite.fernet"
        artifact.write_bytes(self.cipher.encrypt(plaintext))
        return artifact

    def restore_and_verify(self, artifact: Path, *, destination: Path) -> Path:
        destination.mkdir(parents=True, exist_ok=True)
        try:
            plaintext = self.cipher.decrypt(artifact.read_bytes())
        except InvalidToken as error:
            raise ValueError("backup could not be decrypted") from error
        restored = destination / artifact.name.removesuffix(".fernet")
        restored.write_bytes(plaintext)
        connection = sqlite3.connect(f"file:{restored}?mode=ro", uri=True)
        try:
            integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
            if integrity != "ok":
                raise ValueError(f"restored SQLite integrity check failed: {integrity}")
            connection.execute("SELECT max(version) FROM schema_migrations").fetchone()
        finally:
            connection.close()
        return restored
