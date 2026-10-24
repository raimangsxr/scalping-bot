from __future__ import annotations

import os
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
        encrypted = self.cipher.encrypt(plaintext)
        if artifact.exists():
            if artifact.read_bytes() != encrypted:
                raise ValueError("backup artifact identity already exists with different content")
            return artifact
        with tempfile.NamedTemporaryFile(
            dir=destination, prefix=f".{artifact.name}.", delete=False
        ) as temporary:
            temporary.write(encrypted)
            temporary.flush()
            os.fsync(temporary.fileno())
            temporary_name = Path(temporary.name)
        os.replace(temporary_name, artifact)
        return artifact

    def publish_latest(self, artifact: Path) -> Path:
        """Atomically publish the newest artifact without exposing a partial file."""
        if not artifact.is_file() or artifact.stat().st_size == 0:
            raise ValueError("cannot publish an empty encrypted backup")
        latest = artifact.parent / "latest.sqlite.fernet"
        with tempfile.NamedTemporaryFile(
            dir=artifact.parent, prefix=".latest.", delete=False
        ) as tmp:
            tmp.write(artifact.read_bytes())
            tmp.flush()
            os.fsync(tmp.fileno())
            temporary_name = Path(tmp.name)
        os.replace(temporary_name, latest)
        return latest

    def restore_and_verify(self, artifact: Path, *, destination: Path) -> Path:
        destination.mkdir(parents=True, exist_ok=True)
        try:
            plaintext = self.cipher.decrypt(artifact.read_bytes())
        except InvalidToken as error:
            raise ValueError("backup could not be decrypted") from error
        restored = destination / artifact.name.removesuffix(".fernet")
        if restored.exists() and restored.read_bytes() != plaintext:
            raise ValueError("isolated restore target already contains different SQLite data")
        restored.write_bytes(plaintext)
        connection = sqlite3.connect(f"file:{restored}?mode=ro", uri=True)
        try:
            integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
            if integrity != "ok":
                raise ValueError(f"restored SQLite integrity check failed: {integrity}")
            version = connection.execute("SELECT max(version) FROM schema_migrations").fetchone()[0]
            if version is None:
                raise ValueError("restored SQLite database has no schema migration history")
        finally:
            connection.close()
        return restored
