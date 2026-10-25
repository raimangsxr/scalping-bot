from __future__ import annotations

import os
import tempfile
from contextlib import suppress
from pathlib import Path
from typing import Protocol

import boto3
from botocore.exceptions import ClientError


class ImmutableObjectError(RuntimeError):
    pass


class ObjectStore(Protocol):
    def put_immutable(self, key: str, content: bytes) -> object: ...

    def read(self, key: str) -> bytes: ...

    def list_keys(self, prefix: str = "") -> tuple[str, ...]: ...


class LocalObjectStore:
    """Filesystem implementation of immutable object storage."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def path(self, key: str) -> Path:
        candidate = (self.root / key).resolve()
        root = self.root.resolve()
        if not candidate.is_relative_to(root):
            raise ValueError("object key escapes storage root")
        return candidate

    def put_immutable(self, key: str, content: bytes) -> Path:
        target = self.path(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        # Publish through a hard link only after the complete temporary file has
        # been flushed.  Unlike an exists-then-write sequence, link creation is
        # atomic and two processes cannot overwrite an immutable object or make
        # a reader observe a partially written object.
        file_descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{target.name}.", dir=target.parent
        )
        try:
            with os.fdopen(file_descriptor, "wb") as temporary:
                temporary.write(content)
                temporary.flush()
                os.fsync(temporary.fileno())
            try:
                os.link(temporary_name, target)
            except FileExistsError:
                if target.read_bytes() != content:
                    raise ImmutableObjectError(f"immutable object differs: {key}") from None
            return target
        finally:
            with suppress(FileNotFoundError):
                os.unlink(temporary_name)

    def read(self, key: str) -> bytes:
        return self.path(key).read_bytes()

    def list_keys(self, prefix: str = "") -> tuple[str, ...]:
        base = self.path(prefix) if prefix else self.root.resolve()
        if not base.exists():
            return ()
        if base.is_file():
            return (prefix,)
        return tuple(
            sorted(
                str(path.relative_to(self.root.resolve()))
                for path in base.rglob("*")
                if path.is_file()
            )
        )


class S3ObjectStore:
    """S3-compatible immutable storage using conditional object creation."""

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str = "",
        endpoint_url: str | None = None,
        region_name: str | None = None,
    ) -> None:
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.client = boto3.client("s3", endpoint_url=endpoint_url, region_name=region_name)

    def _key(self, key: str) -> str:
        return f"{self.prefix}/{key}" if self.prefix else key

    def put_immutable(self, key: str, content: bytes) -> None:
        object_key = self._key(key)
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=object_key,
                Body=content,
                IfNoneMatch="*",
                ChecksumSHA256=__import__("base64")
                .b64encode(__import__("hashlib").sha256(content).digest())
                .decode(),
            )
        except ClientError as error:
            status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status not in (409, 412) or self.read(key) != content:
                raise ImmutableObjectError(f"immutable S3 object differs: {object_key}") from error

    def read(self, key: str) -> bytes:
        response = self.client.get_object(Bucket=self.bucket, Key=self._key(key))
        return bytes(response["Body"].read())

    def list_keys(self, prefix: str = "") -> tuple[str, ...]:
        keys: list[str] = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self._key(prefix)):
            for item in page.get("Contents", []):
                if isinstance(item, dict) and isinstance(item.get("Key"), str):
                    key = str(item["Key"])
                    if self.prefix:
                        key = key.removeprefix(f"{self.prefix}/")
                    keys.append(key)
        return tuple(sorted(keys))


def object_store(
    *,
    local_root: Path,
    bucket: str | None = None,
    prefix: str = "",
    endpoint_url: str | None = None,
    region_name: str | None = None,
) -> ObjectStore:
    """Build the configured immutable store without ever handling credentials.

    A bucket selects the S3-compatible implementation; otherwise the local
    filesystem implementation is used.  Credentials remain in the standard
    boto3 environment/role chain and are deliberately not accepted as
    arguments, so they cannot leak into CLI output or manifests.
    """
    if bucket is None:
        if endpoint_url is not None or region_name is not None:
            raise ValueError("S3 endpoint and region require --bucket")
        return LocalObjectStore(local_root)
    if not bucket.strip():
        raise ValueError("S3 bucket cannot be empty")
    return S3ObjectStore(
        bucket=bucket.strip(),
        prefix=prefix,
        endpoint_url=endpoint_url,
        region_name=region_name,
    )
