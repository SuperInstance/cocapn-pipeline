"""Destination sinks that accept dictionaries."""

import abc
import json
import urllib.request
from typing import Dict, Any, Optional


class Sink(abc.ABC):
    """Abstract base class for data destinations."""

    @abc.abstractmethod
    def write(self, record: Dict[str, Any]) -> None:
        """Write a single record."""
        ...

    def close(self) -> None:
        """Optional cleanup."""
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class FileSink(Sink):
    """Write records as JSON Lines to a file."""

    def __init__(self, path: str, mode: str = "w"):
        self.path = path
        self.mode = mode
        self._fh = open(path, mode, encoding="utf-8")

    def write(self, record: Dict[str, Any]) -> None:
        self._fh.write(json.dumps(record, ensure_ascii=False) + "\n")

    def close(self) -> None:
        self._fh.close()


class JSONLSink(Sink):
    """Alias for JSON Lines output (same as FileSink but explicit)."""

    def __init__(self, path: str, mode: str = "w"):
        self.path = path
        self.mode = mode
        self._fh = open(path, mode, encoding="utf-8")

    def write(self, record: Dict[str, Any]) -> None:
        self._fh.write(json.dumps(record, ensure_ascii=False) + "\n")

    def close(self) -> None:
        self._fh.close()


class PLATOSink(Sink):
    """Post records to a PLATO room on localhost:8847."""

    def __init__(self, room: str, base_url: str = "http://localhost:8847"):
        self.room = room
        self.base_url = base_url
        self.url = f"{base_url}/rooms/{room}/records"

    def write(self, record: Dict[str, Any]) -> None:
        payload = json.dumps(record).encode("utf-8")
        request = urllib.request.Request(
            self.url,
            data=payload,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=30):
            pass
