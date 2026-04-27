"""Source taps that yield dictionaries."""

import abc
import csv
import json
import urllib.request
from typing import Iterator, Dict, Any, Optional, Iterable


class Tap(abc.ABC):
    """Abstract base class for data sources."""

    @abc.abstractmethod
    def __iter__(self) -> Iterator[Dict[str, Any]]:
        """Yield dictionaries."""
        ...


class FileTap(Tap):
    """Read records from a file (JSON, JSONL, or CSV)."""

    def __init__(self, path: str, fmt: Optional[str] = None):
        self.path = path
        self.fmt = fmt or self._guess_format(path)

    @staticmethod
    def _guess_format(path: str) -> str:
        if path.endswith(".jsonl"):
            return "jsonl"
        if path.endswith(".json"):
            return "json"
        if path.endswith(".csv"):
            return "csv"
        return "jsonl"

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        with open(self.path, "r", encoding="utf-8") as fh:
            if self.fmt == "jsonl":
                for line in fh:
                    line = line.strip()
                    if line:
                        yield json.loads(line)
            elif self.fmt == "json":
                data = json.load(fh)
                if isinstance(data, list):
                    yield from data
                else:
                    yield data
            elif self.fmt == "csv":
                reader = csv.DictReader(fh)
                yield from reader
            else:
                raise ValueError(f"Unsupported format: {self.fmt}")


class UrlTap(Tap):
    """Fetch JSON data from a URL using urllib."""

    def __init__(self, url: str, headers: Optional[Dict[str, str]] = None):
        self.url = url
        self.headers = headers or {}

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        request = urllib.request.Request(self.url, headers=self.headers)
        with urllib.request.urlopen(request, timeout=30) as response:
            data = json.loads(response.read().decode("utf-8"))
        if isinstance(data, list):
            yield from data
        else:
            yield data


class PLATOTap(Tap):
    """Tap that fetches records from a PLATO room on localhost:8847."""

    def __init__(self, room: str, base_url: str = "http://localhost:8847"):
        self.room = room
        self.base_url = base_url

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        url = f"{self.base_url}/rooms/{self.room}/records"
        request = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(request, timeout=30) as response:
            data = json.loads(response.read().decode("utf-8"))
        if isinstance(data, list):
            yield from data
        else:
            yield data


class GitHubTap(Tap):
    """Tap that fetches issues from a GitHub repository via the REST API."""

    def __init__(
        self,
        owner: str,
        repo: str,
        token: Optional[str] = None,
        per_page: int = 30,
    ):
        self.owner = owner
        self.repo = repo
        self.token = token
        self.per_page = per_page

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        page = 1
        headers = {"Accept": "application/vnd.github+json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        while True:
            url = (
                f"https://api.github.com/repos/{self.owner}/{self.repo}/issues"
                f"?state=all&per_page={self.per_page}&page={page}"
            )
            request = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(request, timeout=30) as response:
                data = json.loads(response.read().decode("utf-8"))
            if not isinstance(data, list) or not data:
                break
            yield from data
            if len(data) < self.per_page:
                break
            page += 1
