"""Transform utilities for dictionary records."""

import time
from typing import Callable, Dict, Any, List, Optional


Transform = Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]


class TransformChain:
    """Chain multiple transforms together."""

    def __init__(self, *transforms: Transform):
        self.transforms = list(transforms)

    def add(self, transform: Transform) -> "TransformChain":
        self.transforms.append(transform)
        return self

    def __call__(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        for t in self.transforms:
            if record is None:
                break
            record = t(record)
        return record


def filter_keys(keys: List[str]) -> Transform:
    """Return a transform that keeps only the given keys."""
    key_set = set(keys)

    def _transform(record: Dict[str, Any]) -> Dict[str, Any]:
        return {k: v for k, v in record.items() if k in key_set}

    return _transform


def rename_keys(mapping: Dict[str, str]) -> Transform:
    """Return a transform that renames keys according to *mapping*."""

    def _transform(record: Dict[str, Any]) -> Dict[str, Any]:
        out = {}
        for k, v in record.items():
            out[mapping.get(k, k)] = v
        return out

    return _transform


def add_timestamp(key: str = "_timestamp") -> Transform:
    """Return a transform that adds the current Unix timestamp."""

    def _transform(record: Dict[str, Any]) -> Dict[str, Any]:
        record = dict(record)
        record[key] = time.time()
        return record

    return _transform


def deduplicate(key: Optional[str] = None) -> Transform:
    """Return a transform that drops records already seen.

    If *key* is provided, deduplicate by that field value;
    otherwise the entire record is hashed.
    """
    seen: set = set()

    def _transform(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if key is not None:
            identifier = record.get(key)
        else:
            identifier = tuple(sorted(record.items(), key=lambda x: x[0]))
        if identifier in seen:
            return None
        seen.add(identifier)
        return record

    return _transform
