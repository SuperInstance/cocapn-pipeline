"""Pipeline orchestration: tap → transforms → sink."""

import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

from .tap import Tap
from .sink import Sink
from .transform import TransformChain


@dataclass
class PipelineResult:
    """Result of running a pipeline."""

    count: int
    errors: int
    duration: float


class Pipeline:
    """Pulls records from a tap, applies transforms, and writes to a sink."""

    def __init__(
        self,
        tap: Tap,
        sink: Sink,
        transforms: Optional[TransformChain] = None,
    ):
        self.tap = tap
        self.sink = sink
        self.transforms = transforms or TransformChain()

    def run(self) -> PipelineResult:
        count = 0
        errors = 0
        start = time.monotonic()
        for record in self.tap:
            try:
                transformed = self.transforms(record)
                if transformed is not None:
                    self.sink.write(transformed)
                    count += 1
            except Exception:
                errors += 1
        duration = time.monotonic() - start
        return PipelineResult(count=count, errors=errors, duration=duration)
