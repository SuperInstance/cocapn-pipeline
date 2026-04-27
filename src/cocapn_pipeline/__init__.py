__version__ = "0.1.0"

from .pipeline import Pipeline, PipelineResult
from .tap import Tap, FileTap, UrlTap, PLATOTap, GitHubTap
from .sink import Sink, FileSink, PLATOSink, JSONLSink
from .transform import TransformChain, filter_keys, rename_keys, add_timestamp, deduplicate

__all__ = [
    "__version__",
    "Pipeline",
    "PipelineResult",
    "Tap",
    "FileTap",
    "UrlTap",
    "PLATOTap",
    "GitHubTap",
    "Sink",
    "FileSink",
    "PLATOSink",
    "JSONLSink",
    "TransformChain",
    "filter_keys",
    "rename_keys",
    "add_timestamp",
    "deduplicate",
]
