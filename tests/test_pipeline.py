"""Basic end-to-end tests for cocapn_pipeline."""

import json
import os
import tempfile
import threading
import http.server
import time

from cocapn_pipeline import (
    Pipeline,
    FileTap,
    FileSink,
    TransformChain,
    filter_keys,
    rename_keys,
    add_timestamp,
    deduplicate,
)


def test_file_to_file_pipeline():
    with tempfile.TemporaryDirectory() as tmpdir:
        source_path = os.path.join(tmpdir, "source.jsonl")
        sink_path = os.path.join(tmpdir, "output.jsonl")

        records = [
            {"id": "1", "name": "Alice", "extra": "x"},
            {"id": "2", "name": "Bob", "extra": "y"},
            {"id": "1", "name": "Alice", "extra": "z"},  # duplicate by id
        ]

        with open(source_path, "w") as fh:
            for r in records:
                fh.write(json.dumps(r) + "\n")

        transforms = TransformChain(
            filter_keys(["id", "name"]),
            rename_keys({"name": "user_name"}),
            deduplicate(key="id"),
        )

        pipeline = Pipeline(
            tap=FileTap(source_path),
            sink=FileSink(sink_path),
            transforms=transforms,
        )

        result = pipeline.run()

        assert result.count == 2
        assert result.errors == 0
        assert result.duration >= 0

        with open(sink_path) as fh:
            lines = [json.loads(line) for line in fh]

        assert lines == [
            {"id": "1", "user_name": "Alice"},
            {"id": "2", "user_name": "Bob"},
        ]


def test_add_timestamp_transform():
    with tempfile.TemporaryDirectory() as tmpdir:
        source_path = os.path.join(tmpdir, "source.json")
        sink_path = os.path.join(tmpdir, "output.jsonl")

        record = {"id": "1"}
        with open(source_path, "w") as fh:
            json.dump(record, fh)

        transforms = TransformChain(add_timestamp(key="ts"))
        pipeline = Pipeline(
            tap=FileTap(source_path, fmt="json"),
            sink=FileSink(sink_path),
            transforms=transforms,
        )

        result = pipeline.run()
        assert result.count == 1

        with open(sink_path) as fh:
            output = json.loads(fh.readline())

        assert "ts" in output
        assert isinstance(output["ts"], float)
        assert output["ts"] <= time.time()


def test_url_tap_and_jsonl_sink():
    """Spin up a local HTTP server and fetch JSON from it."""
    records = [{"a": 1}, {"a": 2}]
    payload = json.dumps(records).encode("utf-8")

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format, *args):
            pass

    with tempfile.TemporaryDirectory() as tmpdir:
        sink_path = os.path.join(tmpdir, "out.jsonl")
        server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()

        try:
            from cocapn_pipeline import UrlTap, JSONLSink

            pipeline = Pipeline(
                tap=UrlTap(f"http://127.0.0.1:{port}/data.json"),
                sink=JSONLSink(sink_path),
            )
            result = pipeline.run()
            assert result.count == 2
            assert result.errors == 0

            with open(sink_path) as fh:
                lines = [json.loads(line) for line in fh]
            assert lines == records
        finally:
            server.shutdown()
