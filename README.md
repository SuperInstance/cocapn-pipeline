# cocapn-pipeline

Data pipeline framework for the Cocapn Fleet — tap sources, transform data chains, and sink to destinations with a fluent API.

## What This Gives You

- **Taps** (sources) — File, URL, PLATO, GitHub taps for pulling data
- **Transforms** — filter keys, rename keys, add timestamps, deduplicate
- **Sinks** (destinations) — File, PLATO, JSONL sinks for writing data
- **Pipeline** — chain taps → transforms → sinks in a single fluent call
- **PipelineResult** — structured result with row counts and timing

## Quick Start

```bash
pip install cocapn-pipeline

from cocapn_pipeline import Pipeline, FileTap, JSONLSink, filter_keys, deduplicate

result = (
    Pipeline()
    .tap(FileTap("input.jsonl"))
    .transform(filter_keys(["id", "name", "score"]))
    .transform(deduplicate())
    .sink(JSONLSink("output.jsonl"))
    .run()
)
print(f"Processed {result.rows} rows in {result.duration_ms:.0f}ms")
```

## How It Fits

The data processing backbone for the Cocapn Fleet. Part of the SuperInstance ecosystem.

Related repos:
- [cocapn-core](https://github.com/SuperInstance/cocapn-core) — core fleet library
- [cocapn-plato](https://github.com/SuperInstance/cocapn-plato) — PLATO framework
- [cocapn-curriculum](https://github.com/SuperInstance/cocapn-curriculum) — curriculum management

## License

Apache 2.0
