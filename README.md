# Shopping Sessions

Our entry for the Shopping Sessions challenge: clean messy shop logs, link user
IDs into people, build sessions, credit ad clicks, and write two daily Parquet
tables. The output must match the official answer exactly; after that, the
score is container run time.

- The challenge and its exact rules: [docs/challenge.md](docs/challenge.md), [docs/rules.md](docs/rules.md)
- Our approach and phases: [docs/PLAN.md](docs/PLAN.md)

## Layout

| Path | What it is |
|---|---|
| `Dockerfile` | What the scorer builds and runs: a static binary in an empty image |
| `pipeline/` | The Rust pipeline (the thing being scored) |
| `reference/a/`, `reference/b/` | Two slow Python implementations written independently from the rules, used only to check the pipeline |
| `tests/fixtures/` | The 36 hand-written edge cases from the organisers |
| `tests/dev-answer/` | The published answer for the 1M-event dev set (seed 1) |
| `tools/generator/` | The organisers' dataset generator |
| `scripts/` | Test harness: fixture runner, output comparer, seed sweep, scorer-like `docker run` |
| `data/`, `out/` | Generated datasets and outputs; never committed |

## Commands

Needs Rust, `uv`, and Docker (only for the image targets).

```sh
make venv build     # once
make test           # unit tests and the 36 fixtures
make dev check-dev  # generate the dev set, run, compare with the published answer
make sweep          # pipeline vs both references on 20 seeds
make image-dev      # the same dev check through the real image and container limits
```
