#!/usr/bin/env python3
"""
Differential test: for each seed, generate a dataset, run the fast pipeline and
both references on it, and check that all three outputs are identical.

Usage: sweep.py [--seeds 1-20] [--events 200000] [--clicks 50000] [--jobs 6]
                [--no-refs]   (only compare the pipeline with reference A)
Exit code 0 only if every seed agrees. Datasets and outputs live under
data/sweep/ and are deleted as each seed finishes, unless it fails.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import shutil
import subprocess
import sys

from run_fixtures import SCHEMAS, load_actual, rows

ROOT = Path(__file__).resolve().parent.parent
GENERATE = ROOT / "tools/generator/target/release/generate"
PIPELINE = ROOT / "pipeline/target/release/pipeline"
REFERENCES = {"ref-a": ROOT / "reference/a/reference.py", "ref-b": ROOT / "reference/b/reference.py"}


def parse_seeds(text):
    seeds = []
    for part in text.split(","):
        lo, _, hi = part.partition("-")
        seeds.extend(range(int(lo), int(hi or lo) + 1))
    return seeds


def run(cmd):
    p = subprocess.run([str(c) for c in cmd], capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"{' '.join(map(str, cmd))}: exit {p.returncode}\n{p.stderr[-2000:]}")


def one_seed(seed, args):
    work = ROOT / "data/sweep" / f"seed-{seed}"
    shutil.rmtree(work, ignore_errors=True)
    data = work / "data"
    try:
        run([GENERATE, "--seed", seed, "--events", args.events, "--clicks", args.clicks, "--output", data])
        implementations = {"pipeline": [PIPELINE]}
        names = ["ref-a"] if args.no_refs else list(REFERENCES)
        for name in names:
            implementations[name] = [sys.executable, REFERENCES[name]]
        tables = {}
        for name, cmd in implementations.items():
            out = work / name
            run(cmd + [data, out])
            tables[name] = {t: rows(load_actual(out / t, schema)) for t, schema in SCHEMAS.items()}
        problems = []
        base = tables["pipeline"]
        for name in implementations:
            for table in SCHEMAS:
                if tables[name][table] != base[table]:
                    a, b = set(base[table]), set(tables[name][table])
                    problems.append(f"{table}: pipeline vs {name}: {len(a - b)} rows only in pipeline, "
                                    f"{len(b - a)} only in {name}; e.g. {sorted(a ^ b, key=str)[:4]}")
        if "ref-a" in tables and "ref-b" in tables:
            for table in SCHEMAS:
                if tables["ref-a"][table] != tables["ref-b"][table]:
                    problems.append(f"{table}: ref-a and ref-b disagree with each other")
        if problems:
            return seed, False, "\n    ".join(problems) + f"\n    kept: {work}"
        counts = ", ".join(f"{t} {len(base[t])}" for t in SCHEMAS)
        shutil.rmtree(work, ignore_errors=True)
        return seed, True, counts
    except Exception as error:  # a crash is a failure of that seed, not of the sweep
        return seed, False, f"{error}\n    kept: {work}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--seeds", default="1-20")
    parser.add_argument("--events", type=int, default=200_000)
    parser.add_argument("--clicks", type=int, default=50_000)
    parser.add_argument("--jobs", type=int, default=6)
    parser.add_argument("--no-refs", action="store_true")
    args = parser.parse_args()
    for needed in (GENERATE, PIPELINE):
        if not needed.exists():
            sys.exit(f"missing {needed}: run `make build` first")
    seeds = parse_seeds(args.seeds)
    failed = []
    with ThreadPoolExecutor(args.jobs) as pool:
        for seed, ok, detail in pool.map(lambda s: one_seed(s, args), seeds):
            print(f"{'ok  ' if ok else 'FAIL'} seed {seed}: {detail}", flush=True)
            if not ok:
                failed.append(seed)
    print(f"\n{len(seeds) - len(failed)}/{len(seeds)} seeds agree" + (f"; failed: {failed}" if failed else ""))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
