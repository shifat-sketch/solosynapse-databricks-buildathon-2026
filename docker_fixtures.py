
import os
import sys
import tempfile
import subprocess
from pathlib import Path
import polars as pl

ROOT = Path(__file__).resolve().parent
FIXTURES = ROOT / "fixtures"
IMAGE = "solosynapse-shopping:latest"

SCHEMAS = {
    "user_daily": {
        "user_id": pl.String,
        "day": pl.Date,
        "sessions": pl.Int128,
        "events": pl.Int128,
        "revenue_cents": pl.Int128,
        "top_category": pl.String,
    },
    "campaign_daily": {
        "campaign_id": pl.String,
        "channel": pl.String,
        "day": pl.Date,
        "sessions": pl.Int128,
        "purchasing_sessions": pl.Int128,
        "revenue_cents": pl.Int128,
        "assisted_sessions": pl.Int128,
    },
}


def expected_rows(path, schema):
    df = pl.read_csv(
        path,
        schema_overrides={
            col: pl.String if typ == pl.Date else typ
            for col, typ in schema.items()
        },
    )
    if df.height == 0:
        return pl.DataFrame(schema=schema).rows()

    df = df.with_columns(
        pl.col("day").str.to_date()
    ).cast(schema)

    return sorted(
        df.rows(),
        key=lambda row: tuple(
            "" if value is None else str(value)
            for value in row
        ),
    )


def actual_rows(folder, schema):
    folder = Path(folder)
    files = sorted(folder.glob("*.parquet"))

    if not files:
        raise RuntimeError(f"No Parquet files found in {folder}")

    frames = []
    for file in files:
        df = pl.read_parquet(file)

        if set(df.columns) != set(schema):
            raise RuntimeError(
                f"{file}: wrong columns: {df.columns}"
            )

        df = df.select(list(schema)).cast(schema)
        frames.append(df)

    result = pl.concat(frames)

    return sorted(
        result.rows(),
        key=lambda row: tuple(
            "" if value is None else str(value)
            for value in row
        ),
    )


def run_case(case):
    data = case / "input"
    if not data.exists():
        data = case / "data"

    with tempfile.TemporaryDirectory(prefix="solo_test_") as temp:
        base = Path(temp)
        output = base / "output"
        scratch = base / "scratch"
        output.mkdir()
        scratch.mkdir()

        command = [
            "docker", "run", "--rm",
            "--platform", "linux/amd64",
            "--network", "none",
            "--read-only",
            "--tmpfs", "/tmp:rw,size=64m,mode=1777",
            "--cap-drop", "ALL",
            "--security-opt", "no-new-privileges",
            "--pids-limit", "4096",
            "--cpus", "8",
            "--memory", "24g",
            "--memory-swap", "24g",
            "--user", "1000:1000",
            "--mount",
            f"type=bind,source={data.resolve()},target=/data,readonly",
            "--mount",
            f"type=bind,source={output.resolve()},target=/output",
            "--mount",
            f"type=bind,source={scratch.resolve()},target=/scratch",
            "-e", "DATA_DIR=/data",
            "-e", "OUTPUT_DIR=/output",
            "-e", "SCRATCH_DIR=/scratch",
            "-e", "HOME=/scratch",
            "-e", "NCPU=8",
            "-e", "MEM_GB=24",
            IMAGE,
        ]

        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            raise RuntimeError(
                result.stderr[-3000:] or result.stdout[-3000:]
            )

        print("  Docker:", result.stdout.strip())

        for table, schema in SCHEMAS.items():
            expected_file = (
                case / "expected" / f"{table}.csv"
            )
            actual_folder = output / table

            expected = expected_rows(expected_file, schema)
            actual = actual_rows(actual_folder, schema)

            if expected != actual:
                print(f"  FAIL {table}")
                print("  Expected:", expected[:10])
                print("  Actual:  ", actual[:10])
                return False

            print(f"  PASS {table}")

        return True


def main():
    cases = sorted(
        path for path in FIXTURES.iterdir()
        if path.is_dir()
    )

    passed = 0
    failed = []

    print(f"Testing {len(cases)} fixtures with Docker")
    print("=" * 55)

    for case in cases:
        print(f"\n[{case.name}]")
        try:
            if run_case(case):
                passed += 1
                print(f"PASS {case.name}")
            else:
                failed.append(case.name)
                print(f"FAIL {case.name}")
        except Exception as error:
            failed.append(case.name)
            print(f"ERROR {case.name}: {error}")

    print("\n" + "=" * 55)
    print(f"FINAL RESULT: {passed}/{len(cases)} fixtures passed")

    if failed:
        print("Failed fixtures:", ", ".join(failed))
        sys.exit(1)

    print("ALL FIXTURES PASSED!")


if __name__ == "__main__":
    main()
