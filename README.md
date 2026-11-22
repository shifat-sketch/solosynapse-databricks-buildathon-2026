# Shopping Sessions — SoloSynapse

This is my submission for the Shopping Sessions challenge at BUILDERS Buildathon 2026.

I built the data pipeline using Python and DuckDB. It processes the event, click, product and campaign data, applies the challenge rules, and generates the required daily Parquet outputs.

## What the pipeline does

- Parses the input CSV files using the required CSV rules.
- Handles duplicate and invalid records.
- Resolves linked user identities.
- Groups events into sessions using the 30-minute inactivity rule.
- Calculates daily user activity and purchase revenue.
- Attributes sessions to eligible campaigns using click history.
- Calculates campaign-level sessions, purchases, revenue and assisted sessions.
- Writes the results to the required Parquet output folders.

## Tech stack

- Python 3.12
- DuckDB 1.5.6
- Docker

## Testing and validation

I tested the pipeline against all 36 provided edge-case fixtures.

**Fixture results: 36/36 passed.**

I also tested it using the 1-million-event development dataset with 250,000 clicks. I compared the generated outputs against the provided reference answers, and both tables matched.

- `user_daily`: 117,038 rows
- `campaign_daily`: 27,400 rows
- Reference comparison: MATCH

These are results from my local testing environment, not official leaderboard timings.

## Run with Docker

Build the image:

```bash
docker build --platform linux/amd64 -t solosynapse-shopping:latest .
```

Run the pipeline:

```bash
docker run --rm \
  -v ./dev:/data:ro \
  -v ./out:/output \
  -v ./scratch:/scratch \
  -e DATA_DIR=/data \
  -e OUTPUT_DIR=/output \
  -e SCRATCH_DIR=/scratch \
  solosynapse-shopping:latest
```

The pipeline writes:

```text
/output/
├── user_daily/
└── campaign_daily/
```

## Project structure

```text
starter-duckdb/
├── pipeline/
│   └── pipeline.py
├── scripts/
├── fixtures/
├── Dockerfile
├── requirements.txt
├── run.sh
└── README.md
```

This project is still open to improvement, especially in processing speed and efficiency. I plan to keep working on those areas.