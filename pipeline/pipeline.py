
#!/usr/bin/env python3
"""Shopping Sessions pipeline using strict CSV parsing and DuckDB."""
import datetime as dt
import glob
import json
import os
import re
from collections import Counter, defaultdict, deque

import duckdb

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")

NCPU = int(os.environ.get("NCPU", os.cpu_count() or 1))
MEM_GB = int(os.environ.get("MEM_GB", "8"))

UTC = dt.timezone.utc
EPOCH = dt.datetime(1970, 1, 1, tzinfo=UTC)


ISO_UTC = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$"
)

ISO_OFFSET = re.compile(
    r"^([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})([+-])([0-9]{2}):([0-9]{2})$"
)
DIGITS = re.compile(r"^[0-9]+$")


def parse_ts(value):
    """Parse supported timestamp formats into epoch milliseconds."""
    if value is None:
        return None

    try:
        if DIGITS.fullmatch(value):
            return int(value)

        if ISO_UTC.fullmatch(value):
            value_dt = dt.datetime.strptime(
                value, "%Y-%m-%dT%H:%M:%SZ"
            ).replace(tzinfo=UTC)

      
        elif ISO_OFFSET.fullmatch(value):
            match = ISO_OFFSET.fullmatch(value)

            hour = int(match.group(3))
            minute = int(match.group(4))

            if hour > 23 or minute > 59:
                return None

            value_dt = dt.datetime.fromisoformat(value)

            if value_dt.utcoffset() is None:
                return None

            value_dt = value_dt.astimezone(UTC)

        else:
            return None

        return int((value_dt - EPOCH).total_seconds() * 1000)

    except (ValueError, OverflowError):
        return None


def csv_fields(line, expected):
    """Parse the challenge's restricted physical-line CSV dialect."""
    if line == "":
        return None

    result = []
    i = 0
    n = len(line)

    while True:
        if i < n and line[i] == '"':
            i += 1
            start = i

            while i < n and line[i] != '"':
                if line[i] == ",":
                    return None
                i += 1

            if i >= n:
                return None

            value = line[start:i]
            i += 1

            if i < n and line[i] != ",":
                return None

            result.append(value if value != "" else None)

        else:
            start = i

            while i < n and line[i] != ",":
                if line[i] == '"':
                    return None
                i += 1

            value = line[start:i]
            result.append(value if value != "" else None)

        if i >= n:
            break

        i += 1

        if i == n:
            result.append(None)
            break

    if len(result) != expected:
        return None

    return result


def read_csv_files(pattern, columns):
    """Read matching CSV files using the strict parser."""
    rows = []

    for path in sorted(glob.glob(pattern)):
        try:
            with open(path, "r", encoding="utf-8", newline="\n") as f:
                header = f.readline()

                if header.rstrip("\n") != ",".join(columns):
                    continue

                for raw in f:
                    if raw.endswith("\n"):
                        raw = raw[:-1]

                    values = csv_fields(raw, len(columns))

                    if values is not None:
                        rows.append(dict(zip(columns, values)))

        except (UnicodeDecodeError, OSError):
            continue

    return rows


def bytekey(value):
    return value.encode("utf-8")


def main():

    # --------------------------------------------------
    # 1. Read manifest and event window
    # --------------------------------------------------

    with open(
        os.path.join(DATA, "manifest.json"),
        encoding="utf-8"
    ) as f:
        manifest = json.load(f)

    win_start = parse_ts(manifest["window_start"])
    win_end = parse_ts(manifest["window_end"])

    if (
        win_start is None
        or win_end is None
        or win_start >= win_end
    ):
        raise ValueError("Invalid manifest window")

    print(
        "window:",
        manifest["window_start"],
        "to",
        manifest["window_end"]
    )

    # --------------------------------------------------
    # 2. Read products
    # --------------------------------------------------

    products = {}

    product_rows = read_csv_files(
        os.path.join(DATA, "products.csv"),
        ["product_id", "category"]
    )

    for row in product_rows:
        pid = row["product_id"]

        if pid not in (None, ""):
            category = row["category"]

            products[pid] = (
                category
                if category not in (None, "")
                else "unknown"
            )

    # --------------------------------------------------
    # 3. Read campaigns
    # --------------------------------------------------

    campaigns = {}

    campaign_rows_input = read_csv_files(
        os.path.join(DATA, "campaigns.csv"),
        ["campaign_id", "channel"]
    )

    for row in campaign_rows_input:
        cid = row["campaign_id"]
        channel = row["channel"]

        if cid not in (None, "") and channel not in (None, ""):
            campaigns[cid] = channel

    # --------------------------------------------------
    # 4. Resolve linked user identities
    # --------------------------------------------------

    parent = {}

    def find(x):
        parent.setdefault(x, x)

        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]

        return x

    def union(a, b):
        ra = find(a)
        rb = find(b)

        if ra == rb:
            return

        if bytekey(ra) <= bytekey(rb):
            parent[rb] = ra
        else:
            parent[ra] = rb

    link_rows = read_csv_files(
        os.path.join(DATA, "links", "*.csv"),
        ["user_id_a", "user_id_b"]
    )

    for row in link_rows:
        a = row["user_id_a"]
        b = row["user_id_b"]

        if a not in (None, "") and b not in (None, ""):
            union(a, b)

    def person(uid):
        if uid in parent:
            return find(uid)
        return uid

    # --------------------------------------------------
    # 5. Read and deduplicate events
    # --------------------------------------------------

    raw_events = read_csv_files(
        os.path.join(DATA, "events", "*.csv"),
        [
            "event_id",
            "user_id",
            "ts",
            "ingest_ts",
            "event_type",
            "product_id",
            "price_cents"
        ]
    )

    dedup_events = {}

    for row in raw_events:
        eid = row["event_id"]

        if eid in (None, ""):
            continue

        ingest = parse_ts(row["ingest_ts"])

        rank = (
            ingest
            if ingest is not None
            else 2**63 - 1
        )

        old = dedup_events.get(eid)

        if old is None or rank < old[0]:
            dedup_events[eid] = (rank, row)

    # --------------------------------------------------
    # 6. Validate events
    # --------------------------------------------------

    events = []

    for _, row in dedup_events.values():

        uid = row["user_id"]
        ts = parse_ts(row["ts"])
        ingest = parse_ts(row["ingest_ts"])

        event_type = row["event_type"]
        product_id = row["product_id"]

        if uid in (None, ""):
            continue

        if ts is None:
            continue

        if not (win_start <= ts < win_end):
            continue

        if event_type not in ("view", "cart", "purchase"):
            continue

        if product_id in (None, ""):
            continue

        if ingest is None:
            continue

        if ingest - ts > 86_400_000:
            continue

        price = 0

        if event_type == "purchase":

            price_value = row["price_cents"]

            if (
                price_value is None
                or not DIGITS.fullmatch(price_value)
            ):
                continue

            price = int(price_value)

            if not (1 <= price <= 10_000_000):
                continue

        events.append({
            "user": person(uid),
            "ts": ts,
            "type": event_type,
            "product": product_id,
            "price": price,
            "category": products.get(product_id, "unknown")
        })

    # --------------------------------------------------
    # 7. Sessionization
    # --------------------------------------------------

    events.sort(
        key=lambda e: (
            bytekey(e["user"]),
            e["ts"]
        )
    )

    sessions = []
    current = None
    previous_ts = None

    for event in events:

        new_session = (
            current is None
            or event["user"] != current["user"]
            or event["ts"] - previous_ts > 1_800_000
        )

        if new_session:

            current = {
                "user": event["user"],
                "start": event["ts"],
                "events": [],
                "revenue": 0,
                "purchase": False,
                "categories": Counter()
            }

            sessions.append(current)

        current["events"].append(event)

        if event["type"] == "purchase":

            current["purchase"] = True
            current["revenue"] += event["price"]

            current["categories"][event["category"]] += (
                event["price"]
            )

        previous_ts = event["ts"]

    # --------------------------------------------------
    # 8. Read and deduplicate clicks
    # --------------------------------------------------

    raw_clicks = read_csv_files(
        os.path.join(DATA, "clicks", "*.csv"),
        [
            "click_id",
            "user_id",
            "campaign_id",
            "ts"
        ]
    )

    dedup_clicks = {}

    for row in raw_clicks:

        click_id = row["click_id"]

        if click_id in (None, ""):
            continue

        if click_id not in dedup_clicks:
            dedup_clicks[click_id] = row

    # --------------------------------------------------
    # 9. Validate clicks and group by user
    # --------------------------------------------------

    clicks_by_user = defaultdict(list)

    for row in dedup_clicks.values():

        uid = row["user_id"]
        cid = row["campaign_id"]
        ts = parse_ts(row["ts"])

        if uid in (None, ""):
            continue

        if cid not in campaigns:
            continue

        if ts is None:
            continue

        if not (
            win_start - 7 * 86_400_000
            <= ts
            < win_end
        ):
            continue

        clicks_by_user[person(uid)].append(
            (ts, cid, row["click_id"])
        )

    for uid in clicks_by_user:
        clicks_by_user[uid].sort(
            key=lambda x: (
                x[0],
                bytekey(x[2])
            )
        )

    # --------------------------------------------------
    # 10. Prepare aggregate structures
    # --------------------------------------------------

    user_agg = defaultdict(
        lambda: {
            "sessions": 0,
            "events": 0,
            "revenue": 0,
            "categories": Counter()
        }
    )

    campaign_agg = defaultdict(
        lambda: {
            "sessions": 0,
            "purchasing": 0,
            "revenue": 0,
            "assisted": 0
        }
    )

    
    # --------------------------------------------------
    # 11. Attribute sessions to campaigns
    # Optimized sliding-window attribution
    # --------------------------------------------------

    active_clicks = deque()
    active_campaign_counts = defaultdict(int)

    active_click_user = None
    user_clicks = ()
    click_index = 0

    for session in sessions:

        uid = session["user"]
        start = session["start"]

        # Sessions are already sorted by user and start time.
        if uid != active_click_user:
            active_clicks.clear()
            active_campaign_counts.clear()

            user_clicks = clicks_by_user.get(uid, ())
            click_index = 0
            active_click_user = uid

        # Add clicks up to the session start.
        while (
            click_index < len(user_clicks)
            and user_clicks[click_index][0] <= start
        ):
            click = user_clicks[click_index]
            active_clicks.append(click)
            active_campaign_counts[click[1]] += 1
            click_index += 1

        # Remove clicks older than the seven-day window.
        cutoff = start - 7 * 86_400_000

        while active_clicks and active_clicks[0][0] < cutoff:
            expired = active_clicks.popleft()
            cid = expired[1]

            active_campaign_counts[cid] -= 1

            if active_campaign_counts[cid] == 0:
                del active_campaign_counts[cid]

        # User daily aggregation.
        day = (
            EPOCH
            + dt.timedelta(milliseconds=start)
        ).date()

        user_key = (uid, day)
        ua = user_agg[user_key]

        ua["sessions"] += 1
        ua["events"] += len(session["events"])
        ua["revenue"] += session["revenue"]
        ua["categories"].update(session["categories"])

        # One assist per campaign per session.
        for cid in active_campaign_counts:
            channel = campaigns[cid]

            campaign_agg[
                (cid, channel, day)
            ]["assisted"] += 1

        # Latest eligible click receives session credit.
        if active_clicks:

            latest_ts = active_clicks[-1][0]
            chosen = active_clicks[-1]

            # Clicks are sorted by timestamp and bytewise click_id.
            # Find the bytewise smallest ID at the latest timestamp.
            for click in reversed(active_clicks):

                if click[0] != latest_ts:
                    break

                chosen = click

            campaign_id = chosen[1]
            channel = campaigns[campaign_id]

        else:
            campaign_id = "organic"
            channel = "organic"

        ca = campaign_agg[
            (campaign_id, channel, day)
        ]

        ca["sessions"] += 1

        if session["purchase"]:
            ca["purchasing"] += 1
            ca["revenue"] += session["revenue"]

    # --------------------------------------------------
    # 12. Build user_daily rows
    # --------------------------------------------------

    user_output = []

    for (uid, day), aggregate in user_agg.items():

        top_category = None

        if aggregate["categories"]:

            top_category = min(
                aggregate["categories"],
                key=lambda category: (
                    -aggregate["categories"][category],
                    bytekey(category)
                )
            )

        user_output.append((
            uid,
            day,
            aggregate["sessions"],
            aggregate["events"],
            aggregate["revenue"],
            top_category
        ))

    user_output.sort(
        key=lambda row: (
            bytekey(row[0]),
            row[1]
        )
    )

    # --------------------------------------------------
    # 13. Build campaign_daily rows
    # --------------------------------------------------

    campaign_output = []

    for (cid, channel, day), aggregate in campaign_agg.items():

        campaign_output.append((
            cid,
            channel,
            day,
            aggregate["sessions"],
            aggregate["purchasing"],
            aggregate["revenue"],
            aggregate["assisted"]
        ))

    campaign_output.sort(
        key=lambda row: (
            bytekey(row[0]),
            bytekey(row[1]),
            row[2]
        )
    )

    # --------------------------------------------------
    # 14. Write Parquet output using DuckDB
    # --------------------------------------------------

    os.makedirs(
        os.path.join(OUT, "user_daily"),
        exist_ok=True
    )

    os.makedirs(
        os.path.join(OUT, "campaign_daily"),
        exist_ok=True
    )

    os.makedirs(
        os.path.join(SCRATCH, "duckdb_tmp"),
        exist_ok=True
    )

    con = duckdb.connect()

    con.execute(
        f"SET threads={max(1, NCPU)}"
    )

    con.execute(
        f"SET memory_limit='{max(1, int(MEM_GB * 0.8))}GB'"
    )

    con.execute(
        "SET preserve_insertion_order=false"
    )

    con.execute("""
        CREATE TABLE user_daily (
            user_id VARCHAR,
            day DATE,
            sessions BIGINT,
            events BIGINT,
            revenue_cents BIGINT,
            top_category VARCHAR
        )
    """)

    con.execute("""
        CREATE TABLE campaign_daily (
            campaign_id VARCHAR,
            channel VARCHAR,
            day DATE,
            sessions BIGINT,
            purchasing_sessions BIGINT,
            revenue_cents BIGINT,
            assisted_sessions BIGINT
        )
    """)

    if user_output:
        con.executemany(
            "INSERT INTO user_daily VALUES (?, ?, ?, ?, ?, ?)",
            user_output
        )

    if campaign_output:
        con.executemany(
            "INSERT INTO campaign_daily VALUES (?, ?, ?, ?, ?, ?, ?)",
            campaign_output
        )

    for folder in ("user_daily", "campaign_daily"):

        folder_path = os.path.join(OUT, folder)

        for filename in os.listdir(folder_path):

            if filename.endswith(".parquet"):
                os.remove(
                    os.path.join(folder_path, filename)
                )

    con.execute(
        f"""
        COPY user_daily
        TO '{os.path.join(OUT, 'user_daily', 'part-0.parquet')}'
        (FORMAT PARQUET)
        """
    )

    con.execute(
        f"""
        COPY campaign_daily
        TO '{os.path.join(OUT, 'campaign_daily', 'part-0.parquet')}'
        (FORMAT PARQUET)
        """
    )

    print(
        f"wrote {len(user_output)} user_daily rows "
        f"and {len(campaign_output)} campaign_daily rows"
    )


if __name__ == "__main__":
    main()
