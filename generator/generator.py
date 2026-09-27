"""
Simulate downloads of Apache big data tools on the source Postgres table.

The first run seeds the table with finished downloads from the last seven days. After
that, the generator keeps producing the lifecycle of new downloads, which is what the
CDC pipeline picks up:

- start:  a user starts a download (INSERT, status ``started``).
- finish: a started download completes or fails (UPDATE).
- purge:  a failed download is removed (DELETE).
"""

import os
import random
import signal
import sys
import time
import uuid
from collections import Counter
from collections.abc import Callable, Mapping
from datetime import datetime, timedelta
from typing import Any, NamedTuple

import psycopg
from psycopg.rows import TupleRow

DSN = os.environ["PG_DSN"]
OPS_PER_SEC = float(os.environ.get("OPS_PER_SEC", "5"))
INITIAL_ROWS = int(os.environ.get("INITIAL_ROWS", "200"))
NUM_USERS = int(os.environ.get("NUM_USERS", "500"))
W_START = float(os.environ.get("W_START", "0.45"))
W_FINISH = float(os.environ.get("W_FINISH", "0.45"))
W_PURGE = float(os.environ.get("W_PURGE", "0.1"))

COMPLETION_RATE = 0.85
REPORT_INTERVAL_S = 30

type Cursor = psycopg.Cursor[TupleRow]


class Tool(NamedTuple):
    """
    An Apache tool that users can download.

    Attributes:
        weight (int): Relative popularity, used to pick which tool is downloaded.
        versions (tuple[str, ...]): Versions available for download.
        size_mb (int): Approximate size of the artifact, in MB.
    """

    weight: int
    versions: tuple[str, ...]
    size_mb: int


class User(NamedTuple):
    """
    A user who downloads tools. Each user always downloads from the same place.

    Attributes:
        user_id (str): UUID of the user.
        ip (str): IPv4 address the user downloads from.
        country (str): ISO 3166-1 alpha-2 country code, e.g. ``ES``.
    """

    user_id: str
    ip: str
    country: str


TOOLS = {
    "Spark": Tool(20, ("3.5.3", "3.5.4", "4.0.0"), 400),
    "Kafka": Tool(18, ("3.8.1", "3.9.0", "4.0.0"), 120),
    "Flink": Tool(12, ("1.19.2", "1.20.1", "2.0.0"), 480),
    "Airflow": Tool(10, ("2.10.4", "3.0.1"), 25),
    "Hadoop": Tool(8, ("3.3.6", "3.4.1"), 700),
    "Iceberg": Tool(8, ("1.8.1", "1.9.0", "1.10.1"), 30),
    "Hive": Tool(5, ("3.1.3", "4.0.1"), 330),
    "Cassandra": Tool(5, ("4.1.7", "5.0.3"), 60),
    "Druid": Tool(4, ("31.0.1", "32.0.0"), 900),
    "Pinot": Tool(3, ("1.2.0", "1.3.0"), 850),
    "Paimon": Tool(3, ("1.0.1", "1.1.0"), 40),
    "Fluss": Tool(2, ("0.9.1", "1.0.0"), 350),
}
COUNTRIES = {
    "US": 25,
    "CN": 12,
    "IN": 12,
    "DE": 8,
    "GB": 6,
    "FR": 5,
    "BR": 5,
    "ES": 4,
    "JP": 4,
    "CA": 4,
    "KR": 3,
    "NL": 3,
    "IT": 3,
    "MX": 3,
    "AU": 3,
}
OPERATING_SYSTEMS = {"linux": 60, "macos": 25, "windows": 15}
CHANNELS = {"website": 45, "maven": 25, "docker": 20, "pypi": 10}

INSERT_SQL = """
    INSERT INTO downloads (
        user_id, ip, country, tool, version, os, channel,
        status, size_bytes, duration_ms, created_at, updated_at
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING download_id
"""
FINISH_SQL = """
    UPDATE downloads
    SET status = %s, duration_ms = %s, updated_at = now()
    WHERE download_id = (
        SELECT download_id FROM downloads
        WHERE status = 'started'
        ORDER BY random()
        LIMIT 1
    )
    RETURNING download_id
"""
PURGE_SQL = """
    DELETE FROM downloads
    WHERE download_id = (
        SELECT download_id FROM downloads
        WHERE status = 'failed'
        ORDER BY random()
        LIMIT 1
    )
    RETURNING download_id
"""
COMPLETED_BY_TOOL_SQL = """
    SELECT tool, count(*) AS downloads, max(created_at) AS last_download
    FROM downloads
    WHERE status = 'completed'
    GROUP BY tool
    ORDER BY downloads DESC
"""


def weighted_choice[T](weights: Mapping[T, float]) -> T:
    """
    Pick a key of a mapping with probability proportional to its value.

    Args:
        weights (Mapping[T, float]): Candidates and their relative weights.

    Returns:
        T: The chosen key.
    """
    return random.choices(list(weights), weights=list(weights.values()))[0]


def random_ip() -> str:
    """
    Generate a random IPv4 address.

    Returns:
        str: An address like ``83.127.4.201``.
    """
    return ".".join(str(random.randint(1, 254)) for _ in range(4))


def random_duration_ms() -> int:
    """
    Generate how long a finished download took.

    Returns:
        int: A duration between 2 seconds and 2 minutes, in milliseconds.
    """
    return random.randint(2_000, 120_000)


def random_final_status() -> str:
    """
    Decide how a download ends.

    Returns:
        str: ``completed`` with probability ``COMPLETION_RATE``, ``failed`` otherwise.
    """
    return "completed" if random.random() < COMPLETION_RATE else "failed"


USERS = [
    User(str(uuid.uuid4()), random_ip(), weighted_choice(COUNTRIES))
    for _ in range(NUM_USERS)
]
TOOL_WEIGHTS = {name: tool.weight for name, tool in TOOLS.items()}


def connect() -> psycopg.Connection[TupleRow]:
    """
    Connect to Postgres, retrying until the server accepts connections.

    Returns:
        psycopg.Connection[TupleRow]: A connection in autocommit mode, so every
        statement is its own transaction and shows up as a separate change in the WAL.
    """
    while True:
        try:
            return psycopg.connect(DSN, autocommit=True)
        except psycopg.OperationalError as e:
            print(f"waiting for postgres: {e}", flush=True)
            time.sleep(2)


def fetch_one(cur: Cursor) -> tuple[Any, ...]:
    """
    Return the only row of the last executed query.

    Args:
        cur (Cursor): Cursor that has just executed a query returning one row.

    Returns:
        tuple[Any, ...]: The row.

    Raises:
        RuntimeError: If the query returned no rows.
    """
    row = cur.fetchone()
    if row is None:
        raise RuntimeError("expected one row, got none")
    return row


def insert_download(
    cur: Cursor, status: str, created_at: datetime, duration_ms: int | None
) -> uuid.UUID:
    """
    Insert a download of a random tool by a random user.

    Args:
        cur (Cursor): Cursor to run the insert with.
        status (str): Status of the download: ``started``, ``completed`` or ``failed``.
        created_at (datetime): When the download started.
        duration_ms (int | None): How long it took, or None if it is still running.

    Returns:
        uuid.UUID: Id of the new download.
    """
    user = random.choice(USERS)
    name = weighted_choice(TOOL_WEIGHTS)
    tool = TOOLS[name]
    size_bytes = int(tool.size_mb * random.uniform(0.9, 1.1) * 1024 * 1024)
    updated_at = created_at + timedelta(milliseconds=duration_ms or 0)

    cur.execute(
        INSERT_SQL,
        (
            user.user_id,
            user.ip,
            user.country,
            name,
            random.choice(tool.versions),
            weighted_choice(OPERATING_SYSTEMS),
            weighted_choice(CHANNELS),
            status,
            size_bytes,
            duration_ms,
            created_at,
            updated_at,
        ),
    )
    download_id: uuid.UUID = fetch_one(cur)[0]
    return download_id


def seed(cur: Cursor) -> uuid.UUID:
    """
    Insert a finished download that happened during the last seven days.

    Args:
        cur (Cursor): Cursor to run the insert with.

    Returns:
        uuid.UUID: Id of the new download.
    """
    created_at = datetime.now() - timedelta(seconds=random.randint(60, 7 * 24 * 3600))
    return insert_download(cur, random_final_status(), created_at, random_duration_ms())


def start(cur: Cursor) -> uuid.UUID:
    """
    Start a new download (INSERT with status ``started``).

    Args:
        cur (Cursor): Cursor to run the insert with.

    Returns:
        uuid.UUID: Id of the new download.
    """
    return insert_download(cur, "started", datetime.now(), None)


def finish(cur: Cursor) -> uuid.UUID | None:
    """
    Complete or fail a random started download (UPDATE).

    Args:
        cur (Cursor): Cursor to run the update with.

    Returns:
        uuid.UUID | None: Id of the finished download, or None if none was running.
    """
    cur.execute(FINISH_SQL, (random_final_status(), random_duration_ms()))
    row = cur.fetchone()
    return row[0] if row else None


def purge(cur: Cursor) -> uuid.UUID | None:
    """
    Remove a random failed download (DELETE).

    Args:
        cur (Cursor): Cursor to run the delete with.

    Returns:
        uuid.UUID | None: Id of the removed download, or None if none had failed.
    """
    cur.execute(PURGE_SQL)
    row = cur.fetchone()
    return row[0] if row else None


OPERATIONS: dict[Callable[[Cursor], uuid.UUID | None], float] = {
    start: W_START,
    finish: W_FINISH,
    purge: W_PURGE,
}


def report(cur: Cursor, counts: Counter[str]) -> None:
    """
    Print the operations done so far and the completed downloads per tool.

    Args:
        cur (Cursor): Cursor to run the query with.
        counts (Counter[str]): Number of operations done, by operation name.
    """
    cur.execute(COMPLETED_BY_TOOL_SQL)
    lines = [f"ops so far {dict(counts)} | completed downloads per tool:"]
    for tool, downloads, last_download in cur.fetchall():
        lines.append(
            f"  {tool:<10} {downloads:>6}  last {last_download:%Y-%m-%d %H:%M:%S}"
        )
    print("\n".join(lines), flush=True)


def main() -> None:
    """
    Seed the table if needed and generate downloads until the container stops.
    """
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    cur = connect().cursor()

    cur.execute("SELECT count(*) FROM downloads")
    missing = max(0, INITIAL_ROWS - fetch_one(cur)[0])
    for _ in range(missing):
        seed(cur)
    print(f"seeded downloads table ({missing} new rows)", flush=True)

    counts: Counter[str] = Counter()
    last_report = time.monotonic()
    while True:
        operation = weighted_choice(OPERATIONS)
        if operation(cur) is not None:
            counts[operation.__name__] += 1

        if time.monotonic() - last_report >= REPORT_INTERVAL_S:
            report(cur, counts)
            last_report = time.monotonic()

        time.sleep(1 / OPS_PER_SEC)


if __name__ == "__main__":
    main()
