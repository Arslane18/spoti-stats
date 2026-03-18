import duckdb
from datetime import datetime

DB_PATH = "spoti_stats.db"


def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(DB_PATH)


def init_db() -> None:
    """Create all dimension and fact tables if they don't exist."""
    con = get_connection()

    con.execute("""
        CREATE TABLE IF NOT EXISTS dim_artist (
            artist_id   INTEGER PRIMARY KEY,
            artist_name VARCHAR NOT NULL UNIQUE
        )
    """)

    con.execute("""
        CREATE SEQUENCE IF NOT EXISTS seq_artist START 1
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS dim_album (
            album_id   INTEGER PRIMARY KEY,
            album_name VARCHAR NOT NULL,
            artist_id  INTEGER NOT NULL REFERENCES dim_artist(artist_id),
            UNIQUE (album_name, artist_id)
        )
    """)

    con.execute("""
        CREATE SEQUENCE IF NOT EXISTS seq_album START 1
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS dim_track (
            track_id    VARCHAR PRIMARY KEY,
            track_name  VARCHAR NOT NULL,
            duration_ms INTEGER NOT NULL
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS dim_date (
            date_id     INTEGER PRIMARY KEY,
            played_at   TIMESTAMP NOT NULL UNIQUE,
            hour        INTEGER NOT NULL,
            day_of_week VARCHAR NOT NULL,
            week        INTEGER NOT NULL,
            month       INTEGER NOT NULL,
            year        INTEGER NOT NULL,
            is_weekend  BOOLEAN NOT NULL
        )
    """)

    con.execute("""
        CREATE SEQUENCE IF NOT EXISTS seq_date START 1
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS fact_plays (
            play_id     INTEGER PRIMARY KEY,
            date_id     INTEGER NOT NULL REFERENCES dim_date(date_id),
            track_id    VARCHAR NOT NULL REFERENCES dim_track(track_id),
            artist_id   INTEGER NOT NULL REFERENCES dim_artist(artist_id),
            album_id    INTEGER NOT NULL REFERENCES dim_album(album_id),
            duration_ms INTEGER NOT NULL
        )
    """)

    con.execute("""
        CREATE SEQUENCE IF NOT EXISTS seq_play START 1
    """)

    con.close()
    print("Database initialized.")


def _get_or_create_artist(con: duckdb.DuckDBPyConnection, artist_name: str) -> int:
    row = con.execute(
        "SELECT artist_id FROM dim_artist WHERE artist_name = ?", [artist_name]
    ).fetchone()
    if row:
        return row[0]
    artist_id = con.execute("SELECT nextval('seq_artist')").fetchone()[0]
    con.execute(
        "INSERT INTO dim_artist VALUES (?, ?)", [artist_id, artist_name]
    )
    return artist_id


def _get_or_create_album(
    con: duckdb.DuckDBPyConnection, album_name: str, artist_id: int
) -> int:
    row = con.execute(
        "SELECT album_id FROM dim_album WHERE album_name = ? AND artist_id = ?",
        [album_name, artist_id],
    ).fetchone()
    if row:
        return row[0]
    album_id = con.execute("SELECT nextval('seq_album')").fetchone()[0]
    con.execute(
        "INSERT INTO dim_album VALUES (?, ?, ?)", [album_id, album_name, artist_id]
    )
    return album_id


def _get_or_create_track(
    con: duckdb.DuckDBPyConnection, track_id: str, track_name: str, duration_ms: int
) -> str:
    row = con.execute(
        "SELECT track_id FROM dim_track WHERE track_id = ?", [track_id]
    ).fetchone()
    if not row:
        con.execute(
            "INSERT INTO dim_track VALUES (?, ?, ?)",
            [track_id, track_name, duration_ms],
        )
    return track_id


def _get_or_create_date(
    con: duckdb.DuckDBPyConnection, played_at: datetime
) -> int:
    row = con.execute(
        "SELECT date_id FROM dim_date WHERE played_at = ?", [played_at]
    ).fetchone()
    if row:
        return row[0]
    date_id = con.execute("SELECT nextval('seq_date')").fetchone()[0]
    con.execute(
        """INSERT INTO dim_date VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            date_id,
            played_at,
            played_at.hour,
            played_at.strftime("%A"),
            played_at.isocalendar().week,
            played_at.month,
            played_at.year,
            played_at.weekday() >= 5,
        ],
    )
    return date_id


def insert_plays(tracks: list[dict]) -> int:
    """Insert a list of tracks into the star schema. Returns number of new rows inserted."""
    con = get_connection()
    inserted = 0

    for track in tracks:
        played_at: datetime = track["played_at"]

        # Skip already-stored plays (idempotent)
        already_exists = con.execute(
            """
            SELECT 1 FROM fact_plays f
            JOIN dim_date d ON f.date_id = d.date_id
            WHERE d.played_at = ? AND f.track_id = ?
            """,
            [played_at, track["id"]],
        ).fetchone()

        if already_exists:
            continue

        artist_id = _get_or_create_artist(con, track["artist"])
        album_id  = _get_or_create_album(con, track["album"], artist_id)
        _get_or_create_track(con, track["id"], track["name"], track["duration_ms"])
        date_id   = _get_or_create_date(con, played_at)

        play_id = con.execute("SELECT nextval('seq_play')").fetchone()[0]
        con.execute(
            "INSERT INTO fact_plays VALUES (?, ?, ?, ?, ?, ?)",
            [play_id, date_id, track["id"], artist_id, album_id, track["duration_ms"]],
        )
        inserted += 1

    con.close()
    return inserted
