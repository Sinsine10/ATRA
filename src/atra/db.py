from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

DEFAULT_DB_PATH = Path("data") / "atra.db"

_EXTRA_COLUMNS = [
    ("relevance_et", "REAL"),
    ("impact_level", "TEXT"),
    ("sectors_json", "TEXT"),
    ("cited_by_count", "INTEGER"),
]


def _normalize_database_url(url: str) -> str:
    """Merge SSL and timeout query params — Supabase requires TLS; Streamlit Cloud often omits sslmode."""
    url = url.strip()
    if not url:
        return url
    parsed = urlparse(url)
    if parsed.scheme not in ("postgresql", "postgres"):
        return url
    pairs = list(parse_qsl(parsed.query, keep_blank_values=True))
    keys = {k.lower() for k, _ in pairs}
    host = (parsed.hostname or "").lower()

    if "sslmode" not in keys:
        require_ssl = "supabase" in host or os.environ.get(
            "ATRA_PG_REQUIRE_SSL", ""
        ).strip().lower() in ("1", "true", "yes")
        if require_ssl:
            sslmode = (os.environ.get("ATRA_PG_SSLMODE") or "require").strip() or "require"
            pairs.append(("sslmode", sslmode))

    if "connect_timeout" not in keys:
        to = (os.environ.get("ATRA_PG_CONNECT_TIMEOUT") or "30").strip()
        if to.isdigit():
            pairs.append(("connect_timeout", to))

    return urlunparse(parsed._replace(query=urlencode(pairs)))


def _database_url() -> str | None:
    for key in ("ATRA_DATABASE_URL", "SUPABASE_DB_URL", "DATABASE_URL"):
        v = os.environ.get(key, "").strip()
        if v.startswith(("postgresql://", "postgres://")):
            return _normalize_database_url(v)
    return None


def using_postgres() -> bool:
    return _database_url() is not None


def database_backend_label() -> str:
    return "postgresql" if using_postgres() else "sqlite"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def order_coalesced_pub_ins_desc() -> str:
    """ORDER BY clause: newest papers by published or inserted time."""
    if using_postgres():
        return "COALESCE(published_at::timestamptz, inserted_at::timestamptz) DESC NULLS LAST"
    return "datetime(COALESCE(published_at, inserted_at)) DESC"


def order_inserted_desc() -> str:
    """ORDER BY clause: newest by ingest time."""
    if using_postgres():
        return "inserted_at::timestamptz DESC NULLS LAST"
    return "datetime(inserted_at) DESC"


def date_prefix_expr() -> str:
    """Comparable calendar date (YYYY-MM-DD) from published or inserted."""
    return "LEFT(COALESCE(published_at, inserted_at), 10)"


def exec_sql(con: Any, sql: str, params: Sequence[Any] = ()) -> Any:
    """Run SQL with ``?`` placeholders (normalized to %%s on PostgreSQL)."""
    if using_postgres():
        sql = sql.replace("?", "%s")
        return con.execute(sql, tuple(params))
    return con.execute(sql, tuple(params))


def connect(db_path: Path | None = None) -> Any:
    path = DEFAULT_DB_PATH if db_path is None else Path(db_path)
    url = _database_url()
    if url:
        import psycopg
        from psycopg.rows import dict_row

        # Normalized URL adds sslmode + connect_timeout for Supabase; libpq reads them from conninfo.
        return psycopg.connect(url, row_factory=dict_row)
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(path))
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON;")
    return con


def _migrate_sqlite(con: sqlite3.Connection) -> None:
    for col, coltype in _EXTRA_COLUMNS:
        try:
            con.execute(f"ALTER TABLE papers ADD COLUMN {col} {coltype};")
        except sqlite3.OperationalError:
            pass
    try:
        con.execute("ALTER TABLE papers ADD COLUMN summary TEXT;")
    except sqlite3.OperationalError:
        pass


def _init_postgres(con: Any) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS runs (
          id BIGSERIAL PRIMARY KEY,
          started_at TEXT NOT NULL,
          source TEXT NOT NULL,
          params_json TEXT NOT NULL
        );
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS papers (
          id BIGSERIAL PRIMARY KEY,
          source TEXT NOT NULL,
          external_id TEXT NOT NULL,
          url TEXT,
          title TEXT NOT NULL,
          abstract TEXT,
          published_at TEXT,
          updated_at TEXT,
          authors_json TEXT,
          categories_json TEXT,
          summary TEXT,
          relevance_et DOUBLE PRECISION,
          impact_level TEXT,
          sectors_json TEXT,
          cited_by_count INTEGER,
          inserted_at TEXT NOT NULL,
          UNIQUE(source, external_id)
        );
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_papers_source_published
          ON papers(source, published_at);
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_insights (
          report_for_date TEXT NOT NULL PRIMARY KEY,
          generated_at TEXT NOT NULL,
          payload_json TEXT NOT NULL
        );
        """
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_papers_impact ON papers(impact_level);"
    )


def init_db(db_path: Path | None = None) -> None:
    path = DEFAULT_DB_PATH if db_path is None else Path(db_path)
    if using_postgres():
        con = connect(path)
        try:
            _init_postgres(con)
            con.commit()
        finally:
            con.close()
        return

    con = connect(path)
    try:
        con.executescript(
            """
            CREATE TABLE IF NOT EXISTS runs (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              started_at TEXT NOT NULL,
              source TEXT NOT NULL,
              params_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS papers (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              source TEXT NOT NULL,
              external_id TEXT NOT NULL,
              url TEXT,
              title TEXT NOT NULL,
              abstract TEXT,
              published_at TEXT,
              updated_at TEXT,
              authors_json TEXT,
              categories_json TEXT,
              summary TEXT,
              relevance_et REAL,
              impact_level TEXT,
              sectors_json TEXT,
              cited_by_count INTEGER,
              inserted_at TEXT NOT NULL,
              UNIQUE(source, external_id)
            );

            CREATE INDEX IF NOT EXISTS idx_papers_source_published
              ON papers(source, published_at);

            CREATE TABLE IF NOT EXISTS daily_insights (
              report_for_date TEXT NOT NULL PRIMARY KEY,
              generated_at TEXT NOT NULL,
              payload_json TEXT NOT NULL
            );
            """
        )
        _migrate_sqlite(con)
        try:
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_papers_impact ON papers(impact_level);"
            )
        except sqlite3.OperationalError:
            pass
        con.commit()
    finally:
        con.close()


@dataclass(frozen=True)
class PaperRow:
    source: str
    external_id: str
    url: Optional[str]
    title: str
    abstract: Optional[str]
    published_at: Optional[str]
    updated_at: Optional[str]
    authors_json: Optional[str]
    categories_json: Optional[str]
    summary: Optional[str] = None
    relevance_et: Optional[float] = None
    impact_level: Optional[str] = None
    sectors_json: Optional[str] = None
    cited_by_count: Optional[int] = None


def insert_run(con: Any, *, source: str, params_json: str) -> int:
    now = utc_now_iso()
    if using_postgres():
        row = exec_sql(
            con,
            """
            INSERT INTO runs(started_at, source, params_json)
            VALUES (?, ?, ?) RETURNING id
            """,
            (now, source, params_json),
        ).fetchone()
        return int(row["id"]) if row else 0
    cur = exec_sql(
        con,
        "INSERT INTO runs(started_at, source, params_json) VALUES (?, ?, ?)",
        (now, source, params_json),
    )
    return int(cur.lastrowid)


def upsert_papers(con: Any, rows: Iterable[PaperRow]) -> tuple[int, int]:
    inserted = 0
    skipped = 0
    now = utc_now_iso()
    if using_postgres():
        sql = """
            INSERT INTO papers(
              source, external_id, url, title, abstract,
              published_at, updated_at, authors_json, categories_json,
              summary, relevance_et, impact_level, sectors_json, cited_by_count,
              inserted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (source, external_id) DO NOTHING
            """
        for r in rows:
            cur = exec_sql(
                con,
                sql,
                (
                    r.source,
                    r.external_id,
                    r.url,
                    r.title,
                    r.abstract,
                    r.published_at,
                    r.updated_at,
                    r.authors_json,
                    r.categories_json,
                    r.summary,
                    r.relevance_et,
                    r.impact_level,
                    r.sectors_json,
                    r.cited_by_count,
                    now,
                ),
            )
            if cur.rowcount and cur.rowcount > 0:
                inserted += 1
            else:
                skipped += 1
        return inserted, skipped

    for r in rows:
        try:
            exec_sql(
                con,
                """
                INSERT INTO papers(
                  source, external_id, url, title, abstract,
                  published_at, updated_at, authors_json, categories_json,
                  summary, relevance_et, impact_level, sectors_json, cited_by_count,
                  inserted_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    r.source,
                    r.external_id,
                    r.url,
                    r.title,
                    r.abstract,
                    r.published_at,
                    r.updated_at,
                    r.authors_json,
                    r.categories_json,
                    r.summary,
                    r.relevance_et,
                    r.impact_level,
                    r.sectors_json,
                    r.cited_by_count,
                    now,
                ),
            )
            inserted += 1
        except sqlite3.IntegrityError:
            skipped += 1
    return inserted, skipped


def list_papers(con: Any, *, limit: int = 10) -> list[Any]:
    cur = exec_sql(
        con,
        f"""
        SELECT id, source, external_id, title, published_at, url, summary,
               relevance_et, impact_level, sectors_json
        FROM papers
        ORDER BY {order_inserted_desc()}
        LIMIT ?
        """,
        (limit,),
    )
    return list(cur.fetchall())


def query_papers(
    con: Any,
    *,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    sector: Optional[str] = None,
    impact: Optional[str] = None,
    source: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    dpe = date_prefix_expr()
    clauses: list[str] = ["1=1"]
    params: list[Any] = []

    if date_from:
        clauses.append(f"{dpe} >= ?")
        params.append(date_from[:10])
    if date_to:
        clauses.append(f"{dpe} <= ?")
        params.append(date_to[:10])
    if impact:
        clauses.append("LOWER(COALESCE(impact_level,'')) = LOWER(?)")
        params.append(impact)
    if source:
        clauses.append("LOWER(source) = LOWER(?)")
        params.append(source)
    if search:
        like = f"%{search.lower()}%"
        clauses.append(
            "(LOWER(title) LIKE ? OR LOWER(COALESCE(abstract,'')) LIKE ? OR LOWER(COALESCE(summary,'')) LIKE ?)"
        )
        params.extend([like, like, like])
    if sector:
        clauses.append("LOWER(COALESCE(sectors_json,'')) LIKE ?")
        params.append(f"%{sector.lower()}%")

    where_sql = " AND ".join(clauses)
    ob = order_coalesced_pub_ins_desc()
    sql = f"""
        SELECT id, source, external_id, url, title, abstract, published_at, summary,
               relevance_et, impact_level, sectors_json, cited_by_count, categories_json
        FROM papers
        WHERE {where_sql}
        ORDER BY {ob}
        LIMIT ? OFFSET ?
    """
    params.extend([limit, offset])
    cur = exec_sql(con, sql, params)
    return [dict(r) for r in cur.fetchall()]


def count_papers(con: Any) -> int:
    row = exec_sql(con, "SELECT COUNT(*) AS c FROM papers").fetchone()
    return int(row["c"]) if row else 0


def get_paper_by_id(con: Any, paper_id: int) -> Optional[dict[str, Any]]:
    cur = exec_sql(
        con,
        """
        SELECT id, source, external_id, url, title, abstract, published_at, summary,
               relevance_et, impact_level, sectors_json, cited_by_count, authors_json, categories_json
        FROM papers WHERE id = ?
        """,
        (paper_id,),
    )
    r = cur.fetchone()
    return dict(r) if r else None


def papers_for_trends(con: Any) -> list[Any]:
    cur = exec_sql(
        con,
        """
        SELECT id, published_at, sectors_json, title, abstract, summary, inserted_at
        FROM papers
        WHERE published_at IS NOT NULL OR inserted_at IS NOT NULL
        """,
    )
    return list(cur.fetchall())


def save_daily_insight(
    con: Any,
    *,
    report_for_date: str,
    payload: dict[str, Any],
) -> None:
    exec_sql(
        con,
        """
        INSERT INTO daily_insights(report_for_date, generated_at, payload_json)
        VALUES (?, ?, ?)
        ON CONFLICT(report_for_date) DO UPDATE SET
          generated_at = excluded.generated_at,
          payload_json = excluded.payload_json
        """,
        (report_for_date[:10], utc_now_iso(), json.dumps(payload, ensure_ascii=False)),
    )


def get_latest_daily_insight(con: Any) -> Optional[dict[str, Any]]:
    row = exec_sql(
        con,
        """
        SELECT report_for_date, generated_at, payload_json
        FROM daily_insights
        ORDER BY report_for_date DESC
        LIMIT 1
        """,
    ).fetchone()
    if not row:
        return None
    data = json.loads(row["payload_json"])
    data["report_for_date"] = row["report_for_date"]
    data["stored_generated_at"] = row["generated_at"]
    return data


def list_daily_insights(con: Any, *, limit: int = 14) -> list[dict[str, Any]]:
    rows = exec_sql(
        con,
        """
        SELECT report_for_date, generated_at, payload_json
        FROM daily_insights
        ORDER BY report_for_date DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        payload = json.loads(r["payload_json"])
        payload["report_for_date"] = r["report_for_date"]
        payload["stored_generated_at"] = r["generated_at"]
        out.append(payload)
    return out
