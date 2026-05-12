"""
ATRA decision dashboard (Streamlit).

Run: streamlit run src/atra/dashboard/app.py
Or from repo root with package installed: streamlit run -m atra.dashboard.app
"""

from __future__ import annotations

import json
import os
from datetime import timedelta
from pathlib import Path

import pandas as pd
import requests
import streamlit as st

from atra.daily_pipeline import hours_since_last_insert, run_daily
from atra.db import (
    connect,
    database_backend_label,
    get_latest_daily_insight,
    init_db,
    insert_run,
    query_papers,
    upsert_papers,
    using_postgres,
)
from atra.insights import generate_and_store_daily_insight
from atra.summarize import summarize_missing
from atra.sources.arxiv import ArxivIngestParams, fetch_arxiv
from atra.tagging import list_sector_names, tag_missing_papers
from atra.trends import early_signals, sector_trend_series, top_tokens


def db_path() -> Path:
    """SQLite file path when not using PostgreSQL; ignored by ``connect()`` when ``ATRA_DATABASE_URL`` is set."""
    return Path(os.environ.get("ATRA_DB_PATH", "data/atra.db"))


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)))
    except ValueError:
        return default


def _auto_daily_ingest_enabled() -> bool:
    return os.environ.get("ATRA_AUTO_DAILY_INGEST", "").strip().lower() in ("1", "true", "yes", "on")


def _run_daily_with_env_limits(path: Path) -> None:
    """Same job as `python -m atra daily` with limits from environment (tunable for Streamlit timeouts)."""
    init_db(path)
    run_daily(
        path,
        days=_env_int("ATRA_DAILY_DAYS", 1),
        arxiv_limit=_env_int("ATRA_DAILY_ARXIV_LIMIT", 15),
        openalex_limit=_env_int("ATRA_DAILY_OPENALEX_LIMIT", 30),
    )


def _bootstrap_from_arxiv(path: Path, *, category: str, days: int, limit: int) -> tuple[int, int]:
    """Ingest → summarize → tag → briefing (small pull suitable for Streamlit Cloud)."""
    rows, params_json = fetch_arxiv(ArxivIngestParams(category=category, days=days, limit=limit))
    con = connect(path)
    try:
        insert_run(con, source="arxiv", params_json=params_json)
        inserted, skipped = upsert_papers(con, rows)
        con.commit()
    finally:
        con.close()
    summarize_missing(path, batch_limit=min(500, limit * 20))
    tag_missing_papers(path, batch_limit=min(500, limit * 20))
    generate_and_store_daily_insight(path)
    return inserted, skipped


def _ensure_stored_briefing() -> None:
    """Hosted apps never run `atra daily`; create a briefing row if the table is empty."""
    path = db_path()
    init_db(path)
    con = connect(path)
    try:
        if get_latest_daily_insight(con) is None:
            generate_and_store_daily_insight(path)
    finally:
        con.close()


@st.cache_data(ttl=120)
def load_latest_briefing() -> dict | None:
    init_db(db_path())
    con = connect(db_path())
    try:
        return get_latest_daily_insight(con)
    finally:
        con.close()


@st.fragment(run_every=timedelta(hours=8))
def _scheduled_daily_ingest() -> None:
    """When ATRA_AUTO_DAILY_INGEST=1, periodically ingest if the last paper is old enough."""
    if not _auto_daily_ingest_enabled():
        return
    path = db_path()
    min_h = _env_float("ATRA_DAILY_MIN_INTERVAL_HOURS", 18.0)
    h = hours_since_last_insert(path)
    if h is not None and h < min_h:
        return
    try:
        with st.spinner("Scheduled daily ingest — updating papers and trends…"):
            _run_daily_with_env_limits(path)
        load_latest_briefing.clear()
        st.rerun()
    except (OSError, requests.RequestException):
        return


def _sync_streamlit_secrets_into_environ() -> None:
    """Copy database URL from ``st.secrets`` to ``os.environ`` (Streamlit Cloud does not do this automatically)."""
    try:
        sec = st.secrets
    except (RuntimeError, FileNotFoundError, AttributeError):
        return

    discrete_keys = (
        "ATRA_PG_HOST",
        "ATRA_PG_PASSWORD",
        "ATRA_PG_USER",
        "ATRA_PG_DATABASE",
        "ATRA_PG_PORT",
        "SUPABASE_DB_HOST",
        "SUPABASE_DB_PASSWORD",
        "SUPABASE_DB_USER",
        "SUPABASE_DB_NAME",
        "SUPABASE_DB_PORT",
    )
    for key in discrete_keys:
        if os.environ.get(key, "").strip():
            continue
        try:
            if key not in sec:
                continue
        except Exception:
            continue
        val = sec[key]
        if val is not None and str(val).strip():
            os.environ[key] = str(val).strip()

    for key in ("ATRA_DATABASE_URL", "SUPABASE_DB_URL", "DATABASE_URL"):
        if os.environ.get(key, "").strip():
            continue
        try:
            if key not in sec:
                continue
        except Exception:
            continue
        val = sec[key]
        if val and str(val).strip():
            os.environ[key] = str(val).strip()

    # Optional nested secrets (e.g. [postgres] url = "..." or host/password in secrets.toml)
    try:
        for section in ("postgres", "postgresql", "supabase"):
            if section not in sec:
                continue
            blob = sec[section]
            if isinstance(blob, str) and blob.strip().startswith(
                ("postgresql://", "postgres://")
            ):
                os.environ.setdefault("ATRA_DATABASE_URL", blob.strip())
                continue
            if not isinstance(blob, dict):
                continue
            u = blob.get("url") or blob.get("uri") or blob.get("connection_url")
            if u and str(u).strip().startswith(("postgresql://", "postgres://")):
                os.environ.setdefault("ATRA_DATABASE_URL", str(u).strip())
                continue
            nested_map = {
                "host": "ATRA_PG_HOST",
                "hostname": "ATRA_PG_HOST",
                "user": "ATRA_PG_USER",
                "username": "ATRA_PG_USER",
                "password": "ATRA_PG_PASSWORD",
                "database": "ATRA_PG_DATABASE",
                "dbname": "ATRA_PG_DATABASE",
                "port": "ATRA_PG_PORT",
            }
            for src_key, env_key in nested_map.items():
                if os.environ.get(env_key, "").strip():
                    continue
                if src_key not in blob:
                    continue
                v = blob[src_key]
                if v is not None and str(v).strip():
                    os.environ.setdefault(env_key, str(v).strip())
    except Exception:
        pass


st.set_page_config(page_title="ATRA — MInT", layout="wide")
_sync_streamlit_secrets_into_environ()
st.title("ATRA — Tech trend & research intelligence")
_db_hint = (
    f"Storage: **{database_backend_label()}** (PostgreSQL via URL or `ATRA_PG_HOST` + `ATRA_PG_PASSWORD`)."
    if using_postgres()
    else f"Storage: **SQLite** at `{db_path()}`. On Streamlit Cloud, set **`ATRA_DATABASE_URL`** or discrete **`ATRA_PG_*`** secrets "
    "so papers and trends persist between restarts."
)
st.caption(
    "Ministry of Innovation and Technology · Daily briefing refreshes from the database every ~2 minutes while this page is open. "
    + _db_hint
)

try:
    init_db(db_path())
except Exception as exc:
    if using_postgres() and type(exc).__name__ == "OperationalError":
        st.error("Could not connect to PostgreSQL (check Supabase host, password, and network access).")
        st.markdown(
            r"""
**Common fixes for Streamlit Cloud + Supabase**

1. **Replace the placeholder** — The URI must use your real password, not the text `[YOUR-PASSWORD]` (remove the square brackets too).
2. **Prefer the pooler** — In Supabase → **Project Settings → Database**, copy the **Transaction pooler** or **Session pooler** connection string (often host like `*.pooler.supabase.com`, port **6543**). The direct host `db.*.supabase.co:5432` often fails from cloud hosts (IPv6 / network path).
3. **Avoid broken URLs** — If your password has `@`, `#`, `:`, `/`, or spaces, either escape them in the URI or **skip the URI** and put discrete secrets instead (password is quoted automatically):

```toml
ATRA_PG_HOST = "db.YOUR_REF.supabase.co"
ATRA_PG_USER = "postgres"
ATRA_PG_PASSWORD = "your actual password"
ATRA_PG_DATABASE = "postgres"
ATRA_PG_PORT = "5432"
```

Or use the pooler host/port Supabase shows for pooling.

4. **SSL** — The app adds `sslmode=require` for Supabase-style hosts when it is missing from the URL.

See **Manage app** → logs for the full libpq error (not shown here to avoid leaking credentials).
            """
        )
        st.stop()
    raise
_ensure_stored_briefing()

with st.sidebar:
    st.subheader("Load data")
    if st.button("Fetch sample papers from arXiv"):
        path = db_path()
        init_db(path)
        try:
            with st.spinner("Fetching from arXiv…"):
                ins, sk = _bootstrap_from_arxiv(path, category="cs.AI", days=7, limit=25)
            load_latest_briefing.clear()
            st.success(f"Stored {ins} new papers ({sk} duplicates skipped). Refreshing…")
        except (OSError, requests.RequestException) as exc:
            st.error(f"Could not reach arXiv or save data ({database_backend_label()}): {exc}")
            st.stop()
        st.rerun()
    st.divider()
    st.subheader("Daily trend updates")
    if st.button("Run full daily update now"):
        path = db_path()
        init_db(path)
        try:
            with st.spinner("Running daily pipeline (ingest → summarize → tag → briefing)…"):
                _run_daily_with_env_limits(path)
            load_latest_briefing.clear()
            st.success("Daily update finished. Refreshing…")
        except (OSError, requests.RequestException) as exc:
            st.error(f"Daily update failed (network or database): {exc}")
            st.stop()
        st.rerun()
    st.divider()
    st.header("Filters")
    date_from = st.text_input("Date from (YYYY-MM-DD)", "")
    date_to = st.text_input("Date to (YYYY-MM-DD)", "")
    sector = st.selectbox("Sector", [""] + list_sector_names())
    impact = st.selectbox("Impact", ["", "low", "medium", "high"])
    source = st.selectbox("Source", ["", "arxiv", "openalex"])
    search = st.text_input("Search (title / abstract / summary)", "")
    limit = st.slider("Max rows", 10, 300, 80)

_scheduled_daily_ingest()

con = connect(db_path())
try:
    papers = query_papers(
        con,
        date_from=date_from or None,
        date_to=date_to or None,
        sector=sector or None,
        impact=impact or None,
        source=source or None,
        search=search or None,
        limit=limit,
        offset=0,
    )
finally:
    con.close()

tab0, tab1, tab2, tab3 = st.tabs(
    ["Daily briefing", "Papers", "Trends", "Early signals"]
)

with tab0:
    briefing = load_latest_briefing()
    if not briefing:
        st.warning(
            "No briefing could be loaded. Add papers (sidebar), or run **`python -m atra daily`** / **`python -m atra insights`** "
            "against the same database. If you use PostgreSQL, confirm **`ATRA_DATABASE_URL`** is set in this app’s secrets."
        )
    else:
        st.subheader(f"Report date: {briefing.get('report_for_date', '—')}")
        st.caption(
            f"Generated: {briefing.get('generated_at') or briefing.get('stored_generated_at', '—')}"
        )
        hs = briefing.get("headline_stats") or {}
        c1, c2, c3 = st.columns(3)
        c1.metric("New items (24h)", hs.get("new_items_24h", "—"))
        c2.metric("Corpus size", hs.get("total_papers", "—"))
        c3.metric("Calendar date", hs.get("report_calendar_date", "—"))

        st.markdown("### Executive bullets")
        for b in briefing.get("narrative_bullets") or []:
            st.markdown(f"- {b}")

        mom = briefing.get("sector_momentum") or []
        if mom:
            st.markdown("### Sector momentum")
            st.dataframe(pd.DataFrame(mom), width="stretch", hide_index=True)

        em = briefing.get("emerging_keywords") or []
        if em:
            st.markdown("### Emerging keywords (vs prior week)")
            st.bar_chart(pd.DataFrame(em).set_index("token")["lift"])

        pb = briefing.get("priority_brief") or []
        if pb:
            st.markdown("### Priority brief (Ethiopia relevance)")
            st.dataframe(pd.DataFrame(pb), width="stretch", hide_index=True)

with tab1:
    if not papers:
        st.info(
            "No papers in the database yet. Use **Fetch sample papers from arXiv** in the sidebar, "
            "or run **`python -m atra daily`** pointed at the same database (SQLite path or `ATRA_DATABASE_URL`)."
        )
    else:
        rows = []
        for p in papers:
            sectors = p.get("sectors_json") or ""
            try:
                sj = json.loads(sectors) if sectors else []
                sec_txt = ", ".join(f"{x.get('sector','')} ({x.get('score','')})" for x in sj[:4])
            except json.JSONDecodeError:
                sec_txt = sectors[:120]
            rows.append(
                {
                    "id": p["id"],
                    "date": (p.get("published_at") or "")[:10],
                    "impact": p.get("impact_level"),
                    "ET relevance": p.get("relevance_et"),
                    "title": p.get("title"),
                    "sectors": sec_txt,
                    "source": p.get("source"),
                    "url": p.get("url"),
                }
            )
        df = pd.DataFrame(rows)
        st.dataframe(df, width="stretch", hide_index=True)

with tab2:
    st.caption(
        "Sector and keyword trends reflect **ingested papers**. Use **Run full daily update** or "
        "scheduled ingest so this view gains new days of activity."
    )
    series = sector_trend_series(db_path())
    if series:
        sdf = pd.DataFrame(series)
        try:
            pivot = sdf.pivot_table(
                index="date", columns="sector", values="count", aggfunc="sum"
            ).fillna(0)
            st.subheader("Activity by sector (daily)")
            st.line_chart(pivot)
        except Exception:
            st.dataframe(sdf, width="stretch", hide_index=True)
    else:
        st.info("No trend data yet.")

    kw = top_tokens(db_path(), top_n=25)
    if kw:
        st.subheader("Top keywords (recent papers)")
        st.bar_chart(pd.DataFrame(kw).set_index("token"))

with tab3:
    sigs = early_signals(db_path(), recent_days=14)
    if not sigs:
        st.info("No high-priority signals in the recent window.")
    else:
        st.dataframe(pd.DataFrame(sigs), width="stretch", hide_index=True)
