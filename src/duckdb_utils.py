import duckdb
from pathlib import Path
from typing import Optional, List, Dict, Any
import pandas as pd

DB_PATH = Path("data/market.duckdb")

DDL_QUOTES = """
CREATE TABLE IF NOT EXISTS quotes (
    ts TIMESTAMP,
    minute_ts TIMESTAMP,
    symbol TEXT,
    price DOUBLE
);
"""

DDL_FEATURES = """
CREATE TABLE IF NOT EXISTS features (
    ts TIMESTAMP,
    minute_ts TIMESTAMP,
    symbol TEXT,
    ret_1m DOUBLE,
    vol_15m DOUBLE,
    zret_60m DOUBLE
);
"""

DDL_REGIME = """
CREATE TABLE IF NOT EXISTS regime (
    minute_ts TIMESTAMP,
    score DOUBLE,
    label TEXT
);
"""

DDL_CONTRIBS = """
CREATE TABLE IF NOT EXISTS regime_contribs (
    minute_ts TIMESTAMP,
    symbol TEXT,
    contrib DOUBLE
);
"""

def get_conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(DB_PATH.as_posix())
    con.execute(DDL_QUOTES)
    con.execute(DDL_FEATURES)
    con.execute(DDL_REGIME)
    con.execute(DDL_CONTRIBS)
    return con

def insert_quotes(rows: List[Dict[str, Any]]):
    if not rows:
        return
    df = pd.DataFrame(rows)
    con = get_conn()
    con.register('df_quotes', df)
    con.execute("""
        INSERT INTO quotes
        SELECT ts, minute_ts, symbol, price FROM df_quotes
        """)
    con.commit()

def compute_features(score_cfg: Dict[str, Any], min_since: Optional[pd.Timestamp] = None):
    con = get_conn()
    z_win = int(score_cfg.get("z_window", 60))
    vol_win = int(score_cfg.get("vol_window", 15))
    con.execute("""
        WITH q AS (
            SELECT *
            FROM quotes
            WHERE minute_ts >= (SELECT coalesce(max(minute_ts), '1970–01–01') - INTERVAL {lookback} MINUTE FROM features)
        ),
        r AS (
            SELECT
                symbol,
                ts,
                minute_ts,
                price / lag(price) OVER (PARTITION BY symbol ORDER BY minute_ts, ts) - 1 AS ret_1m
                FROM q
        ),
        f AS (
            SELECT
                symbol,
                ts,
                minute_ts,
                ret_1m,
                stddev_samp(ret_1m) OVER (
                    PARTITION BY symbol ORDER BY minute_ts, ts
                    ROWS BETWEEN {vol_win} PRECEDING AND CURRENT ROW
                ) AS vol_15m,
                (ret_1m - avg(ret_1m) OVER (
                    PARTITION BY symbol ORDER BY minute_ts, ts
                    ROWS BETWEEN {z_win} PRECEDING AND CURRENT ROW
                )) / nullif(stddev_samp(ret_1m) OVER (
                    PARTITION BY symbol ORDER BY minute_ts, ts
                    ROWS BETWEEN {z_win} PRECEDING AND CURRENT ROW
                    ), 0) AS zret_60m
            FROM r
        )
        INSERT INTO features
        SELECT ts, minute_ts, symbol, ret_1m, vol_15m, zret_60m
        FROM f
        WHERE ret_1m IS NOT NULL
            AND minute_ts > (SELECT coalesce(max(minute_ts), '1970–01–01') FROM features WHERE symbol = f.symbol);
    """.format(vol_win=vol_win, z_win=z_win, lookback=z_win+5))
    con.commit()

def compute_regime(symbol_cfg: pd.DataFrame, score_cfg: Dict[str, Any]):
    con = get_conn()
    hi = float(score_cfg.get("hi_threshold", 0.5))
    lo = float(score_cfg.get("lo_threshold", -0.5))
    latest = con.execute("SELECT max(minute_ts) FROM features").fetchone()[0]
    if latest is None:
        return None
    feats = con.execute("""
        SELECT symbol, zret_60m
        FROM features
        WHERE minute_ts = ?
    """, [latest]).fetch_df()
    if feats.empty:
        return None
    merged = feats.merge(symbol_cfg[['name','orientation','weight']],
                        left_on='symbol', right_on='name', how='left')
    merged['orientation'] = merged['orientation'].fillna(0.0)
    merged['weight'] = merged['weight'].fillna(0.0)
    merged['contrib'] = merged['zret_60m'] * merged['orientation'] * merged['weight']
    score = float(merged['contrib'].sum())
    label = 'NEUTRAL'
    if score >= hi:
        label = 'RISK_ON'
    elif score <= lo:
        label = 'RISK_OFF'

    con.execute("INSERT INTO regime (minute_ts, score, label) VALUES (?, ?, ?)", [latest, score, label])
    con.register("df_contribs", merged[['symbol','contrib']])
    con.execute("""
        INSERT INTO regime_contribs (minute_ts, symbol, contrib)
        SELECT ?, symbol, contrib FROM df_contribs
        """, [latest])
    con.commit()
    return latest, score, label, merged[['symbol','contrib']].sort_values('contrib', ascending=False)
