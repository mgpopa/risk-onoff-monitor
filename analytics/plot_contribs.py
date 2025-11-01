from pathlib import Path
import duckdb, pandas as pd, matplotlib.pyplot as plt

DB_PATH = Path("data/market.duckdb")
def main():
    if not DB_PATH.exists():
        print("No DB yet. Run the pipeline first.")
        return
    con = duckdb.connect(DB_PATH.as_posix())
    latest = con.execute("SELECT max(minute_ts) FROM regime_contribs").fetchone()[0]
    if latest is None:
        print("No contributions yet.")
        return
    df = con.execute("""
        SELECT symbol, contrib FROM regime_contribs
        WHERE minute_ts = ?
        ORDER BY contrib DESC
    """, [latest]).fetch_df()
    fig = plt.figure(figsize=(8,5))
    ax = fig.add_subplot(1,1,1)
    ax.bar(df['symbol'], df['contrib'])
    ax.set_title(f"Symbol contributions @ {latest}")
    ax.set_ylabel("Weighted z-return")
    fig.tight_layout()
    out = Path("reports") / "contributions_latest.png"
    fig.savefig(out, dpi=150)
    print(f"Wrote {out}")

if __name__ == "__main__":
    main()