from pathlib import Path
import duckdb, pandas as pd, matplotlib.pyplot as plt

DB_PATH = Path("data/market.duckdb")
def main():
	if not DB_PATH.exists():
		print("No data yet. Run producer & consumer first.")
		return
	con = duckdb.connect(DB_PATH.as_posix())
	df_score = con.execute("""
		SELECT minute_ts, score, label
		FROM regime
		WHERE minute_ts >= now() - INTERVAL 6 HOUR
		ORDER BY minute_ts
	""").fetch_df()
	if df_score.empty:
		print("No regime data yet.")
		return
	eq_ret = con.execute("""
		WITH r AS (
			SELECT symbol, minute_ts,
				price/lag(price) OVER (PARTITION BY symbol ORDER BY minute_ts) - 1 AS ret
			FROM quotes
			WHERE minute_ts >= (SELECT min(minute_ts) FROM regime)
		),
		buckets AS (
			SELECT symbol,
				CASE WHEN symbol IN ('SPY','QQQ','VGK','EWG') THEN 'risk_on'
					WHEN symbol IN ('TLT','GLD') THEN 'risk_off'
					ELSE 'fx'
				END AS bucket
			FROM (SELECT DISTINCT symbol FROM quotes)
		),
		joined AS (
			SELECT r.*, b.bucket FROM r r JOIN buckets b USING(symbol)
		)
		SELECT minute_ts,
				avg(CASE WHEN bucket='risk_on' THEN ret END) AS ret_risk_on,
				avg(CASE WHEN bucket='risk_off' THEN ret END) AS ret_risk_off,
				avg(CASE WHEN bucket='fx' THEN ret END) AS ret_fx
		FROM joined
		GROUP BY minute_ts
		ORDER BY minute_ts
	""").fetch_df()
	for col in ['ret_risk_on','ret_risk_off','ret_fx']:
		df_score[col] = 0.0
	df_b = eq_ret.set_index('minute_ts').fillna(0.0)
	df_score = df_score.set_index('minute_ts')
	for col in ['ret_risk_on','ret_risk_off','ret_fx']:
		if col in df_b:
			df_score[col] = (1 + df_b[col]).cumprod() - 1
	df_score = df_score.reset_index()

	fig = plt.figure(figsize=(12,7))
	ax1 = fig.add_subplot(2,1,1)
	ax1.plot(df_score['minute_ts'], df_score['score'], label='Risk-On Score')
	ax1.axhline(0.5, linestyle=' - ', linewidth=1)
	ax1.axhline(-0.5, linestyle=' - ', linewidth=1)
	ax1.set_title("Live Risk-On / Risk-Off Score")
	ax1.set_ylabel("Score")
	for i in range(len(df_score)-1):
		ts0 = df_score['minute_ts'].iloc[i]
		ts1 = df_score['minute_ts'].iloc[i+1]
		label = df_score['label'].iloc[i]
		if label == 'RISK_ON':
			ax1.axvspan(ts0, ts1, alpha=0.08)
		elif label == 'RISK_OFF':
			ax1.axvspan(ts0, ts1, alpha=0.08)
		ax1.legend(loc='upper left')

	ax2 = fig.add_subplot(2,1,2)
	ax2.plot(df_score['minute_ts'], df_score['ret_risk_on'], label='Risk-on (cum)')
	ax2.plot(df_score['minute_ts'], df_score['ret_risk_off'], label='Defensive (cum)')
	ax2.plot(df_score['minute_ts'], df_score['ret_fx'], label='FX (cum)')
	ax2.set_ylabel("Cumulative return")
	ax2.set_xlabel("Time")
	ax2.legend(loc='upper left')
	fig.tight_layout()
	out = Path("reports") / "regime_dashboard.png"
	fig.savefig(out, dpi=150)
	print(f"Wrote {out}")

if __name__ == "__main__":
	main()

