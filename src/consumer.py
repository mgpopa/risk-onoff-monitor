import os, json, time
from datetime import datetime, timezone
from pathlib import Path
import yaml, pandas as pd
from dotenv import load_dotenv
from kafka import KafkaConsumer
from src.duckdb_utils import insert_quotes, compute_features, compute_regime

load_dotenv()
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC_QUOTES = os.environ.get("KAFKA_TOPIC_QUOTES", "market_quotes")
CONFIG_PATH = Path("config/symbols.yaml")

def load_cfg():
    return yaml.safe_load(open(CONFIG_PATH, "r"))

def minute_floor(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0, tzinfo=timezone.utc)

def main():
    cfg = load_cfg()
    uni_df = pd.DataFrame(cfg["universe"])
    score_cfg = cfg["score"]

    cons = KafkaConsumer(
        KAFKA_TOPIC_QUOTES,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="risk-onoff-consumer",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        key_deserializer=lambda b: b.decode("utf-8"),
        enable_auto_commit=True,
        auto_offset_reset="latest",
        consumer_timeout_ms=1000
    )
    print(f"[consumer] connected to {KAFKA_BOOTSTRAP}, topic={KAFKA_TOPIC_QUOTES}")

    buffer = []
    last_minute = None

    while True:
        polled = cons.poll(timeout_ms=500)
        if not polled:
            now = datetime.utcnow().replace(tzinfo=timezone.utc)
            m = minute_floor(now)
            if last_minute and m > last_minute:
                if buffer:
                    insert_quotes(buffer)
                    buffer.clear()
                compute_features(score_cfg)
                out = compute_regime(uni_df, score_cfg)
                if out:
                    minute_ts, score, label, _ = out
                    print(f"[regime] {minute_ts} score={score:.3f} label={label}")
                last_minute = m
            time.sleep(0.5)
            continue

        for _, messages in polled.items():
            for msg in messages:
                payload = msg.value
                if not all(k in payload for k in ("ts", "minute_ts", "symbol", "price")):
                    continue
                buffer.append({
                    "ts": payload["ts"],
                    "minute_ts": payload["minute_ts"],
                    "symbol": payload["symbol"],
                    "price": float(payload["price"]),
                })
                minute_ts = datetime.fromisoformat(payload["minute_ts"].replace("Z","")).replace(tzinfo=timezone.utc)
                if last_minute is None:
                    last_minute = minute_ts
                if minute_ts > last_minute:
                    insert_quotes(buffer)
                    buffer.clear()
                    compute_features(score_cfg)
                    out = compute_regime(uni_df, score_cfg)
                    if out:
                        minute_ts2, score, label, _ = out
                        print(f"[regime] {minute_ts2} score={score:.3f} label={label}")
                    last_minute = minute_ts

if __name__ == "__main__":
    main()
