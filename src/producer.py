import os, time, json, random
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List
import requests, yaml
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC_QUOTES = os.environ.get("KAFKA_TOPIC_QUOTES", "market_quotes")
API_KEY = os.environ.get("TWELVEDATA_API_KEY")
PRODUCER_MODE = os.environ.get("PRODUCER_MODE", "demo") # "live" or "demo" / I start with demo
CONFIG_PATH = Path("config/symbols.yaml")

def utc_minute(ts: datetime) -> datetime:
    return ts.replace(second=0, microsecond=0, tzinfo=timezone.utc)

def load_universe():
    return yaml.safe_load(open(CONFIG_PATH, "r"))

def get_td_quote(symbol: str) -> Dict[str, Any]:
    assert API_KEY, "TWELVEDATA_API_KEY is not yet set. Use demo mode or set your key."
    url = "https://api.twelvedata.com/quote"
    params = {"symbol": symbol, "apikey": API_KEY}
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict) and data.get("code"):
        raise RuntimeError(f"TwelveData error for {symbol}: {data}")
    price = None
    for key in ("close", "price"):
        if key in data and data[key]:
            try:
                price = float(data[key])
            except Exception:
                pass
    ts = data.get("datetime") or data.get("timestamp") or datetime.utcnow().isoformat()
    return {"price": price, "provider_ts": ts, "raw": data}

def demo_quote(last: float) -> float:
    step = random.gauss(0, 0.05)
    return max(0.01, last * (1 + step/100.0))


def main():
    cfg = load_universe()
    uni = cfg["universe"]
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8"),
        linger_ms=10, retries=3, acks="all",
    )
    state_last_price: Dict[str, float] = {}
    print(f"[producer] mode={PRODUCER_MODE} bootstrap={KAFKA_BOOTSTRAP} topic={KAFKA_TOPIC_QUOTES}")
    while True:
        now = datetime.utcnow().replace(tzinfo=timezone.utc)
        m_ts = utc_minute(now)
        batch: List[Dict[str, Any]] = []
        for asset in uni:
            name = asset["name"]
            symbol = asset["provider_symbol"]
            if PRODUCER_MODE == "demo" or not API_KEY:
                last = state_last_price.get(name, random.uniform(50, 200))
                price = demo_quote(last)
                state_last_price[name] = price
                payload = {"ts": now.isoformat(),"minute_ts": m_ts.isoformat(),"symbol": name,"price": price,"provider": "demo"}
            else:
                try:
                    q = get_td_quote(symbol)
                    price = q["price"]
                    if price is None:
                        raise RuntimeError(f"No price for {symbol}: {q['raw']}")
                    payload = {"ts": now.isoformat(),"minute_ts": m_ts.isoformat(),"symbol": name,"price": float(price),"provider": "twelvedata"}
                except Exception as e:
                    print(f"[producer] error fetching {symbol}: {e}")
                    continue
            batch.append(payload)

        for rec in batch:
            try:
                producer.send(KAFKA_TOPIC_QUOTES, key=rec["symbol"], value=rec)
            except Exception as e:
                print(f"[producer] send error: {e} -> {rec}")
        producer.flush()
        print(f"[producer] published {len(batch)} @ {m_ts.isoformat()}")
        elapsed = (datetime.utcnow().replace(tzinfo=timezone.utc) - m_ts).total_seconds()
        time.sleep(max(1.0, 60.0 - elapsed + 0.2))

if __name__ == "__main__":
    main()