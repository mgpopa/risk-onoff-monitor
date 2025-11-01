# Live Cross-Asset Risk-On / Risk-Off Monitor
Streams minute prices for a small cross-asset universe, computes rolling features in DuckDB, and emits a composite Risk-On / Risk-Off score. No website - just notebooks/plots.
Stack: Kafka (Docker), Python, DuckDB, Matplotlib.