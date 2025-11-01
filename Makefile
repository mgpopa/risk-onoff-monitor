SHELL := /bin/bash
VENV ?= .venv
PY := $(VENV)/bin/python
PIP := $(VENV)/bin/pip
.PHONY: up down install produce consume plot demo
up:
 docker compose -f docker/docker-compose.yml up -d
down:
 docker compose -f docker/docker-compose.yml down
install:
 python3 -m venv $(VENV)
 $(PIP) install - upgrade pip
 $(PIP) install -r requirements.txt
produce:
 set -a && [ -f .env ] && source .env || true; set +a; \
 $(PY) src/producer.py
consume:
 set -a && [ -f .env ] && source .env || true; set +a; \
 $(PY) src/consumer.py
plot:
 $(PY) analytics/plot_regime.py
demo:
 PRODUCER_MODE=demo $(PY) src/producer.py
