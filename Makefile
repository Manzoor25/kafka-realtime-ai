PYTHON ?= python
PYTEST ?= $(PYTHON) -m pytest
DOCKER_COMPOSE ?= docker compose

.PHONY: help install up down logs demo test test-unit test-integration test-e2e eval lint demo-outage demo-kill-worker demo-duplicate-doc demo-live-reembed

help:
	@echo "kafka-realtime-ai targets:"
	@echo "  make up                 Build and start 3-broker Kafka 4.4, Karapace, Qdrant, Ollama, Prometheus, Grafana & workers"
	@echo "  make demo               Run end-to-end streaming RAG + multi-agent ticket triage & human approval walkthrough"
	@echo "  make test               Run unit tests, integration tests, and retrieval evaluation"
	@echo "  make eval               Run retrieval benchmark before and after embedding model switchover"
	@echo "  make demo-outage        Failure demo 1: 2-minute LLM provider outage (0 tickets lost)"
	@echo "  make demo-kill-worker   Failure demo 2: Kill triage agent mid-LLM-call (redelivered, processed once)"
	@echo "  make demo-duplicate-doc Failure demo 3: Duplicate document event idempotency + tombstone cleanup"
	@echo "  make demo-live-reembed  Failure demo 4: Replay & switch embedding models under continuous QA load"
	@echo "  make down               Stop stack and remove containers/volumes"

install:
	$(PYTHON) -m pip install -e ".[dev]"

up:
	$(DOCKER_COMPOSE) up -d --build
	$(PYTHON) scripts/wait_for_stack.py

down:
	$(DOCKER_COMPOSE) down -v --remove-orphans

logs:
	$(DOCKER_COMPOSE) logs -f --tail=100

demo:
	$(PYTHON) scripts/run_demo.py

test-unit:
	$(PYTEST) tests/unit -v

test-integration:
	$(PYTEST) tests/integration -v -m "not docker_e2e"

test-e2e:
	$(PYTEST) tests/integration -v -m "docker_e2e"

eval:
	$(PYTHON) scripts/evaluate_retrieval.py

test:
	$(PYTEST) tests/unit tests/integration -v -m "not docker_e2e"
	$(PYTHON) scripts/evaluate_retrieval.py --provider local

lint:
	$(PYTHON) -m ruff check src tests scripts

demo-outage:
	$(PYTHON) scripts/failure_demos/demo_1_llm_outage.py

demo-kill-worker:
	$(PYTHON) scripts/failure_demos/demo_2_kill_worker.py

demo-duplicate-doc:
	$(PYTHON) scripts/failure_demos/demo_3_duplicate_doc.py

demo-live-reembed:
	$(PYTHON) scripts/failure_demos/demo_4_live_reembed.py
