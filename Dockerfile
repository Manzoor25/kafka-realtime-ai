FROM python:3.12.10-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md LICENSE ./
COPY src ./src
COPY schemas ./schemas
COPY data ./data
COPY scripts ./scripts

RUN pip install --upgrade pip && pip install .

ENTRYPOINT ["python", "-m", "realtime_ai.cli"]
CMD ["api"]
