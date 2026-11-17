# ADR-0002: Qdrant Vector Store with UUIDv5 Idempotency and Alias Cutover

- **Status:** Accepted
- **Date:** 2026-11-16

## Context
Our streaming RAG pipeline requires:
1. Sub-second vector upserts and filtered deletes when Kafka tombstones arrive.
2. Strictly idempotent writes so at-least-once Kafka replays never create duplicate chunks.
3. Zero-downtime corpus re-embedding from a 384-dimension model (`all-minilm:22m` / `local-hash-v1`) to a 768-dimension model (`nomic-embed-text:v1.5` / `local-semantic-v2`) while live `/v1/qa` traffic continues uninterrupted.

We evaluated **Qdrant** (`qdrant/qdrant:v1.20.2`) versus **PostgreSQL + pgvector**.

## Decision
Select **Qdrant (`v1.20.2`)** with:
1. **Deterministic UUIDv5 point IDs** computed over `uuid5(CHUNK_NAMESPACE, f"{doc_id}:{chunk_hash}:{embedding_version}")`, followed by a filtered prune of any superseded `point_id`s belonging to `doc_id`.
2. **Versioned physical collections** (`kb_chunks_<model>_<version>`) behind a single logical **collection alias** (`kb_chunks_active`).
3. **Atomic alias swap + compacted Kafka control plane**: `replay_and_switch_index` replays `kb.documents.sanitized.v1` from offset `0` into the new physical collection, executes `update_collection_aliases` (atomic delete + create alias in one operation), and publishes `KbIndexControlEvent` to compacted `kb.index.control.v1` so both `/v1/qa` query embedders and the live `StreamingIndexerStage` bind collection + model + dimensions together as an immutable `ActiveIndexSnapshot`.

## Alternatives Considered
- **pgvector:** Excellent when transactional joins with relational tables are mandatory, but swapping HNSW vector column dimensions (`vector(384)` -> `vector(768)`) cleanly without view/table lock contention requires heavier migration ceremony, whereas Qdrant natively supports multi-collection atomic aliases and embedded `:memory:` mode for sub-second hermetic unit/integration tests.

## Consequences
- **Positive:** Duplicate Kafka deliveries overwrite the exact UUIDv5 point in place (`delta = 0` points); blue/green model upgrades across incompatible vector dimensions cut over in `< 1 ms` alias swap latency with `0` failed queries.
- **Negative:** Adds a dedicated vector engine container alongside Kafka rather than reusing an existing relational database.
