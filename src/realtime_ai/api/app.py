"""FastAPI endpoints for real-time RAG QA, CDC ingestion, zero-downtime model switchover, and human approvals."""

from __future__ import annotations

import logging
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, Literal

from fastapi import FastAPI, Form, HTTPException, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field

from realtime_ai.api.state import ApiStreamMaterializer
from realtime_ai.api.ui import render_approvals_html
from realtime_ai.config import Settings
from realtime_ai.kafka.producer import EventProducer
from realtime_ai.kafka.topics import (
    TOPIC_HUMAN_APPROVALS,
    TOPIC_KB_RAW,
    TOPIC_REPLIES_APPROVED,
    TOPIC_TICKETS_CREATED,
)
from realtime_ai.metrics import DOCS_WATCHED_TOTAL, QA_LATENCY_SECONDS, QA_REQUESTS_TOTAL
from realtime_ai.pipeline.pii import redact_pii
from realtime_ai.pipeline.reembed import replay_and_switch_index
from realtime_ai.pipeline.vector_store import QdrantVectorRepository
from realtime_ai.providers.factory import create_resilient_provider
from realtime_ai.providers.resilient import ResilientAIProvider
from realtime_ai.schemas.models import (
    Citation,
    KbDocumentRawEvent,
    SupportReplyApprovedEvent,
    SupportTicketCreatedEvent,
    now_ms,
)
from realtime_ai.schemas.registry import SchemaRegistryCodec

logger = logging.getLogger(__name__)


class QARequest(BaseModel):
    question: str = Field(min_length=3, max_length=2000)
    top_k: int = Field(default=3, ge=1, le=10)
    retrieval_only: bool = False


class QAResponse(BaseModel):
    question: str
    answer: str
    citations: list[Citation]
    active_index: dict[str, Any]
    freshness: dict[str, Any]
    latency_ms: float


class DocumentUpsertRequest(BaseModel):
    doc_id: str = Field(min_length=2, max_length=120)
    title: str = Field(min_length=2, max_length=300)
    content: str = Field(min_length=1)
    op: Literal["create", "update"] = "update"
    source_uri: str = "cdc://api"


class ReembedRequest(BaseModel):
    new_embedding_model: str = Field(min_length=2)
    new_embedding_version: str = Field(min_length=1)
    new_dimensions: int | None = None
    reason: str = "blue_green_model_upgrade"


class TicketCreateRequest(BaseModel):
    ticket_id: str | None = None
    customer_id: str = Field(min_length=2)
    customer_email: str = "customer@example.com"
    account_tier: Literal["free", "pro", "enterprise"] = "pro"
    subject: str = Field(min_length=3)
    body: str = Field(min_length=5)


class ApprovalDecisionRequest(BaseModel):
    decision: Literal["approve", "reject"]
    actor: str = Field(default="support-lead@acme", min_length=2)
    comment: str | None = None
    final_reply: str | None = None


def create_app(
    *,
    settings: Settings | None = None,
    codec: SchemaRegistryCodec | None = None,
    producer: EventProducer | None = None,
    provider: ResilientAIProvider | None = None,
    vector_repo: QdrantVectorRepository | None = None,
    start_background_poller: bool = True,
) -> FastAPI:
    cfg = settings or Settings()
    sr_codec = codec or SchemaRegistryCodec(cfg.schema_registry_url, cfg.schemas_dir)
    evt_producer = producer or EventProducer(
        cfg.kafka_bootstrap_servers, sr_codec, client_id="krai-api-producer"
    )
    ai_provider = provider or create_resilient_provider(cfg, audit_producer=evt_producer)
    vec_repo = vector_repo or QdrantVectorRepository(
        url=cfg.qdrant_url,
        local_path=cfg.qdrant_local_path,
        active_alias=cfg.qdrant_active_alias,
    )
    materializer = ApiStreamMaterializer(
        settings=cfg,
        codec=sr_codec,
        vector_repo=vec_repo,
    )

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        materializer.sync_once(max_batches=50, poll_timeout=0.1)
        if start_background_poller:
            materializer.start_background()
        try:
            yield
        finally:
            materializer.stop()
            evt_producer.flush(timeout=5.0)

    app = FastAPI(
        title="Kafka Real-Time AI Pipeline",
        version="0.1.0",
        description="Streaming RAG with sub-second freshness and KIP-932 event-driven support agents on Apache Kafka 4.4",
        lifespan=lifespan,
    )
    app.state.settings = cfg
    app.state.codec = sr_codec
    app.state.producer = evt_producer
    app.state.provider = ai_provider
    app.state.vector_repo = vec_repo
    app.state.materializer = materializer

    def _active_index_dict() -> dict[str, Any]:
        snap = vec_repo.resolve_active_snapshot(
            default_model=cfg.embedding_model,
            default_version=cfg.embedding_version,
            default_dimensions=cfg.embedding_dimensions,
        )
        return {
            "alias_name": snap.alias_name,
            "collection_name": snap.collection_name,
            "embedding_model": snap.embedding_model,
            "embedding_version": snap.embedding_version,
            "dimensions": snap.dimensions,
        }

    @app.get("/healthz")
    def healthz() -> dict[str, Any]:
        materializer.sync_once(max_batches=5, poll_timeout=0.01)
        idx = _active_index_dict()
        return {
            "status": "ok",
            "consumer_mode": cfg.kafka_consumer_mode,
            "ai_provider": ai_provider.provider_name,
            "active_index": idx,
            "pending_approvals": len(materializer.list_approvals(status="pending")),
            "freshness": materializer.get_freshness_summary(),
        }

    @app.get("/metrics")
    def metrics() -> Response:
        return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.get("/", include_in_schema=False)
    def root_redirect() -> RedirectResponse:
        return RedirectResponse(url="/approvals", status_code=302)

    @app.post("/v1/qa", response_model=QAResponse)
    def answer_question(req: QARequest) -> QAResponse:
        t0 = time.perf_counter()
        materializer.sync_once(max_batches=5, poll_timeout=0.01)
        snap = vec_repo.resolve_active_snapshot(
            default_model=cfg.embedding_model,
            default_version=cfg.embedding_version,
            default_dimensions=cfg.embedding_dimensions,
        )
        try:
            q_sanitized = redact_pii(req.question).sanitized_text
            emb = ai_provider.embed(
                [q_sanitized],
                model=snap.embedding_model,
                stage="qa_embed",
                agent_name="qa-api",
                correlation_id="qa",
            )
            citations = vec_repo.search(
                collection_name=snap.collection_name,
                query_vector=emb.vectors[0],
                top_k=req.top_k,
            )
            if req.retrieval_only:
                answer_text = (
                    f"Retrieved {len(citations)} chunk(s) from {snap.collection_name} ({snap.embedding_version})."
                )
            elif not citations:
                answer_text = "No relevant articles were found in the active knowledge base."
            else:
                context_block = "\n\n".join(
                    f"[{c.doc_id}] ({c.title}, ver={c.embedding_version}, score={c.score}):\n{c.snippet}"
                    for c in citations
                )
                chat_res = ai_provider.chat(
                    system_prompt=(
                        "Answer the support question concisely using ONLY the supplied Knowledge Base "
                        "context. Always cite matching document IDs in square brackets like [KB-001]."
                    ),
                    user_prompt=f"QUESTION: {q_sanitized}\n\nCONTEXT:\n{context_block}",
                    stage="qa_chat",
                    agent_name="qa-api",
                    correlation_id="qa",
                )
                answer_text = redact_pii(chat_res.text).sanitized_text
                if f"[{citations[0].doc_id}]" not in answer_text:
                    answer_text = f"{answer_text} [{citations[0].doc_id}]"

            elapsed_s = time.perf_counter() - t0
            QA_LATENCY_SECONDS.observe(elapsed_s)
            QA_REQUESTS_TOTAL.labels(status="ok", embedding_version=snap.embedding_version).inc()
            return QAResponse(
                question=req.question,
                answer=answer_text,
                citations=citations,
                active_index={
                    "alias_name": snap.alias_name,
                    "collection_name": snap.collection_name,
                    "embedding_model": snap.embedding_model,
                    "embedding_version": snap.embedding_version,
                    "dimensions": snap.dimensions,
                },
                freshness=materializer.get_freshness_summary(),
                latency_ms=round(elapsed_s * 1000.0, 2),
            )
        except Exception as exc:
            QA_REQUESTS_TOTAL.labels(status="error", embedding_version=snap.embedding_version).inc()
            raise HTTPException(status_code=503, detail=f"QA pipeline error: {exc}") from exc

    @app.get("/v1/freshness")
    def get_freshness() -> dict[str, Any]:
        materializer.sync_once(max_batches=10, poll_timeout=0.02)
        return {
            "active_index": _active_index_dict(),
            "freshness": materializer.get_freshness_summary(),
        }

    @app.post("/v1/kb/documents", status_code=202)
    def publish_kb_document(req: DocumentUpsertRequest) -> dict[str, Any]:
        evt = KbDocumentRawEvent(
            doc_id=req.doc_id,
            op=req.op,
            title=req.title,
            content=req.content,
            source_uri=req.source_uri,
            source_ts_ms=now_ms(),
        )
        evt_producer.produce_event(TOPIC_KB_RAW, key=req.doc_id, payload=evt, flush=True)
        DOCS_WATCHED_TOTAL.labels(op=req.op).inc()
        return {
            "accepted": True,
            "topic": TOPIC_KB_RAW,
            "doc_id": evt.doc_id,
            "event_id": evt.event_id,
            "source_ts_ms": evt.source_ts_ms,
        }

    @app.delete("/v1/kb/documents/{doc_id}", status_code=202)
    def delete_kb_document(doc_id: str) -> dict[str, Any]:
        ts = now_ms()
        evt_producer.produce_tombstone(
            TOPIC_KB_RAW,
            key=doc_id,
            headers={"source_ts_ms": str(ts)},
            flush=True,
        )
        DOCS_WATCHED_TOTAL.labels(op="delete").inc()
        return {
            "accepted": True,
            "topic": TOPIC_KB_RAW,
            "doc_id": doc_id,
            "tombstone": True,
            "source_ts_ms": ts,
        }

    @app.post("/v1/kb/reembed")
    def trigger_reembed(req: ReembedRequest) -> dict[str, Any]:
        report = replay_and_switch_index(
            settings=cfg,
            codec=sr_codec,
            producer=evt_producer,
            provider=ai_provider,
            vector_repo=vec_repo,
            new_embedding_model=req.new_embedding_model,
            new_embedding_version=req.new_embedding_version,
            new_dimensions=req.new_dimensions,
            reason=req.reason,
        )
        materializer.sync_once(max_batches=10, poll_timeout=0.02)
        return {
            "switched": True,
            "target_collection": report.target_collection,
            "embedding_model": report.embedding_model,
            "embedding_version": report.embedding_version,
            "dimensions": report.dimensions,
            "records_replayed": report.records_replayed,
            "points_in_new_index": report.points_in_new_index,
            "replay_duration_ms": report.replay_duration_ms,
            "cutover_duration_ms": report.cutover_duration_ms,
        }

    @app.post("/v1/tickets", status_code=202)
    def submit_ticket(req: TicketCreateRequest) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "customer_id": req.customer_id,
            "customer_email": req.customer_email,
            "account_tier": req.account_tier,
            "subject": req.subject,
            "body": req.body,
        }
        if req.ticket_id:
            kwargs["ticket_id"] = req.ticket_id
        evt = SupportTicketCreatedEvent(**kwargs)
        evt_producer.produce_event(
            TOPIC_TICKETS_CREATED,
            key=evt.ticket_id,
            payload=evt,
            flush=True,
        )
        return {
            "accepted": True,
            "topic": TOPIC_TICKETS_CREATED,
            "ticket_id": evt.ticket_id,
            "created_ts_ms": evt.created_ts_ms,
        }

    @app.get("/v1/approvals")
    def list_approvals(status: Literal["pending", "approved", "rejected"] | None = None) -> dict[str, Any]:
        materializer.sync_once(max_batches=15, poll_timeout=0.02)
        items = materializer.list_approvals(status=status)
        return {
            "count": len(items),
            "items": [i.model_dump(mode="json") for i in items],
            "approved_replies_sent": len(materializer.list_approved_replies()),
        }

    @app.get("/v1/approvals/{ticket_id}")
    def get_ticket_approval(ticket_id: str) -> dict[str, Any]:
        materializer.sync_once(max_batches=15, poll_timeout=0.02)
        state = materializer.get_approval(ticket_id)
        if state is None:
            raise HTTPException(status_code=404, detail=f"Ticket {ticket_id} not yet in approval store")
        replies = materializer.list_approved_replies(ticket_id=ticket_id)
        return {
            "approval": state.model_dump(mode="json"),
            "sent_replies": [r.model_dump(mode="json") for r in replies],
        }

    def _apply_decision(ticket_id: str, req: ApprovalDecisionRequest) -> dict[str, Any]:
        materializer.sync_once(max_batches=15, poll_timeout=0.02)
        current = materializer.get_approval(ticket_id)
        if current is None:
            raise HTTPException(status_code=404, detail=f"Ticket '{ticket_id}' not found in human approval queue")

        # Idempotent guard: never emit duplicate customer replies if already approved/rejected
        if current.status in ("approved", "rejected"):
            return {
                "ticket_id": ticket_id,
                "status": current.status,
                "idempotent_noop": True,
                "decided_ts_ms": current.decided_ts_ms,
            }

        new_status: Literal["approved", "rejected"] = (
            "approved" if req.decision == "approve" else "rejected"
        )
        clean_final = (
            redact_pii(req.final_reply or current.proposed_reply).sanitized_text.strip()
            if new_status == "approved"
            else None
        )
        updated = current.model_copy(
            update={
                "status": new_status,
                "final_reply": clean_final,
                "human_actor": req.actor,
                "human_comment": req.comment,
                "decided_ts_ms": now_ms(),
            }
        )
        evt_producer.produce_event(
            TOPIC_HUMAN_APPROVALS,
            key=ticket_id,
            payload=updated,
            flush=False,
        )

        reply_evt: SupportReplyApprovedEvent | None = None
        if new_status == "approved":
            assert clean_final is not None
            reply_evt = SupportReplyApprovedEvent(
                ticket_id=ticket_id,
                draft_id=current.draft_id,
                customer_id=current.customer_id,
                subject=current.subject,
                final_reply=clean_final,
                citations=current.citations,
                approved_by=req.actor,
            )
            evt_producer.produce_event(
                TOPIC_REPLIES_APPROVED,
                key=ticket_id,
                payload=reply_evt,
                flush=False,
            )

        evt_producer.flush(timeout=10.0)
        materializer.record_local_approval_update(updated, reply_evt)
        return {
            "ticket_id": ticket_id,
            "status": updated.status,
            "reply_id": reply_evt.reply_id if reply_evt else None,
            "decided_ts_ms": updated.decided_ts_ms,
        }

    @app.post("/v1/approvals/{ticket_id}/decide")
    def decide_approval(ticket_id: str, req: ApprovalDecisionRequest) -> dict[str, Any]:
        return _apply_decision(ticket_id, req)

    @app.get("/approvals", response_class=HTMLResponse)
    def approvals_web_ui() -> HTMLResponse:
        materializer.sync_once(max_batches=15, poll_timeout=0.02)
        pending = materializer.list_approvals(status="pending")
        decided = [
            item
            for item in materializer.list_approvals()
            if item.status in ("approved", "rejected")
        ]
        snap = vec_repo.resolve_active_snapshot(
            default_model=cfg.embedding_model,
            default_version=cfg.embedding_version,
            default_dimensions=cfg.embedding_dimensions,
        )
        html = render_approvals_html(
            pending=pending,
            decided=decided,
            active_index=snap,
            freshness=materializer.get_freshness_summary(),
            consumer_mode=cfg.kafka_consumer_mode,
        )
        return HTMLResponse(content=html)

    @app.post("/approvals/{ticket_id}/form", include_in_schema=False)
    def approvals_form_submit(
        ticket_id: str,
        decision: Literal["approve", "reject"] = Form(...),
        actor: str = Form("support-lead@acme"),
        comment: str = Form(""),
        final_reply: str = Form(""),
    ) -> RedirectResponse:
        _apply_decision(
            ticket_id,
            ApprovalDecisionRequest(
                decision=decision,
                actor=actor,
                comment=comment or None,
                final_reply=final_reply or None,
            ),
        )
        return RedirectResponse(url="/approvals", status_code=303)

    @app.get("/v1/audit/recent")
    def recent_audit_events(limit: int = 40) -> dict[str, Any]:
        materializer.sync_once(max_batches=15, poll_timeout=0.02)
        events = materializer.list_recent_audit(limit=min(100, max(1, limit)))
        return {
            "count": len(events),
            "retention_days": cfg.audit_retention_days,
            "events": [e.model_dump(mode="json") for e in events],
        }

    return app
