"""Server-rendered HTML Human-in-the-Loop Approval Console using Jinja2."""

from __future__ import annotations

from typing import Any

from jinja2 import Environment, select_autoescape

_APPROVAL_TEMPLATE_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Kafka Real-Time AI — Support Human Approval Queue</title>
  <style>
    :root {
      --bg: #0f172a;
      --panel: #1e293b;
      --border: #334155;
      --text: #f8fafc;
      --muted: #94a3b8;
      --accent: #38bdf8;
      --green: #10b981;
      --red: #ef4444;
      --amber: #f59e0b;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
      background: var(--bg);
      color: var(--text);
      line-height: 1.5;
    }
    header {
      background: #0b1120;
      border-bottom: 1px solid var(--border);
      padding: 1.1rem 2rem;
      display: flex;
      flex-wrap: wrap;
      justify-content: space-between;
      align-items: center;
      gap: 1rem;
    }
    header h1 { margin: 0; font-size: 1.25rem; letter-spacing: -0.01em; }
    .stats { display: flex; gap: 0.75rem; flex-wrap: wrap; font-size: 0.85rem; }
    .pill {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 999px;
      padding: 0.28rem 0.75rem;
      color: var(--muted);
    }
    .pill strong { color: var(--text); }
    main { max-width: 1120px; margin: 1.75rem auto; padding: 0 1.5rem 3rem; }
    h2 { font-size: 1.1rem; margin: 1.75rem 0 0.9rem; color: var(--accent); }
    .card {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 1.25rem;
      margin-bottom: 1.1rem;
    }
    .card-head {
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      flex-wrap: wrap;
      gap: 0.5rem;
      margin-bottom: 0.65rem;
    }
    .badge {
      display: inline-block;
      padding: 0.15rem 0.55rem;
      border-radius: 6px;
      font-size: 0.75rem;
      font-weight: 600;
      text-transform: uppercase;
    }
    .urg-p1_critical { background: rgba(239,68,68,0.2); color: #fca5a5; border: 1px solid #ef4444; }
    .urg-p2_high { background: rgba(245,158,11,0.2); color: #fcd34d; border: 1px solid #f59e0b; }
    .urg-p3_normal, .urg-p4_low { background: rgba(56,189,248,0.16); color: #7dd3fc; border: 1px solid #38bdf8; }
    .meta { color: var(--muted); font-size: 0.84rem; margin-bottom: 0.75rem; }
    .citations {
      display: flex;
      gap: 0.5rem;
      flex-wrap: wrap;
      margin-bottom: 0.85rem;
    }
    .cite {
      font-size: 0.78rem;
      background: #0f172a;
      border: 1px solid var(--border);
      padding: 0.25rem 0.6rem;
      border-radius: 6px;
      color: #cbd5e1;
    }
    textarea, input[type="text"] {
      width: 100%;
      background: #0f172a;
      color: var(--text);
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 0.6rem 0.75rem;
      font-family: inherit;
      font-size: 0.9rem;
      margin-bottom: 0.7rem;
    }
    textarea { min-height: 115px; resize: vertical; }
    .form-row { display: grid; grid-template-columns: 1fr 2fr; gap: 0.75rem; }
    .actions { display: flex; gap: 0.75rem; margin-top: 0.25rem; }
    button {
      cursor: pointer;
      border: none;
      border-radius: 6px;
      padding: 0.55rem 1.1rem;
      font-weight: 600;
      font-size: 0.88rem;
    }
    .btn-approve { background: var(--green); color: #052e16; }
    .btn-reject { background: var(--red); color: #fff; }
    .empty {
      color: var(--muted);
      padding: 1.5rem;
      text-align: center;
      border: 1px dashed var(--border);
      border-radius: 10px;
    }
    table { width: 100%; border-collapse: collapse; font-size: 0.86rem; }
    th, td { text-align: left; padding: 0.6rem 0.75rem; border-bottom: 1px solid var(--border); }
    th { color: var(--muted); font-weight: 600; }
    .status-approved { color: var(--green); font-weight: 600; }
    .status-rejected { color: var(--red); font-weight: 600; }
  </style>
</head>
<body>
  <header>
    <div>
      <h1>Kafka Real-Time AI &middot; Support Human Approval Console</h1>
    </div>
    <div class="stats">
      <span class="pill">Active Index: <strong>{{ active_index.collection_name }}</strong> ({{ active_index.embedding_model }} / {{ active_index.embedding_version }})</span>
      <span class="pill">KB Freshness p50: <strong>{{ freshness.p50_freshness_ms if freshness.p50_freshness_ms is not none else "—" }} ms</strong></span>
      <span class="pill">Consumer Mode: <strong>{{ consumer_mode }}</strong></span>
      <span class="pill">Pending Drafts: <strong>{{ pending | length }}</strong></span>
    </div>
  </header>

  <main>
    <h2>Pending AI Replies Awaiting Human Sign-Off ({{ pending | length }})</h2>
    {% if not pending %}
      <div class="empty">No pending drafts right now. Submit a ticket to <code>POST /v1/tickets</code> to trigger Triage &rarr; Tools &rarr; Drafter &rarr; Reviewer.</div>
    {% endif %}

    {% for item in pending %}
      <div class="card" id="ticket-{{ item.ticket_id }}">
        <div class="card-head">
          <div>
            <strong>{{ item.ticket_id }}</strong> &mdash; {{ item.subject }}
          </div>
          <div>
            <span class="badge urg-{{ item.urgency }}">{{ item.urgency }}</span>
            <span class="badge">{{ item.category }}</span>
          </div>
        </div>
        <div class="meta">
          Customer: <strong>{{ item.customer_id }}</strong> &middot;
          Draft: <code>{{ item.draft_id }}</code> (rev {{ item.revision }}) &middot;
          Reviewer QA: {{ item.reviewer_notes }}
        </div>

        {% if item.citations %}
          <div class="citations">
            {% for c in item.citations %}
              <span class="cite">[{{ c.doc_id }}] {{ c.title }} (score={{ c.score }}, {{ c.embedding_version }})</span>
            {% endfor %}
          </div>
        {% endif %}

        <form method="post" action="/approvals/{{ item.ticket_id }}/form">
          <label class="meta">Editable Customer Reply (PII-sanitized):</label>
          <textarea name="final_reply" required>{{ item.proposed_reply }}</textarea>
          <div class="form-row">
            <input type="text" name="actor" value="support-lead@acme" placeholder="Approver ID" required />
            <input type="text" name="comment" value="" placeholder="Optional review note (e.g., Verified SLA & KB citation)" />
          </div>
          <div class="actions">
            <button class="btn-approve" type="submit" name="decision" value="approve">Approve &amp; Publish Reply</button>
            <button class="btn-reject" type="submit" name="decision" value="reject">Reject Draft</button>
          </div>
        </form>
      </div>
    {% endfor %}

    <h2>Completed Human Decisions ({{ decided | length }})</h2>
    {% if not decided %}
      <div class="empty">No approved or rejected replies yet.</div>
    {% else %}
      <div class="card">
        <table>
          <thead>
            <tr>
              <th>Ticket</th>
              <th>Subject</th>
              <th>Decision</th>
              <th>Reviewer</th>
              <th>Notes</th>
            </tr>
          </thead>
          <tbody>
            {% for d in decided %}
              <tr>
                <td><code>{{ d.ticket_id }}</code></td>
                <td>{{ d.subject }}</td>
                <td class="status-{{ d.status }}">{{ d.status | upper }}</td>
                <td>{{ d.human_actor or "—" }}</td>
                <td>{{ d.human_comment or "—" }}</td>
              </tr>
            {% endfor %}
          </tbody>
        </table>
      </div>
    {% endif %}
  </main>
</body>
</html>
"""

_JINJA_ENV = Environment(autoescape=select_autoescape(["html", "xml"]))
_COMPILED_TEMPLATE = _JINJA_ENV.from_string(_APPROVAL_TEMPLATE_HTML)


def render_approvals_html(
    *,
    pending: list[Any],
    decided: list[Any],
    active_index: Any,
    freshness: dict[str, Any],
    consumer_mode: str,
) -> str:
    return _COMPILED_TEMPLATE.render(
        pending=pending,
        decided=decided,
        active_index=active_index,
        freshness=freshness,
        consumer_mode=consumer_mode,
    )
