import { useEffect, useMemo, useState } from "react";
import { getDlq, getHealth, listDlq, replayDlq } from "./api";

function formatTime(value) {
  if (!value) return "-";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return String(value);
  return d.toLocaleString();
}

function prettyJson(value) {
  if (value == null) return "-";
  if (typeof value === "string") {
    try {
      return JSON.stringify(JSON.parse(value), null, 2);
    } catch {
      return value;
    }
  }
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

function parseJsonMaybe(value) {
  if (!value) return {};
  if (typeof value === "object") return value;
  try {
    return JSON.parse(value);
  } catch {
    return {};
  }
}

function Badge({ children, tone = "neutral" }) {
  return <span className={`badge badge-${tone}`}>{children}</span>;
}

function StatCard({ label, value, hint }) {
  return (
    <div className="stat-card">
      <div className="stat-label">{label}</div>
      <div className="stat-value">{value}</div>
      {hint ? <div className="stat-hint">{hint}</div> : null}
    </div>
  );
}

export default function App() {
  const [items, setItems] = useState([]);
  const [selectedId, setSelectedId] = useState("");
  const [selectedItem, setSelectedItem] = useState(null);
  const [health, setHealth] = useState("checking");
  const [loading, setLoading] = useState(true);
  const [detailLoading, setDetailLoading] = useState(false);
  const [error, setError] = useState("");
  const [search, setSearch] = useState("");
  const [replayBy, setReplayBy] = useState("operator");
  const [replayNotes, setReplayNotes] = useState("");
  const [replayStatus, setReplayStatus] = useState("");
  const [replaying, setReplaying] = useState(false);
  const [lastRefresh, setLastRefresh] = useState(null);

  async function refresh() {
    setLoading(true);
    setError("");
    setReplayStatus("");

    try {
      const [healthRes, listRes] = await Promise.all([
        getHealth().catch(() => null),
        listDlq({ limit: 200, offset: 0 }),
      ]);

      setHealth(healthRes?.status || "unknown");

      const nextItems = listRes?.items || [];
      setItems(nextItems);
      setLastRefresh(new Date().toISOString());

      if (!selectedId && nextItems.length > 0) {
        setSelectedId(nextItems[0].event_id);
      }
    } catch (err) {
      setError(err.message || "Failed to load dashboard");
    } finally {
      setLoading(false);
    }
  }

  async function loadSelected(eventId) {
    if (!eventId) return;
    setDetailLoading(true);
    try {
      const item = await getDlq(eventId);
      setSelectedItem(item);
    } catch (err) {
      setError(err.message || "Failed to load DLQ item");
    } finally {
      setDetailLoading(false);
    }
  }

  useEffect(() => {
    refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    if (selectedId) {
      loadSelected(selectedId);
    } else {
      setSelectedItem(null);
    }
  }, [selectedId]);

  const filteredItems = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return items;

    return items.filter((item) => {
      const haystack = [
        item.event_id,
        item.aggregate_type,
        item.aggregate_id,
        item.event_type,
        item.last_error,
        item.original_status,
      ]
        .filter(Boolean)
        .join(" ")
        .toLowerCase();

      return haystack.includes(q);
    });
  }, [items, search]);

  const summary = useMemo(() => {
    const total = items.length;
    const replayed = items.filter((x) => x.replayed_at).length;
    const pendingReplay = items.filter((x) => !x.replayed_at).length;
    const retries = items.reduce(
      (acc, x) => acc + (Number(x.retry_count) || 0),
      0,
    );
    return { total, replayed, pendingReplay, retries };
  }, [items]);

  const detail =
    selectedItem || items.find((x) => x.event_id === selectedId) || null;
  const replayedAt = detail?.replayed_at || null;
  const payloadObj = parseJsonMaybe(detail?.payload);
  const metadataObj = parseJsonMaybe(detail?.metadata);
  const destinationObj = parseJsonMaybe(detail?.destination);

  async function handleReplay() {
    if (!detail) return;
    setReplaying(true);
    setReplayStatus("");

    try {
      const res = await replayDlq(detail.event_id, {
        replayed_by: replayBy,
        notes: replayNotes,
      });

      setReplayStatus(`Replayed into outbox as ${res.new_outbox_event_id}`);
      await refresh();
      await loadSelected(detail.event_id);
    } catch (err) {
      setReplayStatus(err.message || "Replay failed");
    } finally {
      setReplaying(false);
    }
  }

  return (
    <div className="app-shell">
      <header className="topbar">
        <div>
          <div className="eyebrow">Rhombus</div>
          <h1>Replay Dashboard</h1>
          <p className="subtitle">
            Inspect DLQ events, review payloads, and replay safely back into the
            outbox.
          </p>
        </div>

        <div className="topbar-meta">
          <Badge tone={health === "ok" ? "success" : "warning"}>
            API: {health}
          </Badge>
          <button className="button" onClick={refresh} disabled={loading}>
            {loading ? "Refreshing..." : "Refresh"}
          </button>
        </div>
      </header>

      <section className="stats-grid">
        <StatCard
          label="DLQ events"
          value={summary.total}
          hint="Total failed events"
        />
        <StatCard
          label="Replayable"
          value={summary.pendingReplay}
          hint="Not yet replayed"
        />
        <StatCard
          label="Replayed"
          value={summary.replayed}
          hint="Marked in audit trail"
        />
        <StatCard
          label="Retry count"
          value={summary.retries}
          hint="Sum of retries across DLQ"
        />
      </section>

      <section className="toolbar">
        <input
          className="search"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder="Search by event id, aggregate, event type, or error..."
        />
        <div className="toolbar-meta">
          {lastRefresh ? (
            <span>Last refresh: {formatTime(lastRefresh)}</span>
          ) : null}
          <span>{filteredItems.length} shown</span>
        </div>
      </section>

      {error ? <div className="alert alert-error">{error}</div> : null}
      {replayStatus ? (
        <div className="alert alert-info">{replayStatus}</div>
      ) : null}

      <main className="layout">
        <aside className="panel list-panel">
          <div className="panel-header">
            <h2>Failed events</h2>
            <span>{filteredItems.length}</span>
          </div>

          <div className="event-list">
            {filteredItems.map((item) => {
              const active = item.event_id === selectedId;
              return (
                <button
                  key={item.event_id}
                  className={`event-row ${active ? "active" : ""}`}
                  onClick={() => setSelectedId(item.event_id)}
                >
                  <div className="event-row-top">
                    <strong>{item.event_type}</strong>
                    <Badge tone={item.replayed_at ? "success" : "danger"}>
                      {item.replayed_at ? "replayed" : "dlq"}
                    </Badge>
                  </div>
                  <div className="event-row-meta">
                    <span>
                      {item.aggregate_type}:{item.aggregate_id}
                    </span>
                    <span>Retries: {item.retry_count}</span>
                  </div>
                  <div className="event-row-error">{item.last_error}</div>
                  <div className="event-row-time">
                    {formatTime(item.moved_to_dlq_at)}
                  </div>
                </button>
              );
            })}

            {!loading && filteredItems.length === 0 ? (
              <div className="empty-state">
                No DLQ events match your search.
              </div>
            ) : null}
          </div>
        </aside>

        <section className="panel detail-panel">
          <div className="panel-header">
            <h2>Event details</h2>
            {detail ? <span>{detail.event_id}</span> : <span>-</span>}
          </div>

          {detailLoading ? (
            <div className="empty-state">Loading details...</div>
          ) : detail ? (
            <>
              <div className="detail-grid">
                <div className="detail-card">
                  <div className="detail-label">Aggregate</div>
                  <div className="detail-value">
                    {detail.aggregate_type}:{detail.aggregate_id}
                  </div>
                </div>
                <div className="detail-card">
                  <div className="detail-label">Event type</div>
                  <div className="detail-value">{detail.event_type}</div>
                </div>
                <div className="detail-card">
                  <div className="detail-label">Retry count</div>
                  <div className="detail-value">{detail.retry_count}</div>
                </div>
                <div className="detail-card">
                  <div className="detail-label">Moved to DLQ</div>
                  <div className="detail-value">
                    {formatTime(detail.moved_to_dlq_at)}
                  </div>
                </div>
              </div>

              <div className="detail-block">
                <div className="detail-label">Last error</div>
                <pre>{detail.last_error}</pre>
              </div>

              <div className="detail-block">
                <div className="detail-label">Payload</div>
                <pre>{prettyJson(payloadObj)}</pre>
              </div>

              <div className="detail-block">
                <div className="detail-label">Metadata</div>
                <pre>{prettyJson(metadataObj)}</pre>
              </div>

              <div className="detail-block">
                <div className="detail-label">Destination</div>
                <pre>{prettyJson(destinationObj)}</pre>
              </div>

              <div className="detail-grid">
                <div className="detail-card">
                  <div className="detail-label">Original status</div>
                  <div className="detail-value">{detail.original_status}</div>
                </div>
                <div className="detail-card">
                  <div className="detail-label">Replayed at</div>
                  <div className="detail-value">
                    {replayedAt ? formatTime(replayedAt) : "Not replayed"}
                  </div>
                </div>
              </div>

              <div className="replay-box">
                <h3>Replay event</h3>
                <div className="form-grid">
                  <label>
                    Replayed by
                    <input
                      value={replayBy}
                      onChange={(e) => setReplayBy(e.target.value)}
                    />
                  </label>
                  <label>
                    Notes
                    <input
                      value={replayNotes}
                      onChange={(e) => setReplayNotes(e.target.value)}
                    />
                  </label>
                </div>
                <button
                  className="button primary"
                  onClick={handleReplay}
                  disabled={replaying}
                >
                  {replaying ? "Replaying..." : "Replay into outbox"}
                </button>
              </div>
            </>
          ) : (
            <div className="empty-state">Select an event to inspect it.</div>
          )}
        </section>
      </main>
    </div>
  );
}
