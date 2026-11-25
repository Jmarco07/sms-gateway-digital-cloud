import { useEffect, useState, type FormEvent } from "react";
import { api } from "./api";
import { Panel } from "./components";
import { money } from "./format";
import { useAsync, useDebouncedValue } from "./hooks";
import type { Tenant } from "./types";

export function SendForm({ tenants }: { tenants: Tenant[] }) {
  const [tenantId, setTenantId] = useState("");
  const [to, setTo] = useState("+8801711000001");
  const [from, setFrom] = useState("ACME");
  const [text, setText] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string>();
  const [sentId, setSentId] = useState<string>();

  useEffect(() => {
    if (!tenantId && tenants.length > 0) setTenantId(tenants[0].id);
  }, [tenants, tenantId]);

  // Live preview: asked from the server so the dashboard can never disagree with the billing code.
  const dTo = useDebouncedValue(to, 250);
  const dText = useDebouncedValue(text, 250);
  const quote = useAsync(() => api.quote(dTo, dText), [dTo, dText]);
  const q = quote.data;
  const stale = to !== dTo || text !== dText || quote.loading;

  const canSend =
    !busy && !!tenantId && text.trim() !== "" && !!q && q.problems.length === 0 && !stale;

  async function submit(e: FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError(undefined);
    setSentId(undefined);
    try {
      const res = await api.send(tenantId, { to, from, text });
      setSentId(res.id);
      setText("");
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  }

  return (
    <Panel title="Send a test message">
      <form className="send" onSubmit={submit}>
        <div className="row">
          <label>
            Tenant
            <select value={tenantId} onChange={(e) => setTenantId(e.target.value)}>
              {tenants.map((t) => (
                <option key={t.id} value={t.id}>
                  {t.name}
                </option>
              ))}
            </select>
          </label>
          <label>
            To (E.164)
            <input value={to} onChange={(e) => setTo(e.target.value.trim())} placeholder="+8801711000001" />
          </label>
          <label>
            Sender id
            <input value={from} onChange={(e) => setFrom(e.target.value.trim())} maxLength={16} />
          </label>
        </div>
        <label>
          Text
          <textarea
            rows={3}
            value={text}
            onChange={(e) => setText(e.target.value)}
            placeholder="Type a message. Try { } [ ] € or Bangla text or an emoji."
          />
        </label>

        <div className={`quote${stale ? " quote-stale" : ""}`} aria-live="polite">
          <span>
            Encoding <strong>{q?.encoding ?? "-"}</strong>
          </span>
          <span>
            Length <strong>{q?.length ?? 0}</strong>
          </span>
          <span>
            Segments <strong>{q?.segments ?? 0}</strong>
          </span>
          <span>
            Price / segment <strong>{money(q?.price_per_segment_micros)}</strong>
            {q?.country ? ` (${q.country})` : ""}
          </span>
          <span>
            Total <strong>{money(q?.price_micros)}</strong>
          </span>
        </div>
        {q?.problems.map((p) => (
          <div className="field-error" key={p}>
            {p}
          </div>
        ))}
        {quote.error && <div className="field-error">Preview unavailable: {quote.error}</div>}

        <div className="row actions">
          <button className="btn btn-primary" type="submit" disabled={!canSend}>
            {busy ? "Sending…" : "Send message"}
          </button>
          {sentId && (
            <span className="ok">
              Accepted. <a href={`#/messages/${sentId}`}>Open message {sentId.slice(0, 8)}</a>
            </span>
          )}
          {error && <span className="field-error">{error}</span>}
        </div>
      </form>
    </Panel>
  );
}
