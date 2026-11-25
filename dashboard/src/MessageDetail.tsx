import { api } from "./api";
import { Empty, ErrorBox, Panel, Spinner, StatusBadge } from "./components";
import { elapsed, money, time } from "./format";
import { useAsync, useDebouncedCallback, useLiveEvents } from "./hooks";

export function MessageDetailView({ id }: { id: string }) {
  const { data: m, error, loading, reload } = useAsync(() => api.message(id), [id]);
  const reloadSoon = useDebouncedCallback(reload, 300);
  useLiveEvents((e) => {
    if (e.message_id === id) reloadSoon();
  });

  return (
    <>
      <p>
        <a href="#/">← Back to the dashboard</a>
      </p>
      {loading && !m && <Spinner label="Loading message" />}
      {error && <ErrorBox message={error} onRetry={reload} />}
      {m && (
        <>
          <Panel title={`Message ${m.id}`} right={<StatusBadge status={m.status} />}>
            <dl className="facts">
              <dt>Tenant</dt>
              <dd>{m.tenant_name}</dd>
              <dt>To / from</dt>
              <dd>
                {m.to} / {m.from}
              </dd>
              <dt>Text</dt>
              <dd className="pre">{m.text}</dd>
              <dt>Encoding</dt>
              <dd>
                {m.encoding}, {m.segments} segment{m.segments === 1 ? "" : "s"}
              </dd>
              <dt>Price</dt>
              <dd>{money(m.price_micros)}</dd>
              <dt>Client ref</dt>
              <dd>{m.client_ref ?? "-"}</dd>
              <dt>Carrier id</dt>
              <dd>{m.carrier_id ?? "-"}</dd>
              <dt>Submit attempts</dt>
              <dd>{m.attempts}</dd>
              {m.last_error && (
                <>
                  <dt>Last error</dt>
                  <dd>{m.last_error}</dd>
                </>
              )}
            </dl>
          </Panel>

          <Panel title="Event history">
            {m.events.length === 0 ? (
              <Empty>No events recorded.</Empty>
            ) : (
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Time</th>
                      <th>Since accepted</th>
                      <th>Kind</th>
                      <th>Status</th>
                      <th>Detail</th>
                    </tr>
                  </thead>
                  <tbody>
                    {m.events.map((e) => (
                      <tr key={e.id} className={e.kind === "ATTEMPT" ? "row-attempt" : undefined}>
                        <td className="nowrap">{time(e.created_at)}</td>
                        <td className="nowrap">{elapsed(m.events[0].created_at, e.created_at)}</td>
                        <td>{e.kind === "STATUS" ? "status change" : "failed attempt"}</td>
                        <td>
                          <StatusBadge status={e.status} />
                        </td>
                        <td>
                          <code>{Object.keys(e.detail).length ? JSON.stringify(e.detail) : ""}</code>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </Panel>
        </>
      )}
    </>
  );
}
