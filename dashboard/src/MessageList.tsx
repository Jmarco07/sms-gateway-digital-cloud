import { useEffect, useState } from "react";
import { api } from "./api";
import { Empty, ErrorBox, Panel, Spinner, StatusBadge } from "./components";
import { dayStart, time } from "./format";
import { useAsync, useDebouncedCallback, useLiveEvents } from "./hooks";
import { STATUSES, type Message, type Tenant } from "./types";

export interface Filters {
  tenantId: string;
  status: string;
  from: string;
  to: string;
}

const PAGE = 50;

interface Props {
  tenants: Tenant[];
  filters: Filters;
  onChange: (f: Filters) => void;
}

export function MessageList({ tenants, filters, onChange }: Props) {
  const [pages, setPages] = useState(1);
  const [rows, setRows] = useState<Message[]>([]);

  // a new filter starts again from the first page
  useEffect(() => setPages(1), [filters.tenantId, filters.status, filters.from, filters.to]);

  const list = useAsync(
    () =>
      api.messages({
        tenant_id: filters.tenantId || undefined,
        status: filters.status || undefined,
        created_from: dayStart(filters.from),
        created_to: dayStart(filters.to, 1),
        limit: PAGE * pages,
        offset: 0,
      }),
    [filters.tenantId, filters.status, filters.from, filters.to, pages],
  );
  useEffect(() => {
    if (list.data) setRows(list.data.items);
  }, [list.data]);

  // Live updates: change the status in place right away, then reload quietly so that new
  // messages appear and rows that no longer match the filter disappear.
  const reloadSoon = useDebouncedCallback(list.reload, 800);
  const connected = useLiveEvents((e) => {
    setRows((prev) => prev.map((r) => (r.id === e.message_id ? { ...r, status: e.status } : r)));
    reloadSoon();
  });

  const set = (patch: Partial<Filters>) => onChange({ ...filters, ...patch });
  const canLoadMore = list.data !== undefined && list.data.items.length === PAGE * pages;

  return (
    <Panel
      title="Messages"
      right={<span className={connected ? "live live-on" : "live"}>{connected ? "● live" : "○ reconnecting…"}</span>}
    >
      <div className="filters">
        <label>
          Tenant
          <select value={filters.tenantId} onChange={(e) => set({ tenantId: e.target.value })}>
            <option value="">All tenants</option>
            {tenants.map((t) => (
              <option key={t.id} value={t.id}>
                {t.name}
              </option>
            ))}
          </select>
        </label>
        <label>
          Status
          <select value={filters.status} onChange={(e) => set({ status: e.target.value })}>
            <option value="">Any status</option>
            {STATUSES.map((s) => (
              <option key={s}>{s}</option>
            ))}
          </select>
        </label>
        <label>
          From
          <input type="date" value={filters.from} onChange={(e) => set({ from: e.target.value })} />
        </label>
        <label>
          To
          <input type="date" value={filters.to} onChange={(e) => set({ to: e.target.value })} />
        </label>
        <button className="btn" onClick={() => onChange({ tenantId: "", status: "", from: "", to: "" })}>
          Clear filters
        </button>
      </div>

      {list.loading && rows.length === 0 && <Spinner label="Loading messages" />}
      {list.error && <ErrorBox message={list.error} onRetry={list.reload} />}
      {!list.loading && !list.error && rows.length === 0 && (
        <Empty>No messages match these filters.</Empty>
      )}

      {rows.length > 0 && (
        <>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Created</th>
                  <th>Tenant</th>
                  <th>To</th>
                  <th>Text</th>
                  <th>Enc / seg</th>
                  <th>Status</th>
                  <th>Tries</th>
                  <th>ID</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((m) => (
                  <tr key={m.id}>
                    <td className="nowrap">{time(m.created_at)}</td>
                    <td>{m.tenant_name}</td>
                    <td className="nowrap">{m.to}</td>
                    <td className="text-cell" title={m.text}>
                      {m.text}
                    </td>
                    <td className="nowrap">
                      {m.encoding} / {m.segments}
                    </td>
                    <td>
                      <StatusBadge status={m.status} />
                    </td>
                    <td>{m.attempts}</td>
                    <td>
                      <a href={`#/messages/${m.id}`}>{m.id.slice(0, 8)}</a>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="list-foot">
            <span className="muted">{rows.length.toLocaleString()} shown, newest first</span>
            {canLoadMore && (
              <button className="btn" onClick={() => setPages((p) => p + 1)} disabled={list.loading}>
                Load more
              </button>
            )}
          </div>
        </>
      )}
    </Panel>
  );
}
