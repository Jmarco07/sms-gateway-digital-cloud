import { Empty, ErrorBox, Panel, Spinner } from "./components";
import { money } from "./format";
import type { AsyncState } from "./hooks";
import { STATUSES, type Status, type TenantsResponse } from "./types";

interface Props {
  tenants: AsyncState<TenantsResponse>;
  onPick: (tenantId: string, status: Status | "") => void;
}

export function TenantCards({ tenants, onPick }: Props) {
  const { data, error, loading, reload } = tenants;
  return (
    <Panel
      title="Tenants"
      right={data && <span className="muted">queue depth: {data.queue_depth}</span>}
    >
      {loading && !data && <Spinner label="Loading tenants" />}
      {error && <ErrorBox message={error} onRetry={reload} />}
      {data && data.items.length === 0 && <Empty>No tenants yet. Run the migration to load the seed data.</Empty>}
      {data && data.items.length > 0 && (
        <div className="cards">
          {data.items.map((t) => (
            <article className="card" key={t.id}>
              <div className="card-head">
                <h3>{t.name}</h3>
                <span
                  className={t.balance_consistent ? "pill pill-ok" : "pill pill-bad"}
                  title="balance = opening balance - sum of the ledger"
                >
                  {t.balance_consistent ? "ledger ✓" : "ledger MISMATCH"}
                </span>
              </div>
              <div className="balance">{money(t.balance_micros)}</div>
              <div className="muted small">opening {money(t.opening_balance_micros)}</div>
              <div className="chips">
                {STATUSES.map((s) => (
                  <button
                    key={s}
                    className={`chip badge-${s}`}
                    data-zero={t.counts[s] === 0}
                    onClick={() => onPick(t.id, s)}
                    title={`Show ${s} messages of ${t.name}`}
                  >
                    <span>{s}</span>
                    <strong>{t.counts[s].toLocaleString()}</strong>
                  </button>
                ))}
              </div>
            </article>
          ))}
        </div>
      )}
    </Panel>
  );
}
