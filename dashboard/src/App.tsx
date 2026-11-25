import { useState } from "react";
import { api } from "./api";
import { useAsync, useDebouncedCallback, useHashRoute, useLiveEvents } from "./hooks";
import { MessageDetailView } from "./MessageDetail";
import { MessageList, type Filters } from "./MessageList";
import { SendForm } from "./SendForm";
import { TenantCards } from "./TenantCards";

const EMPTY_FILTERS: Filters = { tenantId: "", status: "", from: "", to: "" };

export default function App() {
  const route = useHashRoute();
  const detail = route.match(/^#\/messages\/([0-9a-f-]{36})$/i);

  const tenants = useAsync(() => api.tenants(), []);
  const [filters, setFilters] = useState<Filters>(EMPTY_FILTERS);

  // balances and counters follow the live events, at most once a second
  const reloadTenants = useDebouncedCallback(tenants.reload, 1000);
  useLiveEvents(() => reloadTenants());

  return (
    <main>
      <header className="top">
        <h1>
          <a href="#/">SMS Gateway</a>
        </h1>
        <span className="muted">internal dashboard</span>
      </header>

      {detail ? (
        <MessageDetailView id={detail[1]} />
      ) : (
        <>
          <TenantCards
            tenants={tenants}
            onPick={(tenantId, status) => setFilters({ ...EMPTY_FILTERS, tenantId, status })}
          />
          <SendForm tenants={tenants.data?.items ?? []} />
          <MessageList tenants={tenants.data?.items ?? []} filters={filters} onChange={setFilters} />
        </>
      )}
    </main>
  );
}
