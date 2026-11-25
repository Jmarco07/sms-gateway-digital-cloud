import type { Message, MessageDetail, MessageQuery, Quote, TenantsResponse } from "./types";

export class ApiFailure extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  let res: Response;
  try {
    res = await fetch(`/api${path}`, init);
  } catch {
    throw new ApiFailure(0, "Cannot reach the gateway. Is it running?");
  }
  if (!res.ok) {
    let message = `${res.status} ${res.statusText}`;
    try {
      const body = await res.json();
      const err = body?.error;
      if (err?.message) {
        const details = Array.isArray(err.details)
          ? err.details.map((d: { field: string; message: string }) => `${d.field}: ${d.message}`).join("; ")
          : "";
        message = details ? `${err.message} (${details})` : err.message;
      }
    } catch {
      /* keep the generic message */
    }
    throw new ApiFailure(res.status, message);
  }
  return (await res.json()) as T;
}

function qs(params: Record<string, string | number | undefined>): string {
  const q = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== "") q.set(key, String(value));
  }
  const s = q.toString();
  return s ? `?${s}` : "";
}

export const api = {
  tenants: () => request<TenantsResponse>("/tenants"),
  messages: (query: MessageQuery) =>
    request<{ items: Message[] }>(`/messages${qs({ ...query })}`),
  message: (id: string) => request<MessageDetail>(`/messages/${id}`),
  quote: (to: string, text: string) => request<Quote>(`/quote${qs({ to, text })}`),
  send: (tenantId: string, body: { to: string; from: string; text: string; client_ref?: string }) =>
    request<{ id: string }>(`/tenants/${tenantId}/messages`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),
};
