export type Status =
  | "ACCEPTED"
  | "QUEUED"
  | "SUBMITTED"
  | "DELIVERED"
  | "UNDELIVERED"
  | "EXPIRED"
  | "FAILED";

export type Counts = Record<Status, number>;

export interface Tenant {
  id: string;
  name: string;
  balance_micros: number;
  opening_balance_micros: number;
  ledger_total_micros: number;
  balance_consistent: boolean;
  counts: Counts;
}

export interface TenantsResponse {
  items: Tenant[];
  statuses: Status[];
  queue_depth: number;
}

export interface Message {
  id: string;
  tenant_id: string;
  tenant_name: string;
  client_ref: string | null;
  to: string;
  from: string;
  text: string;
  encoding: "GSM7" | "UCS2";
  segments: number;
  price_micros: number;
  status: Status;
  carrier_id: string | null;
  attempts: number;
  last_error: string | null;
  created_at: string;
  updated_at: string;
  submitted_at: string | null;
  final_at: string | null;
}

export interface MessageEvent {
  id: number;
  kind: "STATUS" | "ATTEMPT";
  status: Status;
  detail: Record<string, unknown>;
  created_at: string;
}

export interface MessageDetail extends Message {
  events: MessageEvent[];
}

export interface MessageQuery {
  tenant_id?: string;
  status?: string;
  created_from?: string;
  created_to?: string;
  limit: number;
  offset: number;
}

export interface Quote {
  encoding: "GSM7" | "UCS2" | null;
  length: number;
  segments: number;
  price_per_segment_micros: number | null;
  price_micros: number | null;
  country: string | null;
  problems: string[];
}

export interface LiveEvent {
  message_id: string;
  tenant_id: string;
  status: Status;
  at: string;
}

export const STATUSES: Status[] = [
  "ACCEPTED",
  "QUEUED",
  "SUBMITTED",
  "DELIVERED",
  "UNDELIVERED",
  "EXPIRED",
  "FAILED",
];
