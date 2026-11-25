export function money(micros: number | null | undefined): string {
  if (micros === null || micros === undefined) return "-";
  return (micros / 1_000_000).toLocaleString(undefined, {
    minimumFractionDigits: 4,
    maximumFractionDigits: 4,
  });
}

export function time(iso: string | null | undefined): string {
  if (!iso) return "-";
  const d = new Date(iso);
  return `${d.toLocaleDateString()} ${d.toLocaleTimeString(undefined, { hour12: false })}.${String(
    d.getMilliseconds(),
  ).padStart(3, "0")}`;
}

/** Milliseconds between two ISO timestamps, as text. */
export function elapsed(from: string, to: string): string {
  const ms = new Date(to).getTime() - new Date(from).getTime();
  return ms >= 1000 ? `+${(ms / 1000).toFixed(2)} s` : `+${ms} ms`;
}

/** yyyy-mm-dd from a <input type="date"> to an ISO instant (local midnight). */
export function dayStart(value: string, addDays = 0): string | undefined {
  if (!value) return undefined;
  const [y, m, d] = value.split("-").map(Number);
  return new Date(y, m - 1, d + addDays).toISOString();
}
