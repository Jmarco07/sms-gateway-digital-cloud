import { useCallback, useEffect, useRef, useState, useSyncExternalStore } from "react";
import type { LiveEvent } from "./types";

// ---------------------------------------------------------------- async data
export interface AsyncState<T> {
  data: T | undefined;
  error: string | undefined;
  loading: boolean;
  reload: () => void;
}

/** Runs `fn` when `deps` change (and on reload()). Stale answers are ignored.
 *  `silent` reloads keep the old data on screen instead of showing a spinner. */
export function useAsync<T>(fn: () => Promise<T>, deps: unknown[]): AsyncState<T> {
  const [data, setData] = useState<T>();
  const [error, setError] = useState<string>();
  const [loading, setLoading] = useState(true);
  const [tick, setTick] = useState(0);
  const latest = useRef(0);
  const silent = useRef(false);

  useEffect(() => {
    const id = ++latest.current;
    if (!silent.current) setLoading(true);
    silent.current = false;
    fn()
      .then((result) => {
        if (id !== latest.current) return;
        setData(result);
        setError(undefined);
      })
      .catch((e: unknown) => {
        if (id !== latest.current) return;
        setError(e instanceof Error ? e.message : String(e));
      })
      .finally(() => {
        if (id === latest.current) setLoading(false);
      });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [...deps, tick]);

  const reload = useCallback(() => {
    silent.current = true;
    setTick((t) => t + 1);
  }, []);
  return { data, error, loading, reload };
}

/** Returns a function that calls `fn` at most once per `ms`, after the last call. */
export function useDebouncedCallback(fn: () => void, ms: number): () => void {
  const ref = useRef(fn);
  ref.current = fn;
  const timer = useRef<number>();
  useEffect(() => () => window.clearTimeout(timer.current), []);
  return useCallback(() => {
    window.clearTimeout(timer.current);
    timer.current = window.setTimeout(() => ref.current(), ms);
  }, [ms]);
}

export function useDebouncedValue<T>(value: T, ms: number): T {
  const [v, setV] = useState(value);
  useEffect(() => {
    const t = window.setTimeout(() => setV(value), ms);
    return () => window.clearTimeout(t);
  }, [value, ms]);
  return v;
}

// ------------------------------------------------------------ live events (SSE)
// One EventSource shared by the whole page. The browser reconnects by itself.
type Handler = (e: LiveEvent) => void;
const handlers = new Set<Handler>();
const connectionListeners = new Set<() => void>();
let source: EventSource | null = null;
let connected = false;

function setConnected(value: boolean) {
  connected = value;
  connectionListeners.forEach((l) => l());
}

function ensureSource() {
  if (source) return;
  source = new EventSource("/api/stream");
  source.onopen = () => setConnected(true);
  source.onerror = () => setConnected(false);
  source.onmessage = (msg) => {
    try {
      const event = JSON.parse(msg.data) as LiveEvent;
      handlers.forEach((h) => h(event));
    } catch {
      /* ignore a malformed event */
    }
  };
}

export function useLiveEvents(handler: Handler): boolean {
  const ref = useRef(handler);
  ref.current = handler;
  useEffect(() => {
    const h: Handler = (e) => ref.current(e);
    handlers.add(h);
    ensureSource();
    return () => {
      handlers.delete(h);
      if (handlers.size === 0 && source) {
        source.close();
        source = null;
        setConnected(false);
      }
    };
  }, []);
  return useSyncExternalStore(
    (listener) => {
      connectionListeners.add(listener);
      return () => connectionListeners.delete(listener);
    },
    () => connected,
  );
}

// --------------------------------------------------------------------- routing
export function useHashRoute(): string {
  return useSyncExternalStore(
    (listener) => {
      window.addEventListener("hashchange", listener);
      return () => window.removeEventListener("hashchange", listener);
    },
    () => window.location.hash || "#/",
  );
}
