export type DataMode = 'mock' | 'hybrid' | 'strict' | 'api';

export const DATA_MODE: DataMode =
  (['mock', 'hybrid', 'strict', 'api'] as const).includes(
    import.meta.env.VITE_DATA_MODE as DataMode,
  )
    ? (import.meta.env.VITE_DATA_MODE as DataMode)
    : 'mock';

/**
 * Base URL for the Flask API server.
 * In dev: Vite proxies /api/* to this URL (see vite.config.ts).
 * In prod: set via VITE_API_BASE_URL env var.
 */
export const API_BASE_URL: string =
  import.meta.env.VITE_API_BASE_URL ?? '';

/**
 * Helper: fetch from the API, using the proxy path in dev.
 * Returns the parsed JSON response.
 * Throws on non-OK status.
 */
export async function apiFetch<T = unknown>(path: string): Promise<T> {
  const url = `${API_BASE_URL}${path}`;
  const res = await fetch(url);
  if (!res.ok) {
    throw new IntegrationError(
      `API ${res.status}: ${res.statusText} — ${path}`,
      path,
    );
  }
  return res.json();
}

export class IntegrationError extends Error {
  source: string;
  constructor(message: string, source: string) {
    super(message);
    this.name = 'IntegrationError';
    this.source = source;
  }
}
