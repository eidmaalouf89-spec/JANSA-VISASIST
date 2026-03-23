export type DataMode = 'mock' | 'hybrid' | 'strict';

export const DATA_MODE: DataMode =
  (['mock', 'hybrid', 'strict'] as const).includes(
    import.meta.env.VITE_DATA_MODE as DataMode,
  )
    ? (import.meta.env.VITE_DATA_MODE as DataMode)
    : 'mock';

export class IntegrationError extends Error {
  source: string;
  constructor(message: string, source: string) {
    super(message);
    this.name = 'IntegrationError';
    this.source = source;
  }
}
