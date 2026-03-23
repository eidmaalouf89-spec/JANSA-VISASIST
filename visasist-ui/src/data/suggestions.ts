import type { AISuggestion } from '../types';

// M5 integration is out of scope for P3.5.
// Replace internals with real M5 API calls in P4.

export async function getSuggestion(_docVersionKey: string): Promise<AISuggestion | null> {
  const { aiSuggestion } = await import('../mock/ai-suggestion');
  return aiSuggestion;
}

export async function getTopSuggestions(_limit: number): Promise<AISuggestion[]> {
  const { aiSuggestion } = await import('../mock/ai-suggestion');
  return [aiSuggestion];
}
