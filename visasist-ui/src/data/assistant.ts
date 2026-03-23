// M6 integration is out of scope for P3.5.
// Replace internals with real M6 API calls in P4.

export async function sendMessage(
  _query: string,
  _lang: 'en' | 'fr',
  _context?: { docVersionKey?: string },
): Promise<string> {
  return 'AI Assistant is not yet connected. Deterministic data is available in the Queue and Workspace.';
}
