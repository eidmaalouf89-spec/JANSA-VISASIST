export const ROUTES = {
  dashboard:   '/dashboard',
  queue:       '/queue',
  workspace:   '/workspace/:docVersionKey',
  suggestions: '/suggestions',
  aiAssistant: '/ai-assistant',
  reports:     '/reports',
  audit:       '/audit',
} as const;

export const workspaceRoute = (docVersionKey: string) =>
  `/workspace/${encodeURIComponent(docVersionKey)}`;
