import { StrictMode, useState, useEffect } from 'react';
import { createRoot } from 'react-dom/client';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { LangContext } from './i18n/lang-context';
import { ROUTES } from './routes';
import AppShell from './components/AppShell';
import DashboardScreen from './screens/dashboard/DashboardScreen';
import QueueScreen from './screens/queue/QueueScreen';
import WorkspaceScreen from './screens/workspace/WorkspaceScreen';
import SuggestionsScreen from './screens/suggestions/SuggestionsScreen';
import AIAssistantScreen from './screens/ai-assistant/AIAssistantScreen';
import ReportsScreen from './screens/reports/ReportsScreen';
import AuditScreen from './screens/audit/AuditScreen';
import type { Lang } from './types';
import './tokens/theme.css';

function getInitialLang(): Lang {
  try {
    const stored = localStorage.getItem('visasist-lang');
    if (stored === 'en' || stored === 'fr') return stored;
  } catch {
    // localStorage unavailable
  }
  return 'en';
}

function App() {
  const [lang, setLang] = useState<Lang>(getInitialLang);

  useEffect(() => {
    try {
      localStorage.setItem('visasist-lang', lang);
    } catch {
      // ignore
    }
  }, [lang]);

  return (
    <LangContext.Provider value={{ lang, setLang }}>
      <BrowserRouter>
        <Routes>
          <Route element={<AppShell />}>
            <Route index element={<Navigate to={ROUTES.dashboard} replace />} />
            <Route path={ROUTES.dashboard} element={<DashboardScreen />} />
            <Route path={ROUTES.queue} element={<QueueScreen />} />
            <Route path={ROUTES.workspace} element={<WorkspaceScreen />} />
            <Route path={ROUTES.suggestions} element={<SuggestionsScreen />} />
            <Route path={ROUTES.aiAssistant} element={<AIAssistantScreen />} />
            <Route path={ROUTES.reports} element={<ReportsScreen />} />
            <Route path={ROUTES.audit} element={<AuditScreen />} />
            <Route path="*" element={<Navigate to={ROUTES.dashboard} replace />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </LangContext.Provider>
  );
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>
);
