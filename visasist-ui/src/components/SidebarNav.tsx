import { useLocation, useNavigate } from 'react-router-dom';
import { useTranslation } from '../i18n/use-translation';
import { ROUTES } from '../routes';
import type { TranslationKey } from '../i18n/en';

interface NavItem {
  path: string;
  labelKey: TranslationKey;
  icon: string;
}

const NAV_ITEMS: NavItem[] = [
  { path: ROUTES.dashboard,   labelKey: 'nav_dashboard',     icon: '◻' },
  { path: ROUTES.queue,       labelKey: 'nav_queue',         icon: '☰' },
  { path: ROUTES.suggestions, labelKey: 'nav_suggestions',   icon: '💡' },
  { path: ROUTES.aiAssistant, labelKey: 'nav_ai_assistant',  icon: '🤖' },
  { path: ROUTES.reports,     labelKey: 'nav_reports',       icon: '📊' },
  { path: ROUTES.audit,       labelKey: 'nav_audit',         icon: '🔍' },
];

interface SidebarNavProps {
  collapsed: boolean;
  onToggle: () => void;
}

export default function SidebarNav({ collapsed, onToggle }: SidebarNavProps) {
  const { t } = useTranslation();
  const location = useLocation();
  const navigate = useNavigate();

  const isActive = (path: string) => {
    if (path === ROUTES.dashboard) return location.pathname === ROUTES.dashboard;
    if (path === ROUTES.workspace) return location.pathname.startsWith('/workspace');
    return location.pathname.startsWith(path);
  };

  return (
    <nav
      style={{
        width: collapsed ? 52 : 210,
        height: '100%',
        background: 'var(--bg-secondary)',
        borderRight: '1px solid var(--border-default)',
        display: 'flex',
        flexDirection: 'column',
        transition: 'width 200ms ease',
        overflow: 'hidden',
        flexShrink: 0,
      }}
    >
      <button
        onClick={onToggle}
        aria-label={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
        style={{
          padding: '16px',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          color: 'var(--text-secondary)',
          borderBottom: '1px solid var(--border-default)',
          minHeight: 48,
        }}
      >
        {collapsed ? '→' : '←'}
      </button>

      <div style={{ flex: 1, padding: '8px 0' }}>
        {NAV_ITEMS.map((item) => {
          const active = isActive(item.path);
          return (
            <a
              key={item.path}
              href={item.path}
              onClick={(e) => {
                e.preventDefault();
                navigate(item.path);
              }}
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: 10,
                padding: collapsed ? '10px 16px' : '10px 16px',
                margin: '2px 6px',
                borderRadius: 6,
                background: active ? 'var(--bg-secondary)' : 'transparent',
                borderLeft: active ? '2px solid var(--color-accent)' : '2px solid transparent',
                color: active ? 'var(--color-accent)' : 'var(--text-secondary)',
                whiteSpace: 'nowrap',
                fontSize: 13,
                textDecoration: 'none',
                transition: 'background 120ms ease, color 120ms ease',
              }}
              onMouseEnter={(e) => {
                if (!active) e.currentTarget.style.background = 'var(--bg-tertiary)';
              }}
              onMouseLeave={(e) => {
                if (!active) e.currentTarget.style.background = 'transparent';
              }}
            >
              <span style={{ fontSize: 16, width: 20, textAlign: 'center', flexShrink: 0 }}>
                {item.icon}
              </span>
              {!collapsed && <span>{t(item.labelKey)}</span>}
            </a>
          );
        })}
      </div>
    </nav>
  );
}
