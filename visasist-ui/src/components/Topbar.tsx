import { useContext, useState, useEffect } from 'react';
import { LangContext } from '../i18n/lang-context';
import { useTranslation } from '../i18n/use-translation';
import { getPipelineRun } from '../data/pipeline';
import type { PipelineRun } from '../data/pipeline';
import type { Lang } from '../types';

export default function Topbar() {
  const { lang, setLang } = useContext(LangContext);
  const { t } = useTranslation();
  const [run, setRun] = useState<PipelineRun | null>(null);

  useEffect(() => {
    getPipelineRun().then(setRun);
  }, []);

  const handleLangSwitch = (newLang: Lang) => {
    setLang(newLang);
  };

  return (
    <header
      style={{
        height: 48,
        background: 'var(--bg-secondary)',
        borderBottom: '1px solid var(--border-default)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        padding: '0 20px',
        flexShrink: 0,
      }}
    >
      {/* Left: project name */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
        <span style={{ fontWeight: 600, fontSize: 14, color: 'var(--text-primary)' }}>
          JANSA VISASIST
        </span>
        {run && (
          <span style={{ fontSize: 11, color: 'var(--text-tertiary)' }}>
            Run #{run.run_id}
          </span>
        )}
      </div>

      {/* Right: controls */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
        {/* Language toggle pill */}
        <div
          style={{
            display: 'flex',
            borderRadius: 6,
            overflow: 'hidden',
            border: '1px solid var(--border-default)',
          }}
        >
          {(['en', 'fr'] as const).map((l) => (
            <button
              key={l}
              onClick={() => handleLangSwitch(l)}
              aria-pressed={lang === l}
              style={{
                padding: '4px 12px',
                fontSize: 12,
                fontWeight: 500,
                background: lang === l ? 'var(--color-accent)' : 'transparent',
                color: lang === l ? '#ffffff' : 'var(--text-secondary)',
                transition: 'background 120ms ease, color 120ms ease',
              }}
            >
              {l.toUpperCase()}
            </button>
          ))}
        </div>

        {/* Import button stub */}
        <button
          style={{
            padding: '5px 12px',
            fontSize: 12,
            borderRadius: 6,
            border: '1px solid var(--border-default)',
            color: 'var(--text-secondary)',
          }}
        >
          {t('common_import')}
        </button>

        {/* Notifications placeholder */}
        <button
          aria-label={t('common_notifications')}
          style={{
            width: 32,
            height: 32,
            borderRadius: '50%',
            background: 'var(--bg-tertiary)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            fontSize: 14,
            color: 'var(--text-secondary)',
          }}
        >
          🔔
        </button>

        {/* Avatar placeholder */}
        <div
          style={{
            width: 28,
            height: 28,
            borderRadius: '50%',
            background: 'var(--color-accent)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            fontSize: 12,
            color: '#fff',
            fontWeight: 600,
          }}
        >
          E
        </div>
      </div>
    </header>
  );
}
