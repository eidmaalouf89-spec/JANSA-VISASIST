import { useParams } from 'react-router-dom';
import { useTranslation } from '../../i18n/use-translation';

export default function WorkspaceScreen() {
  const { t } = useTranslation();
  const { docVersionKey } = useParams<{ docVersionKey: string }>();
  return (
    <div style={{ padding: '24px' }}>
      <h1 style={{ color: 'var(--text-primary)' }}>{t('nav_workspace')}</h1>
      <p style={{ color: 'var(--text-secondary)', marginTop: '8px' }}>{docVersionKey}</p>
    </div>
  );
}
