import { useTranslation } from '../../i18n/use-translation';

export default function AuditScreen() {
  const { t } = useTranslation();
  return <div style={{ padding: '24px' }}><h1 style={{ color: 'var(--text-primary)' }}>{t('nav_audit')}</h1></div>;
}
