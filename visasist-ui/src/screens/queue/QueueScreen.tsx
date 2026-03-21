import { useTranslation } from '../../i18n/use-translation';

export default function QueueScreen() {
  const { t } = useTranslation();
  return <div style={{ padding: '24px' }}><h1 style={{ color: 'var(--text-primary)' }}>{t('nav_queue')}</h1></div>;
}
