import { useTranslation } from '../../i18n/use-translation';

export default function SuggestionsScreen() {
  const { t } = useTranslation();
  return <div style={{ padding: '24px' }}><h1 style={{ color: 'var(--text-primary)' }}>{t('nav_suggestions')}</h1></div>;
}
