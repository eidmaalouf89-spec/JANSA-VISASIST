import { useContext } from 'react';
import en from './en';
import fr from './fr';
import type { TranslationKey } from './en';
import { LangContext } from './lang-context';

const translations: Record<string, Record<TranslationKey, string>> = {
  en,
  fr,
};

export function useTranslation() {
  const { lang } = useContext(LangContext);
  const dict = translations[lang] as Record<TranslationKey, string>;

  const t = (key: TranslationKey): string => {
    return dict[key] || key;
  };

  return { t, lang };
}
