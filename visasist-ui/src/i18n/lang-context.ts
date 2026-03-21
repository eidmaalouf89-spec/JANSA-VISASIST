import { createContext } from 'react';
import type { Lang } from '../types';

export const LangContext = createContext<{
  lang: Lang;
  setLang: (l: Lang) => void;
}>({ lang: 'en', setLang: () => {} });
