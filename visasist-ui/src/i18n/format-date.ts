import type { Lang } from '../types';

const LOCALE_MAP: Record<Lang, string> = {
  en: 'en-GB',
  fr: 'fr-FR',
};

/**
 * Format a date string according to the active language.
 * Returns a short date + time string (e.g. "21 Mar 2026, 08:00" in EN,
 * "21 mars 2026, 08:00" in FR).
 */
export function formatRunDate(isoString: string, lang: Lang): string {
  const locale = LOCALE_MAP[lang];
  return new Date(isoString).toLocaleDateString(locale, {
    day: '2-digit',
    month: 'short',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}
