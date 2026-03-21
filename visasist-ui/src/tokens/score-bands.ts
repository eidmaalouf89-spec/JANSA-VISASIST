export const SCORE_BAND_CRITICAL = { min: 80, max: 100, label: 'CRITICAL' } as const;
export const SCORE_BAND_HIGH     = { min: 60, max: 79,  label: 'HIGH' } as const;
export const SCORE_BAND_MEDIUM   = { min: 40, max: 59,  label: 'MEDIUM' } as const;
export const SCORE_BAND_LOW      = { min: 0,  max: 39,  label: 'LOW' } as const;

export const SCORE_BANDS = [
  SCORE_BAND_CRITICAL,
  SCORE_BAND_HIGH,
  SCORE_BAND_MEDIUM,
  SCORE_BAND_LOW,
] as const;
