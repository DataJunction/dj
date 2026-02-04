export const AUTH_COOKIE = '__dj';
export const LOGGED_IN_FLAG_COOKIE = '__djlif';

// Scan estimate thresholds (in GB)
// These determine the warning level colors in the UI
const GB = 1024 * 1024 * 1024;

export const SCAN_WARNING_THRESHOLD_GB = parseInt(
  process.env.REACT_APP_SCAN_WARNING_THRESHOLD_GB || '10',
  10,
);
export const SCAN_CRITICAL_THRESHOLD_GB = parseInt(
  process.env.REACT_APP_SCAN_CRITICAL_THRESHOLD_GB || '100',
  10,
);

// Export as bytes for internal use
export const SCAN_WARNING_THRESHOLD = SCAN_WARNING_THRESHOLD_GB * GB;
export const SCAN_CRITICAL_THRESHOLD = SCAN_CRITICAL_THRESHOLD_GB * GB;
