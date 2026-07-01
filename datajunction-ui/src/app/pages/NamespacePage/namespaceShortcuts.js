// Per-device pinned namespaces, backed by localStorage (no backend).
const PINNED_KEY = 'dj.ns.pinned';

function read(key) {
  try {
    const v = JSON.parse(localStorage.getItem(key));
    return Array.isArray(v) ? v.filter(x => typeof x === 'string') : [];
  } catch {
    return [];
  }
}

function write(key, list) {
  try {
    localStorage.setItem(key, JSON.stringify(list));
  } catch {
    /* ignore quota / unavailable storage */
  }
}

export function getPinned() {
  return read(PINNED_KEY);
}

export function togglePinned(ns) {
  const cur = read(PINNED_KEY);
  const next = cur.includes(ns) ? cur.filter(x => x !== ns) : [...cur, ns];
  write(PINNED_KEY, next);
  return next;
}

export { PINNED_KEY };
