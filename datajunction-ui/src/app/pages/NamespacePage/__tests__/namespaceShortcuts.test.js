import { describe, it, expect, beforeEach } from 'vitest';
import { getPinned, togglePinned } from '../namespaceShortcuts';

beforeEach(() => localStorage.clear());

describe('pinned namespaces (localStorage)', () => {
  it('togglePinned adds then removes', () => {
    expect(getPinned()).toEqual([]);
    expect(togglePinned('x')).toEqual(['x']);
    expect(getPinned()).toEqual(['x']);
    expect(togglePinned('x')).toEqual([]);
  });

  it('tolerates malformed storage', () => {
    localStorage.setItem('dj.ns.pinned', 'not json');
    expect(getPinned()).toEqual([]);
  });
});
