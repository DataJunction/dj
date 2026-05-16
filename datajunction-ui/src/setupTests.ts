import '@testing-library/jest-dom/vitest';

import { vi } from 'vitest';
import { render as originalRender } from '@testing-library/react';
import createFetchMock from 'vitest-fetch-mock';
import ResizeObserver from 'resize-observer-polyfill';

const fetchMocker = createFetchMock(vi);
fetchMocker.enableMocks();
// Expose the singleton so the `src/mocks/fetchMock` shim can re-export it
(globalThis as any).__vitestFetchMock = fetchMocker;

(global as any).ResizeObserver = ResizeObserver;

// jsdom doesn't ship rAF/cAF; polyfill so CodeMirror and other libs that
// schedule paints don't crash inside the test environment.
if (!(global as any).requestAnimationFrame) {
  (global as any).requestAnimationFrame = (cb: FrameRequestCallback) =>
    setTimeout(() => cb(Date.now()), 0) as unknown as number;
  (global as any).cancelAnimationFrame = (id: number) => clearTimeout(id);
}

// Range.getBoundingClientRect / getClientRects are used by CodeMirror; jsdom
// returns degenerate values that crash some plugins. Stub them at the prototype
// once so every Range gets them.
if (typeof Range !== 'undefined') {
  if (!Range.prototype.getBoundingClientRect) {
    Range.prototype.getBoundingClientRect = vi.fn() as any;
  }
  if (!Range.prototype.getClientRects) {
    Range.prototype.getClientRects = (() => ({
      item: () => null,
      length: 0,
      [Symbol.iterator]: vi.fn(),
    })) as any;
  }
}

// Kept as a named export for tests that previously imported a wrapped
// `render` from this file. It just forwards to testing-library now —
// the old version recreated jsdom mid-test, which fights vitest's env.
export const render = originalRender;
