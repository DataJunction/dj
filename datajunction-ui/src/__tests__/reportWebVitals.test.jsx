import reportWebVitals from '../reportWebVitals';

// Mocking the web-vitals module
vi.mock('web-vitals', () => ({
  getCLS: vi.fn(),
  getFID: vi.fn(),
  getFCP: vi.fn(),
  getLCP: vi.fn(),
  getTTFB: vi.fn(),
}));

describe('reportWebVitals', () => {
  // Mock web-vitals functions
  const mockGetCLS = vi.fn();
  const mockGetFID = vi.fn();
  const mockGetFCP = vi.fn();
  const mockGetLCP = vi.fn();
  const mockGetTTFB = vi.fn();

  beforeAll(() => {
    vi.doMock('web-vitals', () => ({
      getCLS: mockGetCLS,
      getFID: mockGetFID,
      getFCP: mockGetFCP,
      getLCP: mockGetLCP,
      getTTFB: mockGetTTFB,
    }));
  });

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('does not call the web vitals functions if onPerfEntry is not a function', async () => {
    await reportWebVitals(undefined);

    const { getCLS, getFID, getFCP, getLCP, getTTFB } = await import(
      'web-vitals'
    );
    expect(getCLS).not.toHaveBeenCalled();
    expect(getFID).not.toHaveBeenCalled();
    expect(getFCP).not.toHaveBeenCalled();
    expect(getLCP).not.toHaveBeenCalled();
    expect(getTTFB).not.toHaveBeenCalled();
  });
});
