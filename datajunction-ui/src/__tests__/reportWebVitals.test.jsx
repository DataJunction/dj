import reportWebVitals from '../reportWebVitals';

// Mocking the web-vitals module
jest.mock('web-vitals', () => ({
  getCLS: jest.fn(),
  getFID: jest.fn(),
  getFCP: jest.fn(),
  getLCP: jest.fn(),
  getTTFB: jest.fn(),
}));

describe('reportWebVitals', () => {
  // Mock web-vitals functions
  const mockGetCLS = jest.fn();
  const mockGetFID = jest.fn();
  const mockGetFCP = jest.fn();
  const mockGetLCP = jest.fn();
  const mockGetTTFB = jest.fn();

  beforeAll(() => {
    jest.doMock('web-vitals', () => ({
      getCLS: mockGetCLS,
      getFID: mockGetFID,
      getFCP: mockGetFCP,
      getLCP: mockGetLCP,
      getTTFB: mockGetTTFB,
    }));
  });

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('does not call the web vitals functions if onPerfEntry is not a function', async () => {
    await reportWebVitals(undefined);

    const { getCLS, getFID, getFCP, getLCP, getTTFB } = require('web-vitals');
    expect(getCLS).not.toHaveBeenCalled();
    expect(getFID).not.toHaveBeenCalled();
    expect(getFCP).not.toHaveBeenCalled();
    expect(getLCP).not.toHaveBeenCalled();
    expect(getTTFB).not.toHaveBeenCalled();
  });
});
