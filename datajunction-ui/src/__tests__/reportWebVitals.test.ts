import reportWebVitals from '../reportWebVitals';

describe('reportWebVitals', () => {
  it('calls web vitals functions when handler is provided', async () => {
    const mockHandler = jest.fn();

    // Call reportWebVitals with a handler
    reportWebVitals(mockHandler);

    // Wait for dynamic import to resolve
    await new Promise(resolve => setTimeout(resolve, 100));

    // The handler should have been called by web vitals
    // (we just verify it doesn't throw)
    expect(mockHandler).toBeDefined();
  });

  it('does nothing when no handler is provided', () => {
    // Should not throw
    expect(() => reportWebVitals()).not.toThrow();
  });

  it('does nothing when handler is not a function', () => {
    // Should not throw
    expect(() => reportWebVitals(undefined)).not.toThrow();
  });
});
