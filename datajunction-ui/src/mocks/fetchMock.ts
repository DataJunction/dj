/**
 * Drop-in shim replacing `jest-fetch-mock`'s default export.
 *
 * The singleton is created in `setupTests.ts` and stashed on globalThis so
 * test files that previously did `import fetchMock from 'jest-fetch-mock'`
 * can simply `import fetchMock from 'src/mocks/fetchMock'` and keep calling
 * the same methods (`resetMocks`, `mockResponseOnce`, etc.).
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const fetchMock: any = (globalThis as any).__vitestFetchMock;
export default fetchMock;
