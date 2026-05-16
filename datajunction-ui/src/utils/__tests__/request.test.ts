/**
 * Test the request function
 */

import { request } from '../request';

declare let window: { fetch: Mock };

describe('request', () => {
  beforeEach(() => {
    window.fetch = vi.fn();
  });

  describe('stubbing successful response', () => {
    beforeEach(() => {
      const res = new Response('{"hello":"world"}', {
        status: 200,
        headers: {
          'Content-type': 'application/json',
        },
      });

      window.fetch.mockReturnValue(Promise.resolve(res));
    });

    it('should format the response correctly', async () => {
      const json = await request('/thisurliscorrect');
      expect(json.hello).toBe('world');
    });
  });

  describe('stubbing 204 response', () => {
    beforeEach(() => {
      // The Response constructor rejects a body with status 204, so pass null.
      const res = new Response(null, {
        status: 204,
        statusText: 'No Content',
      });

      window.fetch.mockReturnValue(Promise.resolve(res));
    });

    it('should return null on 204 response', async () => {
      const json = await request('/thisurliscorrect');
      expect(json).toBeNull();
    });
  });

  describe('stubbing error response', () => {
    beforeEach(() => {
      const res = new Response('', {
        status: 404,
        statusText: 'Not Found',
        headers: {
          'Content-type': 'application/json',
        },
      });

      window.fetch.mockReturnValue(Promise.resolve(res));
    });

    it('should catch errors', async () => {
      await expect(request('/thisdoesntexist')).rejects.toMatchObject({
        response: {
          status: 404,
          statusText: 'Not Found',
        },
      });
    });
  });
});
