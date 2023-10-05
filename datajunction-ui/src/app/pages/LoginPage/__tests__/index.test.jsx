import React from 'react';
import { render, fireEvent, waitFor } from '@testing-library/react';
import { LoginPage } from '../index';

describe('LoginPage', () => {
  const original = window.location;

  beforeAll(() => {
    Object.defineProperty(window, 'location', {
      configurable: true,
      value: { reload: jest.fn() },
    });
  });

  afterAll(() => {
    Object.defineProperty(window, 'location', {
      configurable: true,
      value: original,
    });
  });

  beforeEach(() => {
    fetch.resetMocks();
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('displays error messages when fields are empty and form is submitted', async () => {
    const { getByText, queryAllByText } = render(<LoginPage />);
    fireEvent.click(getByText('Login'));

    await waitFor(() => {
      expect(getByText('DataJunction')).toBeInTheDocument();
      expect(getByText('Username is required')).toBeInTheDocument();
      expect(getByText('Password is required')).toBeInTheDocument();
    });
  });

  it('calls fetch with correct data on submit', async () => {
    const username = 'testUser';
    const password = 'testPassword';

    const { getByText, getByPlaceholderText } = render(<LoginPage />);
    fireEvent.change(getByPlaceholderText('Username'), {
      target: { value: username },
    });
    fireEvent.change(getByPlaceholderText('Password'), {
      target: { value: password },
    });
    fireEvent.click(getByText('Login'));

    await waitFor(() => {
      expect(fetch).toHaveBeenCalledWith(
        `${process.env.REACT_APP_DJ_URL}/basic/login/`,
        expect.objectContaining({
          method: 'POST',
          body: expect.any(FormData),
          credentials: 'include',
        }),
      );
      expect(window.location.reload).toHaveBeenCalled();
    });
  });
});
