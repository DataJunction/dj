import * as React from 'react';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router-dom';

import AddNodeDropdown from '../AddNodeDropdown';
import DJClientContext from '../../providers/djclient';
import UserContext from '../../providers/UserProvider';

const buildClient = () => ({
  DataJunctionAPI: {
    addNamespace: jest.fn().mockResolvedValue({ status: 201, json: {} }),
  },
});

const renderDropdown = ({ namespace, user, djClient = buildClient() }) => {
  const userCtx = {
    currentUser: user,
    loading: false,
    error: null,
    refetchUser: async () => {},
  };
  return {
    djClient,
    ...render(
      <MemoryRouter initialEntries={['/']}>
        <DJClientContext.Provider value={djClient}>
          <UserContext.Provider value={userCtx}>
            <Routes>
              <Route
                path="/"
                element={<AddNodeDropdown namespace={namespace} />}
              />
              <Route path="/create/:nodeType/:ns" element={<div>landed</div>} />
            </Routes>
          </UserContext.Provider>
        </DJClientContext.Provider>
      </MemoryRouter>,
    ),
  };
};

describe('<AddNodeDropdown />', () => {
  it('uses the explicit namespace when provided', () => {
    renderDropdown({
      namespace: 'finance.metrics',
      user: { username: 'alice' },
    });
    expect(screen.getByText('Transform').closest('a')).toHaveAttribute(
      'href',
      '/create/transform/finance.metrics',
    );
    expect(screen.getByText('Metric').closest('a')).toHaveAttribute(
      'href',
      '/create/metric/finance.metrics',
    );
  });

  it('falls back to users.<handle> when at the top-level namespace', () => {
    renderDropdown({ namespace: 'default', user: { username: 'alice' } });
    expect(screen.getByText('Transform').closest('a')).toHaveAttribute(
      'href',
      '/create/transform/users.alice',
    );
  });

  it('falls back to users.<handle> when namespace is undefined', () => {
    renderDropdown({ namespace: undefined, user: { username: 'alice' } });
    expect(screen.getByText('Transform').closest('a')).toHaveAttribute(
      'href',
      '/create/transform/users.alice',
    );
  });

  it('strips email domain and sanitizes the handle', () => {
    renderDropdown({
      namespace: 'default',
      user: { username: 'Alice.B@netflix.com' },
    });
    // dots in the handle get replaced with underscores
    expect(screen.getByText('Transform').closest('a')).toHaveAttribute(
      'href',
      '/create/transform/users.alice_b',
    );
  });

  it('creates the personal namespace on click when falling back', async () => {
    const { djClient } = renderDropdown({
      namespace: 'default',
      user: { username: 'alice' },
    });
    fireEvent.click(screen.getByText('Transform'));
    await waitFor(() => {
      expect(djClient.DataJunctionAPI.addNamespace).toHaveBeenCalledWith(
        'users.alice',
      );
    });
  });

  it('does NOT call addNamespace when an explicit namespace is provided', async () => {
    const { djClient } = renderDropdown({
      namespace: 'finance.metrics',
      user: { username: 'alice' },
    });
    fireEvent.click(screen.getByText('Transform'));
    // give the click a tick to settle
    await new Promise(r => setTimeout(r, 0));
    expect(djClient.DataJunctionAPI.addNamespace).not.toHaveBeenCalled();
  });

  it('leaves the Register Table link unconditional', () => {
    renderDropdown({ namespace: 'default', user: { username: 'alice' } });
    expect(screen.getByText('Register Table').closest('a')).toHaveAttribute(
      'href',
      '/create/source',
    );
  });
});
