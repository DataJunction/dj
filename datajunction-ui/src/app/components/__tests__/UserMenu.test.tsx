import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import UserMenu from '../UserMenu';
import DJClientContext from '../../providers/djclient';

describe('<UserMenu />', () => {
  const createMockDjClient = (overrides = {}) => ({
    whoami: jest.fn().mockResolvedValue({
      id: 1,
      username: 'testuser',
      email: 'test@example.com',
    }),
    logout: jest.fn().mockResolvedValue({}),
    ...overrides,
  });

  const renderWithContext = (mockDjClient: any, props = {}) => {
    return render(
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <UserMenu {...props} />
      </DJClientContext.Provider>,
    );
  };

  // Mock window.location.reload
  const originalLocation = window.location;

  beforeEach(() => {
    jest.clearAllMocks();
    delete (window as any).location;
    window.location = { ...originalLocation, reload: jest.fn() };
  });

  afterEach(() => {
    window.location = originalLocation;
  });

  it('renders the avatar button', async () => {
    const mockDjClient = createMockDjClient();
    renderWithContext(mockDjClient);

    const button = screen.getByRole('button');
    expect(button).toBeInTheDocument();
    expect(button).toHaveClass('avatar-button');
  });

  it('shows "?" before user is loaded', () => {
    const mockDjClient = createMockDjClient({
      whoami: jest.fn().mockImplementation(
        () => new Promise(() => {}), // Never resolves
      ),
    });
    renderWithContext(mockDjClient);

    const button = screen.getByRole('button');
    expect(button).toHaveTextContent('?');
  });

  it('displays initials from username (first two letters uppercase)', async () => {
    const mockDjClient = createMockDjClient({
      whoami: jest.fn().mockResolvedValue({
        id: 1,
        username: 'johndoe',
        email: 'john@example.com',
      }),
    });
    renderWithContext(mockDjClient);

    await waitFor(() => {
      expect(mockDjClient.whoami).toHaveBeenCalled();
    });

    const button = await screen.findByText('JO');
    expect(button).toBeInTheDocument();
  });

  it('displays initials from name when available', async () => {
    const mockDjClient = createMockDjClient({
      whoami: jest.fn().mockResolvedValue({
        id: 1,
        username: 'johndoe',
        email: 'john@example.com',
        name: 'John Doe',
      }),
    });
    renderWithContext(mockDjClient);

    await waitFor(() => {
      expect(mockDjClient.whoami).toHaveBeenCalled();
    });

    const button = await screen.findByText('JD');
    expect(button).toBeInTheDocument();
  });

  it('opens dropdown when avatar is clicked', async () => {
    const mockDjClient = createMockDjClient();
    renderWithContext(mockDjClient);

    await waitFor(() => {
      expect(mockDjClient.whoami).toHaveBeenCalled();
    });

    const button = screen.getByRole('button');
    fireEvent.click(button);

    expect(screen.getByText('testuser')).toBeInTheDocument();
  });

  it('shows Settings and Logout links in dropdown', async () => {
    const mockDjClient = createMockDjClient();
    renderWithContext(mockDjClient);

    await waitFor(() => {
      expect(mockDjClient.whoami).toHaveBeenCalled();
    });

    const button = screen.getByRole('button');
    fireEvent.click(button);

    const settingsLink = screen.getByText('Settings');
    expect(settingsLink).toHaveAttribute('href', '/settings');

    const logoutLink = screen.getByText('Logout');
    expect(logoutLink).toHaveAttribute('href', '/');
  });

  it('calls logout and reloads page when Logout is clicked', async () => {
    const mockDjClient = createMockDjClient();
    renderWithContext(mockDjClient);

    await waitFor(() => {
      expect(mockDjClient.whoami).toHaveBeenCalled();
    });

    const button = screen.getByRole('button');
    fireEvent.click(button);

    const logoutLink = screen.getByText('Logout');
    fireEvent.click(logoutLink);

    expect(mockDjClient.logout).toHaveBeenCalled();
  });

  it('calls onDropdownToggle when dropdown is opened', async () => {
    const mockDjClient = createMockDjClient();
    const onDropdownToggle = jest.fn();
    renderWithContext(mockDjClient, { onDropdownToggle });

    await waitFor(() => {
      expect(mockDjClient.whoami).toHaveBeenCalled();
    });

    const button = screen.getByRole('button');
    fireEvent.click(button);

    expect(onDropdownToggle).toHaveBeenCalledWith(true);
  });

  it('closes dropdown when forceClose becomes true', async () => {
    const mockDjClient = createMockDjClient();

    const { rerender } = render(
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <UserMenu forceClose={false} />
      </DJClientContext.Provider>,
    );

    await waitFor(() => {
      expect(mockDjClient.whoami).toHaveBeenCalled();
    });

    // Open the dropdown
    const button = screen.getByRole('button');
    fireEvent.click(button);

    // Verify dropdown is open
    expect(screen.getByText('testuser')).toBeInTheDocument();

    // Rerender with forceClose=true
    rerender(
      <DJClientContext.Provider value={{ DataJunctionAPI: mockDjClient }}>
        <UserMenu forceClose={true} />
      </DJClientContext.Provider>,
    );

    // Dropdown should be closed
    expect(screen.queryByText('Settings')).not.toBeInTheDocument();
  });

  it('closes dropdown when clicking outside', async () => {
    const mockDjClient = createMockDjClient();
    const onDropdownToggle = jest.fn();
    renderWithContext(mockDjClient, { onDropdownToggle });

    await waitFor(() => {
      expect(mockDjClient.whoami).toHaveBeenCalled();
    });

    // Open the dropdown
    const button = screen.getByRole('button');
    fireEvent.click(button);

    // Verify dropdown is open
    expect(screen.getByText('testuser')).toBeInTheDocument();

    // Click outside
    fireEvent.click(document.body);

    // Dropdown should be closed
    expect(screen.queryByText('Settings')).not.toBeInTheDocument();

    // onDropdownToggle should be called with false
    expect(onDropdownToggle).toHaveBeenCalledWith(false);
  });

  it('toggles dropdown closed when clicking avatar again', async () => {
    const mockDjClient = createMockDjClient();
    const onDropdownToggle = jest.fn();
    renderWithContext(mockDjClient, { onDropdownToggle });

    await waitFor(() => {
      expect(mockDjClient.whoami).toHaveBeenCalled();
    });

    const button = screen.getByRole('button');

    // Open
    fireEvent.click(button);
    expect(onDropdownToggle).toHaveBeenCalledWith(true);
    expect(screen.getByText('testuser')).toBeInTheDocument();

    // Close by clicking again
    fireEvent.click(button);
    expect(onDropdownToggle).toHaveBeenCalledWith(false);
  });
});
