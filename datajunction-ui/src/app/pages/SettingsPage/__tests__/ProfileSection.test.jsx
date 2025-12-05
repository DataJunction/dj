import React from 'react';
import { render, screen } from '@testing-library/react';
import { ProfileSection } from '../ProfileSection';

describe('ProfileSection', () => {
  it('renders user initials from name', () => {
    const user = {
      name: 'John Doe',
      username: 'johndoe',
      email: 'john@example.com',
    };

    render(<ProfileSection user={user} />);

    expect(screen.getByText('JD')).toBeInTheDocument();
    expect(screen.getByText('johndoe')).toBeInTheDocument();
    expect(screen.getByText('john@example.com')).toBeInTheDocument();
  });

  it('renders initials from username when name is not available', () => {
    const user = {
      username: 'alice',
      email: 'alice@example.com',
    };

    render(<ProfileSection user={user} />);

    expect(screen.getByText('AL')).toBeInTheDocument();
  });

  it('renders fallback when no user info available', () => {
    render(<ProfileSection user={null} />);

    expect(screen.getByText('?')).toBeInTheDocument();
    expect(screen.getAllByText('-')).toHaveLength(2);
  });

  it('renders section title', () => {
    render(<ProfileSection user={null} />);

    expect(screen.getByText('Profile')).toBeInTheDocument();
  });

  it('handles single name', () => {
    const user = {
      name: 'Alice',
      username: 'alice',
    };

    render(<ProfileSection user={user} />);

    expect(screen.getByText('A')).toBeInTheDocument();
  });

  it('handles multi-word name and takes only first two initials', () => {
    const user = {
      name: 'John Paul Smith',
      username: 'jps',
    };

    render(<ProfileSection user={user} />);

    expect(screen.getByText('JP')).toBeInTheDocument();
  });
});
