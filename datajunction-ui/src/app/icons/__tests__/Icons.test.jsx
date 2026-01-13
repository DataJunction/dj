import * as React from 'react';
import { render, screen } from '@testing-library/react';

import CommitIcon from '../CommitIcon';
import InvalidIcon from '../InvalidIcon';

describe('Icon components', () => {
  it('should render CommitIcon with default props', () => {
    render(<CommitIcon />);
    expect(document.querySelector('svg')).toBeInTheDocument();
  });

  it('should render InvalidIcon with default props', () => {
    render(<InvalidIcon />);
    expect(screen.getByTestId('invalid-icon')).toBeInTheDocument();
  });

  it('should render InvalidIcon with custom props', () => {
    render(<InvalidIcon width="50px" height="50px" style={{ color: 'red' }} />);
    const icon = screen.getByTestId('invalid-icon');
    expect(icon).toHaveAttribute('width', '50px');
    expect(icon).toHaveAttribute('height', '50px');
  });
});
