import React from 'react';
import { render, fireEvent, screen, waitFor } from '@testing-library/react';
import ClientCodePopover from '../ClientCodePopover';
import userEvent from '@testing-library/user-event';

describe('<ClientCodePopover />', () => {
  const defaultProps = {
    code: "print('Hello, World!')",
  };

  it('toggles the code popover visibility when the button is clicked', async () => {
    render(<ClientCodePopover {...defaultProps} />);

    const button = screen.getByRole('button', 'code-button');

    // Initially, the popover should be hidden
    expect(screen.getByRole('dialog', { hidden: true })).toHaveStyle(
      'display: none',
    );

    // Clicking the button should display the popover
    fireEvent.click(button);
    expect(screen.getByRole('dialog', { hidden: true })).not.toHaveStyle(
      'display: none',
    );

    // Clicking the button again should hide the popover
    fireEvent.click(button);
    expect(screen.getByRole('dialog', { hidden: true })).toHaveStyle(
      'display: none',
    );

    // Trigger onClose by pressing <escape>
    userEvent.keyboard('{Escape}');
    // fireEvent.click(screen.getByTestId('body').firstChild());
    await waitFor(() => {
      expect(screen.getByRole('dialog', { hidden: true })).toHaveStyle(
        'display: none',
      );
    });
  });

  it('renders the provided code within the SyntaxHighlighter', () => {
    render(<ClientCodePopover {...defaultProps} />);
    expect(screen.getByRole('dialog', { hidden: true })).toHaveTextContent(
      defaultProps.code,
    );
  });
});
