import React from 'react';
import { render, fireEvent, screen } from '@testing-library/react';
import ToggleSwitch from '../ToggleSwitch';

describe('<ToggleSwitch />', () => {
  const defaultProps = {
    checked: false,
    onChange: jest.fn(),
    toggleName: 'Toggle Switch',
  };

  it('renders without crashing', () => {
    render(<ToggleSwitch {...defaultProps} />);
  });

  it('displays the correct toggle name', () => {
    render(<ToggleSwitch {...defaultProps} />);
    expect(screen.getByText(defaultProps.toggleName)).toBeInTheDocument();
  });

  it('reflects the checked state correctly', () => {
    render(<ToggleSwitch {...defaultProps} checked={true} />);
    const checkbox = screen.getByRole('checkbox');
    expect(checkbox).toBeChecked();
  });

  it('calls onChange with the correct value when toggled', () => {
    render(<ToggleSwitch {...defaultProps} />);
    const checkbox = screen.getByRole('checkbox');

    fireEvent.click(checkbox);
    expect(defaultProps.onChange).toHaveBeenCalledWith(true);

    fireEvent.click(checkbox);
    expect(checkbox).not.toBeChecked();
  });

  it('is unchecked by default if no checked prop is provided', () => {
    render(<ToggleSwitch onChange={jest.fn()} toggleName="Test Toggle" />);
    const checkbox = screen.getByRole('checkbox');
    expect(checkbox).not.toBeChecked();
  });
});
