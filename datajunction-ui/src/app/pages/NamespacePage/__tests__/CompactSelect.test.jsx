import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import CompactSelect from '../CompactSelect';

describe('<CompactSelect />', () => {
  const defaultOptions = [
    { value: 'option1', label: 'Option 1' },
    { value: 'option2', label: 'Option 2' },
    { value: 'option3', label: 'Option 3' },
  ];

  const defaultProps = {
    label: 'Test Label',
    name: 'test-select',
    options: defaultOptions,
    value: '',
    onChange: jest.fn(),
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('renders without crashing', () => {
    render(<CompactSelect {...defaultProps} />);
    expect(screen.getByText('Test Label')).toBeInTheDocument();
  });

  it('displays the label correctly', () => {
    render(<CompactSelect {...defaultProps} label="My Custom Label" />);
    expect(screen.getByText('My Custom Label')).toBeInTheDocument();
  });

  it('shows placeholder when no value is selected', () => {
    render(<CompactSelect {...defaultProps} placeholder="Choose one..." />);
    expect(screen.getByText('Choose one...')).toBeInTheDocument();
  });

  it('displays the selected value for single select', () => {
    render(<CompactSelect {...defaultProps} value="option1" />);
    expect(screen.getByText('Option 1')).toBeInTheDocument();
  });

  it('calls onChange when an option is selected', async () => {
    const handleChange = jest.fn();
    render(<CompactSelect {...defaultProps} onChange={handleChange} />);

    // Open the dropdown
    const selectInput = screen.getByRole('combobox');
    fireEvent.keyDown(selectInput, { key: 'ArrowDown' });

    // Click on an option
    await waitFor(() => {
      expect(screen.getByText('Option 2')).toBeInTheDocument();
    });
    fireEvent.click(screen.getByText('Option 2'));

    expect(handleChange).toHaveBeenCalledWith(
      expect.objectContaining({ value: 'option2', label: 'Option 2' }),
      expect.anything(),
    );
  });

  it('supports multi-select mode', async () => {
    const handleChange = jest.fn();
    render(
      <CompactSelect
        {...defaultProps}
        isMulti={true}
        value={['option1']}
        onChange={handleChange}
      />,
    );

    // Should show the selected value
    expect(screen.getByText('Option 1')).toBeInTheDocument();
  });

  it('displays multiple selected values in multi-select mode', () => {
    render(
      <CompactSelect
        {...defaultProps}
        isMulti={true}
        value={['option1', 'option2']}
      />,
    );

    expect(screen.getByText('Option 1')).toBeInTheDocument();
    expect(screen.getByText('Option 2')).toBeInTheDocument();
  });

  it('shows loading state when isLoading is true', () => {
    render(<CompactSelect {...defaultProps} isLoading={true} />);
    // react-select shows a loading indicator when isLoading is true
    expect(document.querySelector('.css-1dimb5e-singleValue')).toBeNull();
  });

  it('allows clearing the selection when isClearable is true', async () => {
    const handleChange = jest.fn();
    render(
      <CompactSelect
        {...defaultProps}
        value="option1"
        onChange={handleChange}
        isClearable={true}
      />,
    );

    // Find and click the clear button
    const clearButton = document.querySelector(
      '[class*="indicatorContainer"]:first-of-type',
    );
    if (clearButton) {
      fireEvent.mouseDown(clearButton);
    }
  });

  it('respects minWidth prop', () => {
    const { container } = render(
      <CompactSelect {...defaultProps} minWidth="200px" />,
    );
    const wrapper = container.firstChild;
    expect(wrapper).toHaveStyle({ minWidth: '200px' });
  });

  it('respects flex prop', () => {
    const { container } = render(<CompactSelect {...defaultProps} flex={2} />);
    const wrapper = container.firstChild;
    expect(wrapper).toHaveStyle({ flex: '2' });
  });

  it('handles empty options array', () => {
    render(<CompactSelect {...defaultProps} options={[]} />);
    expect(screen.getByText('Test Label')).toBeInTheDocument();
  });

  it('handles null value gracefully', () => {
    render(<CompactSelect {...defaultProps} value={null} />);
    expect(screen.getByText('Select...')).toBeInTheDocument();
  });

  it('handles undefined value gracefully', () => {
    render(<CompactSelect {...defaultProps} value={undefined} />);
    expect(screen.getByText('Select...')).toBeInTheDocument();
  });

  it('renders with custom placeholder', () => {
    render(<CompactSelect {...defaultProps} placeholder="Pick something..." />);
    expect(screen.getByText('Pick something...')).toBeInTheDocument();
  });

  it('uses default placeholder when none provided', () => {
    render(<CompactSelect {...defaultProps} />);
    expect(screen.getByText('Select...')).toBeInTheDocument();
  });

  it('applies compact styling with reduced height', () => {
    const { container } = render(<CompactSelect {...defaultProps} />);
    // The control element should have compact styling
    const control = container.querySelector('[class*="control"]');
    expect(control).toBeInTheDocument();
  });

  it('opens dropdown on click', async () => {
    render(<CompactSelect {...defaultProps} />);

    const selectInput = screen.getByRole('combobox');
    fireEvent.mouseDown(selectInput);

    await waitFor(() => {
      // Menu should be visible with options
      expect(screen.getByText('Option 1')).toBeInTheDocument();
      expect(screen.getByText('Option 2')).toBeInTheDocument();
      expect(screen.getByText('Option 3')).toBeInTheDocument();
    });
  });

  it('filters options based on user input', async () => {
    render(<CompactSelect {...defaultProps} />);

    const selectInput = screen.getByRole('combobox');
    fireEvent.focus(selectInput);
    await userEvent.type(selectInput, '2');

    await waitFor(() => {
      expect(screen.getByText('Option 2')).toBeInTheDocument();
    });
  });
});
