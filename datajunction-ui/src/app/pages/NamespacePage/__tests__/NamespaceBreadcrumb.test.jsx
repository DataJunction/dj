import { render, screen, fireEvent } from '@testing-library/react';
import NamespaceBreadcrumb from '../NamespaceBreadcrumb';

it('renders crumbs and navigates to the clicked ancestor', () => {
  const onNavigate = vi.fn();
  render(
    <NamespaceBreadcrumb path="growth.metrics.dau" onNavigate={onNavigate} />,
  );
  fireEvent.click(screen.getByText('growth'));
  expect(onNavigate).toHaveBeenCalledWith('growth');
  fireEvent.click(screen.getByText('metrics'));
  expect(onNavigate).toHaveBeenCalledWith('growth.metrics');
  // Current (last) segment is not a button.
  expect(screen.getByText('dau').closest('button')).toBeNull();
});
