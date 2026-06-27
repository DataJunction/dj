import { render, screen, fireEvent } from '@testing-library/react';
import FolderList from '../FolderList';

it('renders folder rows and fires onOpen with the path', () => {
  const onOpen = vi.fn();
  render(
    <FolderList
      folders={[
        { namespace: 'experiments', path: 'growth.experiments' },
        { namespace: 'metrics', path: 'growth.metrics' },
      ]}
      onOpen={onOpen}
    />,
  );
  expect(screen.getByText('FOLDERS')).toBeInTheDocument();
  fireEvent.click(screen.getByText('experiments'));
  expect(onOpen).toHaveBeenCalledWith('growth.experiments');
});

it('renders nothing when there are no folders', () => {
  const { container } = render(<FolderList folders={[]} onOpen={() => {}} />);
  expect(container).toBeEmptyDOMElement();
});
