import * as React from 'react';
import { render, fireEvent } from '@testing-library/react';

import Tab from '../Tab';

describe('<Tab />', () => {
  it('renders without crashing', () => {
    render(<Tab />);
  });

  it('has the active class when selectedTab matches id', () => {
    const { container } = render(<Tab id="1" selectedTab="1" />);
    expect(container.querySelector('.col')).toHaveClass('active');
  });

  it('does not have the active class when selectedTab does not match id', () => {
    const { container } = render(<Tab id="1" selectedTab="2" />);
    expect(container.querySelector('.col')).not.toHaveClass('active');
  });

  it('calls onClick when the button is clicked', () => {
    const onClickMock = jest.fn();
    const { getByRole } = render(<Tab id="1" onClick={onClickMock} />);
    fireEvent.click(getByRole('button'));
    expect(onClickMock).toHaveBeenCalledTimes(1);
  });
});
