import * as React from 'react';
import { render, screen } from '@testing-library/react';

import LinkedSQL from '../LinkedSQL';

describe('LinkedSQL', () => {
  it('links every upstream the query references', () => {
    render(
      <LinkedSQL
        sql={
          'SELECT a.x, b.y FROM default.orders a JOIN default.customers b ON a.id = b.id'
        }
        upstreams={['default.orders', 'default.customers']}
      />,
    );

    expect(
      screen.getByRole('link', { name: 'default.orders' }),
    ).toHaveAttribute('href', '/nodes/default.orders');
    expect(
      screen.getByRole('link', { name: 'default.customers' }),
    ).toHaveAttribute('href', '/nodes/default.customers');
  });

  it('styles links as code rather than as page links', () => {
    render(
      <LinkedSQL
        sql={'SELECT 1 FROM default.orders'}
        upstreams={['default.orders']}
      />,
    );

    expect(screen.getByRole('link', { name: 'default.orders' })).toHaveClass(
      'sql-upstream-link',
    );
  });

  it('links a dotted name the highlighter splits into separate tokens', () => {
    render(
      <LinkedSQL
        sql={'SELECT 1 FROM users.bmiroglio.funnel'}
        upstreams={['users.bmiroglio.funnel']}
      />,
    );

    expect(
      screen.getByRole('link', { name: 'users.bmiroglio.funnel' }),
    ).toHaveAttribute('href', '/nodes/users.bmiroglio.funnel');
  });

  it('leaves the query alone when it references no upstream', () => {
    render(
      <LinkedSQL sql={'SELECT 1 FROM somewhere.else'} upstreams={['a.b.c']} />,
    );

    expect(screen.queryByRole('link')).toBeNull();
    expect(screen.getByText(/somewhere/)).toBeDefined();
  });

  it('renders without upstreams', () => {
    render(<LinkedSQL sql={'SELECT 1'} />);

    expect(screen.queryByRole('link')).toBeNull();
  });

  it('does not link a name that is only part of a longer identifier', () => {
    render(
      <LinkedSQL
        sql={'SELECT 1 FROM default.orders_archive'}
        upstreams={['default.orders']}
      />,
    );

    expect(screen.queryByRole('link')).toBeNull();
  });

  it('styles tokens on a linked row exactly as on an unlinked row', () => {
    const { container } = render(
      <LinkedSQL
        sql={'SELECT 1\nFROM default.orders'}
        upstreams={['default.orders']}
      />,
    );

    const tokenStyle = text =>
      [...container.querySelectorAll('span')]
        .filter(span => span.textContent === text)
        .map(span => span.getAttribute('style'));

    // `SELECT` sits on a row with no link, `FROM` on the linked row.
    expect(tokenStyle('SELECT')).toEqual(['color: rgb(0, 153, 153);']);
    expect(tokenStyle('FROM')).toEqual(['color: rgb(0, 153, 153);']);
  });

  it('nests the link inside its token so it inherits the token colour', () => {
    const { container } = render(
      <LinkedSQL
        sql={'SELECT 1 FROM default.orders'}
        upstreams={['default.orders']}
      />,
    );

    const link = screen.getByRole('link', { name: 'default.orders' });
    expect(link.parentElement.tagName).toEqual('SPAN');
    expect(container.querySelector('code').contains(link)).toEqual(true);
  });

  it('prefers the longest upstream when one name prefixes another', () => {
    render(
      <LinkedSQL
        sql={'SELECT 1 FROM default.orders.detail'}
        upstreams={['default.orders', 'default.orders.detail']}
      />,
    );

    const links = screen.getAllByRole('link');
    expect(links).toHaveLength(1);
    expect(links[0]).toHaveAttribute('href', '/nodes/default.orders.detail');
  });
});
