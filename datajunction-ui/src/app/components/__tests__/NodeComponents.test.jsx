import React from 'react';
import { render, screen } from '@testing-library/react';
import { NodeBadge, NodeLink, NodeDisplay, NodeChip } from '../NodeComponents';

describe('NodeComponents', () => {
  describe('<NodeBadge />', () => {
    it('should render badge with node type', () => {
      render(<NodeBadge type="METRIC" />);
      expect(screen.getByText('METRIC')).toBeInTheDocument();
    });

    it('should return null when no type provided', () => {
      const { container } = render(<NodeBadge />);
      expect(container.firstChild).toBeNull();
    });

    it('should render abbreviated badge', () => {
      render(<NodeBadge type="METRIC" abbreviated={true} />);
      expect(screen.getByText('M')).toBeInTheDocument();
    });

    it('should apply small size styles', () => {
      render(<NodeBadge type="METRIC" size="small" />);
      const badge = screen.getByText('METRIC');
      expect(badge).toHaveStyle({ fontSize: '7px' });
    });

    it('should apply medium size styles', () => {
      render(<NodeBadge type="METRIC" size="medium" />);
      const badge = screen.getByText('METRIC');
      expect(badge).toHaveStyle({ fontSize: '9px' });
    });

    it('should apply large size styles', () => {
      render(<NodeBadge type="METRIC" size="large" />);
      const badge = screen.getByText('METRIC');
      expect(badge).toHaveStyle({ fontSize: '11px' });
    });

    it('should use medium as default when invalid size provided', () => {
      render(<NodeBadge type="METRIC" size="invalid" />);
      const badge = screen.getByText('METRIC');
      expect(badge).toHaveStyle({ fontSize: '9px' });
    });

    it('should apply custom styles', () => {
      render(<NodeBadge type="METRIC" style={{ color: 'red' }} />);
      const badge = screen.getByText('METRIC');
      expect(badge).toHaveStyle({ color: 'red' });
    });

    it('should have correct class name', () => {
      render(<NodeBadge type="METRIC" />);
      const badge = screen.getByText('METRIC');
      expect(badge).toHaveClass('node_type__metric');
      expect(badge).toHaveClass('badge');
      expect(badge).toHaveClass('node_type');
    });
  });

  describe('<NodeLink />', () => {
    const mockNode = {
      name: 'default.my_metric',
      type: 'METRIC',
      current: {
        displayName: 'My Metric',
      },
    };

    it('should render link with display name', () => {
      render(<NodeLink node={mockNode} />);
      expect(screen.getByText('My Metric')).toBeInTheDocument();
    });

    it('should return null when no node provided', () => {
      const { container } = render(<NodeLink />);
      expect(container.firstChild).toBeNull();
    });

    it('should return null when node has no name', () => {
      const { container } = render(<NodeLink node={{}} />);
      expect(container.firstChild).toBeNull();
    });

    it('should show full name when showFullName is true', () => {
      render(<NodeLink node={mockNode} showFullName={true} />);
      expect(screen.getByText('default.my_metric')).toBeInTheDocument();
    });

    it('should use last part of name when no display name', () => {
      const nodeWithoutDisplayName = {
        name: 'default.my_metric',
        type: 'METRIC',
      };
      render(<NodeLink node={nodeWithoutDisplayName} />);
      expect(screen.getByText('my_metric')).toBeInTheDocument();
    });

    it('should apply ellipsis styles when enabled', () => {
      render(<NodeLink node={mockNode} ellipsis={true} />);
      const link = screen.getByText('My Metric');
      expect(link).toHaveStyle({
        overflow: 'hidden',
        textOverflow: 'ellipsis',
        whiteSpace: 'nowrap',
      });
    });

    it('should not apply ellipsis styles when disabled', () => {
      render(<NodeLink node={mockNode} ellipsis={false} />);
      const link = screen.getByText('My Metric');
      expect(link).not.toHaveStyle({ overflow: 'hidden' });
    });

    it('should apply small size styles', () => {
      render(<NodeLink node={mockNode} size="small" />);
      const link = screen.getByText('My Metric');
      expect(link).toHaveStyle({ fontSize: '10px', fontWeight: '500' });
    });

    it('should apply medium size styles', () => {
      render(<NodeLink node={mockNode} size="medium" />);
      const link = screen.getByText('My Metric');
      expect(link).toHaveStyle({ fontSize: '12px', fontWeight: '500' });
    });

    it('should apply large size styles', () => {
      render(<NodeLink node={mockNode} size="large" />);
      const link = screen.getByText('My Metric');
      expect(link).toHaveStyle({ fontSize: '14px', fontWeight: '500' });
    });

    it('should use medium as default when invalid size provided', () => {
      render(<NodeLink node={mockNode} size="invalid" />);
      const link = screen.getByText('My Metric');
      expect(link).toHaveStyle({ fontSize: '12px' });
    });

    it('should apply custom styles', () => {
      render(<NodeLink node={mockNode} style={{ color: 'blue' }} />);
      const link = screen.getByText('My Metric');
      expect(link).toHaveStyle({ color: 'blue' });
    });

    it('should link to correct node page', () => {
      render(<NodeLink node={mockNode} />);
      const link = screen.getByText('My Metric');
      expect(link).toHaveAttribute('href', '/nodes/default.my_metric');
    });
  });

  describe('<NodeDisplay />', () => {
    const mockNode = {
      name: 'default.my_metric',
      type: 'METRIC',
      current: {
        displayName: 'My Metric',
      },
    };

    it('should render both link and badge', () => {
      render(<NodeDisplay node={mockNode} />);
      expect(screen.getByText('My Metric')).toBeInTheDocument();
      expect(screen.getByText('METRIC')).toBeInTheDocument();
    });

    it('should return null when no node provided', () => {
      const { container } = render(<NodeDisplay />);
      expect(container.firstChild).toBeNull();
    });

    it('should hide badge when showBadge is false', () => {
      render(<NodeDisplay node={mockNode} showBadge={false} />);
      expect(screen.getByText('My Metric')).toBeInTheDocument();
      expect(screen.queryByText('METRIC')).not.toBeInTheDocument();
    });

    it('should show abbreviated badge', () => {
      render(<NodeDisplay node={mockNode} abbreviatedBadge={true} />);
      expect(screen.getByText('M')).toBeInTheDocument();
    });

    it('should apply ellipsis to link', () => {
      render(<NodeDisplay node={mockNode} ellipsis={true} />);
      const link = screen.getByText('My Metric');
      expect(link).toHaveStyle({ overflow: 'hidden' });
    });

    it('should apply custom gap', () => {
      const { container } = render(<NodeDisplay node={mockNode} gap="10px" />);
      const wrapper = container.firstChild;
      expect(wrapper).toHaveStyle({ gap: '10px' });
    });

    it('should apply custom container styles', () => {
      const { container } = render(
        <NodeDisplay
          node={mockNode}
          containerStyle={{ backgroundColor: 'red' }}
        />,
      );
      const wrapper = container.firstChild;
      expect(wrapper).toHaveStyle({ backgroundColor: 'red' });
    });

    it('should not show badge when node has no type', () => {
      const nodeWithoutType = {
        name: 'default.my_metric',
        current: { displayName: 'My Metric' },
      };
      render(<NodeDisplay node={nodeWithoutType} />);
      expect(screen.getByText('My Metric')).toBeInTheDocument();
      expect(screen.queryByText('METRIC')).not.toBeInTheDocument();
    });
  });

  describe('<NodeChip />', () => {
    const mockNode = {
      name: 'default.my_metric',
      type: 'METRIC',
      current: {
        displayName: 'My Metric',
      },
    };

    it('should render chip with badge and name', () => {
      render(<NodeChip node={mockNode} />);
      expect(screen.getByText('M')).toBeInTheDocument(); // abbreviated badge
      expect(screen.getByText('My Metric')).toBeInTheDocument();
    });

    it('should return null when no node provided', () => {
      const { container } = render(<NodeChip />);
      expect(container.firstChild).toBeNull();
    });

    it('should use last part of name when no display name', () => {
      const nodeWithoutDisplayName = {
        name: 'default.my_metric',
        type: 'METRIC',
      };
      render(<NodeChip node={nodeWithoutDisplayName} />);
      expect(screen.getByText('my_metric')).toBeInTheDocument();
    });

    it('should link to correct node page', () => {
      const { container } = render(<NodeChip node={mockNode} />);
      const link = container.querySelector('a');
      expect(link).toHaveAttribute('href', '/nodes/default.my_metric');
    });

    it('should have compact styling', () => {
      const { container } = render(<NodeChip node={mockNode} />);
      const link = container.querySelector('a');
      expect(link).toHaveStyle({
        fontSize: '12px',
        padding: '2px 6px',
        whiteSpace: 'nowrap',
      });
    });
  });
});
