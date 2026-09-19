import React from 'react';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';

import DJClientContext from '../../../providers/djclient';
import PartitionColumnPopover from '../PartitionColumnPopover';

describe('<PartitionColumnPopover />', () => {
  it('submits a role-qualified cube column', async () => {
    const setPartition = vi.fn().mockReturnValue({ status: 201, json: {} });
    const context = {
      DataJunctionAPI: {
        setPartition,
        removePartition: vi.fn(),
      },
    };
    const column = {
      name: 'v3.date.date_id',
      dimension_column: '[ship]',
      partition: null,
    };
    const node = { name: 'v3.role_cube', type: 'cube' };

    render(
      <DJClientContext.Provider value={context}>
        <PartitionColumnPopover
          column={column}
          node={node}
          onSubmit={vi.fn()}
        />
      </DJClientContext.Provider>,
    );

    fireEvent.click(screen.getByLabelText('PartitionColumn'));
    fireEvent.change(screen.getByLabelText('Partition Type'), {
      target: { value: 'temporal' },
    });
    fireEvent.click(screen.getByLabelText('SaveEditColumn'));

    await waitFor(() => {
      expect(setPartition).toHaveBeenCalledWith(
        'v3.role_cube',
        'v3.date.date_id[ship]',
        'temporal',
        'yyyyMMdd',
        'day',
      );
    });
  });
});
