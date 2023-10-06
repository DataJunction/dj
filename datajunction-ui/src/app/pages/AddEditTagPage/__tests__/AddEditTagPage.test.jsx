import React from 'react';
import { screen, waitFor } from '@testing-library/react';
import fetchMock from 'jest-fetch-mock';
import userEvent from '@testing-library/user-event';
import { render } from '../../../../setupTests';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import DJClientContext from '../../../providers/djclient';
import { AddEditTagPage } from '../index';

describe('<AddEditTagPage />', () => {
  const initializeMockDJClient = () => {
    return {
      DataJunctionAPI: {
        addTag: jest.fn(),
      },
    };
  };

  const mockDjClient = initializeMockDJClient();

  beforeEach(() => {
    fetchMock.resetMocks();
    jest.clearAllMocks();
    window.scrollTo = jest.fn();
  });

  const renderAddEditTagPage = element => {
    return render(
      <MemoryRouter initialEntries={['/create/tag']}>
        <Routes>
          <Route path="create/tag" element={element} />
        </Routes>
      </MemoryRouter>,
    );
  };

  const testElement = djClient => {
    return (
      <DJClientContext.Provider value={djClient}>
        <AddEditTagPage />
      </DJClientContext.Provider>
    );
  };

  it('adds a tag correctly', async () => {
    mockDjClient.DataJunctionAPI.addTag.mockReturnValue({
      status: 200,
      json: {
        name: 'amanita_muscaria',
        display_name: 'Amanita Muscaria',
        tag_type: 'fungi',
        description: 'Fly agaric, is poisonous',
      },
    });

    const element = testElement(mockDjClient);
    renderAddEditTagPage(element);

    await userEvent.type(screen.getByLabelText('Name'), 'amanita_muscaria');
    await userEvent.type(
      screen.getByLabelText('Display Name'),
      'Amanita Muscaria',
    );
    await userEvent.type(screen.getByLabelText('Tag Type'), 'fungi');
    await userEvent.click(screen.getByRole('button'));

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.addTag).toBeCalled();
      expect(mockDjClient.DataJunctionAPI.addTag).toBeCalledWith(
        'amanita_muscaria',
        'Amanita Muscaria',
        'fungi',
        undefined,
      );
      expect(screen.getByTestId('success')).toHaveTextContent(
        'Successfully added tag Amanita Muscaria',
      );
    });
  }, 60000);

  it('fails to add a tag', async () => {
    mockDjClient.DataJunctionAPI.addTag.mockReturnValue({
      status: 500,
      json: { message: 'Tag exists' },
    });

    const element = testElement(mockDjClient);
    renderAddEditTagPage(element);

    await userEvent.click(screen.getByRole('button'));

    await userEvent.type(screen.getByLabelText('Name'), 'amanita_muscaria');
    await userEvent.type(
      screen.getByLabelText('Display Name'),
      'Amanita Muscaria',
    );
    await userEvent.type(screen.getByLabelText('Tag Type'), 'fungi');
    await userEvent.click(screen.getByRole('button'));

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.addTag).toBeCalled();
      expect(screen.getByTestId('failure')).toHaveTextContent(
        'alert_fillTag exists',
      );
    });
  }, 60000);
});
