import React from 'react';
import { fireEvent, screen, waitFor } from '@testing-library/react';
import fetchMock from 'jest-fetch-mock';
import userEvent from '@testing-library/user-event';
import { render } from '../../../../setupTests';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import DJClientContext from '../../../providers/djclient';
import { RegisterTablePage } from '../index';

describe('<RegisterTablePage />', () => {
  const initializeMockDJClient = () => {
    return {
      DataJunctionAPI: {
        catalogs: jest.fn(),
        registerTable: jest.fn(),
      },
    };
  };

  const mockDjClient = initializeMockDJClient();

  beforeEach(() => {
    fetchMock.resetMocks();
    jest.clearAllMocks();
    window.scrollTo = jest.fn();

    mockDjClient.DataJunctionAPI.catalogs.mockReturnValue([
      {
        name: 'warehouse',
        engines: [
          {
            name: 'duckdb',
            version: '0.7.1',
            uri: null,
            dialect: null,
          },
        ],
      },
    ]);
  });

  const renderRegisterTable = element => {
    return render(
      <MemoryRouter initialEntries={['/create/source']}>
        <Routes>
          <Route path="create/source" element={element} />
        </Routes>
      </MemoryRouter>,
    );
  };

  const testElement = djClient => {
    return (
      <DJClientContext.Provider value={djClient}>
        <RegisterTablePage />
      </DJClientContext.Provider>
    );
  };

  it('registers a table correctly', async () => {
    mockDjClient.DataJunctionAPI.registerTable.mockReturnValue({
      status: 201,
      json: { name: 'source.warehouse.schema.some_table' },
    });

    const element = testElement(mockDjClient);
    const { container, getByTestId } = renderRegisterTable(element);

    const catalog = getByTestId('choose-catalog');
    await waitFor(async () => {
      fireEvent.keyDown(catalog.firstChild, { key: 'ArrowDown' });
      const warehouseOptions = screen.getAllByText('warehouse');
      fireEvent.click(warehouseOptions[warehouseOptions.length - 1]);
    });

    await userEvent.type(screen.getByLabelText('Schema'), 'schema');
    await userEvent.type(screen.getByLabelText('Table'), 'some_table');
    await userEvent.click(screen.getByRole('button'));

    await waitFor(() => {
      expect(mockDjClient.DataJunctionAPI.registerTable).toBeCalled();
      expect(mockDjClient.DataJunctionAPI.registerTable).toBeCalledWith(
        'warehouse',
        'schema',
        'some_table',
      );
    });
    expect(container.getElementsByClassName('message')).toMatchSnapshot();
  }, 60000);

  it('fails to register a table', async () => {
    mockDjClient.DataJunctionAPI.registerTable.mockReturnValue({
      status: 500,
      json: { message: 'table not found' },
    });

    const element = testElement(mockDjClient);
    const { getByTestId } = renderRegisterTable(element);

    const catalog = getByTestId('choose-catalog');
    await waitFor(async () => {
      fireEvent.keyDown(catalog.firstChild, { key: 'ArrowDown' });
      const warehouseOptions = screen.getAllByText('warehouse');
      fireEvent.click(warehouseOptions[warehouseOptions.length - 1]);
    });

    await userEvent.type(screen.getByLabelText('Schema'), 'schema');
    await userEvent.type(screen.getByLabelText('Table'), 'some_table');
    await userEvent.click(screen.getByRole('button'));
    expect(screen.getByText('table not found')).toBeInTheDocument();
  }, 60000);
});
