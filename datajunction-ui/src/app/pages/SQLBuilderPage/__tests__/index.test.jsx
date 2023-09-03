import { render, act, screen } from '@testing-library/react';
import { SQLBuilderPage } from '../index';
import DJClientContext from '../../../providers/djclient';
import {DataJunctionAPI} from "../../../services/DJService";

// Mocking DataJunctionAPI and DJClientContext
// jest.mock('../../../services/DJService', () => ({
//   DataJunctionAPI: {
//     metrics: jest.fn(() =>
//       Promise.resolve([
//         'default.num_repair_orders',
//         'default.avg_repair_price',
//         'default.total_repair_cost',
//         'default.avg_length_of_employment',
//         'default.total_repair_order_discounts',
//         'default.avg_repair_order_discounts',
//         'default.avg_time_to_dispatch',
//         'default.regional_repair_efficiency',
//       ]),
//     ),
//     commonDimensions: jest.fn(() => Promise.resolve([])),
//     // stream: jest.fn(() => /* mock SSE implementation */),
//     // data: jest.fn(() => /* mock data implementation */),
//     // sqls: jest.fn(() => /* mock SQL implementation */),
//   },
// }));

// jest.mock('../../../providers/djclient', () => ({
//   __esModule: true,
// const DataJunctionAPI = {
//   metrics: jest
//     .fn()
//     .mockImplementation(() => Promise.resolve([
//       'default.num_repair_orders',
//       'default.avg_repair_price',
//       'default.total_repair_cost',
//       'default.avg_length_of_employment',
//       'default.total_repair_order_discounts',
//       'default.avg_repair_order_discounts',
//       'default.avg_time_to_dispatch',
//       'default.regional_repair_efficiency',
//     ])),
//   commonDimensions: jest.fn(() => Promise.resolve([])),
//   // stream: jest.fn(() => /* mock SSE implementation */),
//   // data: jest.fn(() => /* mock data implementation */),
//   // sqls: jest.fn(() => /* mock SQL implementation */),
// };
// }));

test('fetches metrics and updates state', async () => {
  global.fetch = jest.fn(() =>
    Promise.resolve({
      json: () => Promise.resolve(['default.num_repair_orders']),
    }),
  );
  const val = await DataJunctionAPI.metrics();
  expect(val).toStrictEqual(['default.num_repair_orders']);

  // Render the component
  render(
    // <DJClientContext.Provider value={{ DataJunctionAPI }}>
    <SQLBuilderPage />,
    // </DJClientContext.Provider>,
  );

  // Let's isolate the specific useEffect call we want to test
  act(() => {
    // Trigger the useEffect
    // In this case, it would be triggered by rendering the component
  });

  // Check if the metrics state has been updated based on the mocked data
  // const metricsOptions = await screen.getByText('Metrics');
  const metricsOptions = screen.getAllByTitle('Select');
  expect(metricsOptions).toMatchSnapshot();
});
