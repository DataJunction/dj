/**
 * Metric unit select component
 */
import { ErrorMessage, Field } from 'formik';
import { useContext, useMemo, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import { labelize } from '../../../utils/form';

export const MetricMetadataFields = () => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;

  // Metric metadata
  const [metricUnits, setMetricUnits] = useState([]);
  const [metricDirections, setMetricDirections] = useState([]);

  // Get metric metadata values
  useMemo(() => {
    const fetchData = async () => {
      const metadata = await djClient.listMetricMetadata();
      setMetricDirections(metadata.directions);
      setMetricUnits(metadata.units);
    };
    fetchData().catch(console.error);
  }, [djClient]);

  return (
    <>
      <div
        className="MetricDirectionInput NodeCreationInput"
        style={{ width: '25%' }}
      >
        <ErrorMessage name="metric_direction" component="span" />
        <label htmlFor="MetricDirection">Metric Direction</label>
        <Field as="select" name="metric_direction" id="MetricDirection">
          <option value=""></option>
          {metricDirections.map(direction => (
            <option value={direction} key={direction}>
              {labelize(direction)}
            </option>
          ))}
        </Field>
      </div>
      <div
        className="MetricUnitInput NodeCreationInput"
        style={{ width: '25%' }}
      >
        <ErrorMessage name="metric_unit" component="span" />
        <label htmlFor="MetricUnit">Metric Unit</label>
        <Field as="select" name="metric_unit" id="MetricUnit">
          <option value=""></option>
          {metricUnits.map(unit => (
            <option value={unit.name} key={unit.name}>
              {unit.label}
            </option>
          ))}
        </Field>
      </div>
    </>
  );
};
