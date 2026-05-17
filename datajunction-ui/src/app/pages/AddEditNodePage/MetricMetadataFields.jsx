/**
 * Metric unit select component
 */
import { ErrorMessage, Field } from 'formik';
import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import { labelize } from '../../../utils/form';

export const MetricMetadataFields = () => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;

  // Metric metadata
  const [metricUnits, setMetricUnits] = useState([]);
  const [metricDirections, setMetricDirections] = useState([]);

  // Get metric metadata values
  useEffect(() => {
    const fetchData = async () => {
      const metadata = await djClient.listMetricMetadata();
      setMetricDirections(metadata.directions);
      setMetricUnits(metadata.units);
    };
    fetchData().catch(console.error);
  }, [djClient]);

  return (
    <div className="MetricMetadataFields node-row">
      <div className="NodeCreationInput NodeModeInput">
        <ErrorMessage name="metric_direction" component="span" />
        <label htmlFor="MetricDirection">Direction</label>
        <Field as="select" name="metric_direction" id="MetricDirection">
          <option value=""></option>
          {metricDirections.map(direction => (
            <option value={direction} key={direction}>
              {labelize(direction)}
            </option>
          ))}
        </Field>
      </div>
      <div className="NodeCreationInput NodeModeInput">
        <ErrorMessage name="metric_unit" component="span" />
        <label htmlFor="MetricUnit">Unit</label>
        <Field as="select" name="metric_unit" id="MetricUnit">
          <option value=""></option>
          {metricUnits.map(unit => (
            <option value={unit.name} key={unit.name}>
              {unit.label}
            </option>
          ))}
        </Field>
      </div>
      <div className="NodeCreationInput NodeModeInput">
        <ErrorMessage name="significant_digits" component="span" />
        <label htmlFor="SignificantDigits">Significant Digits</label>
        <Field as="select" name="significant_digits" id="SignificantDigits">
          <option value=""></option>
          {[1, 2, 3, 4, 5, 6, 7, 8, 9, 10].map(val => (
            <option value={val} key={val}>
              {val}
            </option>
          ))}
        </Field>
      </div>
    </div>
  );
};
