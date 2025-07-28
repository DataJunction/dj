/**
 * A select component for picking metrics.
 */
import { useField, useFormikContext } from 'formik';
import Select from 'react-select';
import React, { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';

export const MetricsSelect = ({ cube }) => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const { values } = useFormikContext();

  // eslint-disable-next-line no-unused-vars
  const [field, _, helpers] = useField('metrics');
  const { setValue } = helpers;

  // All metrics options
  const [metrics, setMetrics] = useState([]);

  // The existing cube's metrics, if editing a cube
  const [defaultMetrics, setDefaultMetrics] = useState([]);

  // Get metrics
  useEffect(() => {
    const fetchData = async () => {
      if (cube) {
        const cubeMetrics = cube?.current.cubeMetrics.map(metric => {
          return {
            value: metric.name,
            label: metric.name,
          };
        });
        setDefaultMetrics(cubeMetrics);
        await setValue(cubeMetrics.map(m => m.value));
      }

      const metrics = await djClient.metrics();
      setMetrics(metrics.map(m => ({ value: m, label: m })));
    };
    fetchData().catch(console.error);
  }, [djClient, djClient.metrics, cube]);

  const getValue = options => {
    if (options) {
      return options.map(option => option.value);
    } else {
      return [];
    }
  };

  const render = () => {
    if (
      metrics.length > 0 ||
      (cube !== undefined && defaultMetrics.length > 0 && metrics.length > 0)
    ) {
      return (
        <Select
          defaultValue={defaultMetrics}
          options={metrics}
          name="metrics"
          placeholder={`${metrics.length} Available Metrics`}
          onBlur={field.onBlur}
          onChange={selected => {
            setValue(getValue(selected));
          }}
          noOptionsMessage={() => 'No metrics found.'}
          isMulti
          isClearable
          closeMenuOnSelect={false}
        />
      );
    }
  };
  return render();
};
