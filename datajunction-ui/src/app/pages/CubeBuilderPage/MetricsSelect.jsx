/**
 * A select component for picking metrics.
 */
import { useField, useFormikContext } from 'formik';
import Select from 'react-select';
import React, { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';

export const MetricsSelect = ({ nodeName = null, className = '' }) => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const { values } = useFormikContext();

  // eslint-disable-next-line no-unused-vars
  const [field, _, helpers] = useField('metrics');
  const { setValue } = helpers;

  // All metrics options
  const [metrics, setMetrics] = useState([]);
  const [node, setNode] = useState(undefined);
  const [defaultMetrics, setDefaultMetrics] = useState([]);

  // Get metrics
  useEffect(() => {
    const fetchData = async () => {
      if (nodeName) {
        const cube = await djClient.cube(nodeName);
        setNode(cube);
        const cubeMetrics = cube?.cube_elements
          .filter(element => element.type === 'metric')
          .map(metric => {
            return {
              value: metric.node_name,
              label: metric.node_name,
            };
          });
        setDefaultMetrics(cubeMetrics);
        await setValue(cubeMetrics.map(m => m.value));
      }

      const metrics = await djClient.metrics();
      setMetrics(metrics.map(m => ({ value: m, label: m })));
      console.log('metrics', metrics);
    };
    fetchData().catch(console.error);
  }, [djClient, djClient.metrics, nodeName]);

  const getValue = options => {
    if (options) {
      return options.map(option => option.value);
    } else {
      return [];
    }
  };

  const render = () => {
    if (metrics.length > 0 || node !== undefined) {
      return (
        <Select
          className={className}
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
          isDisabled={!!(values.metrics.length && values.dimensions.length)}
        />
      );
    }
  };
  return render();
};
