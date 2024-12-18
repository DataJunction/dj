/**
 * A select component for picking metrics.
 */
import { useField, useFormikContext } from 'formik';
import Select, { components } from 'react-select';
import { SortableContainer, SortableElement, arrayMove, SortableHandle } from 'react-sortable-hoc';
import React, { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';

// function arrayMove(array, from, to) {
//   const slicedArray = array.slice();
//   slicedArray.splice(
//     to < 0 ? array.length + to : to,
//     0,
//     slicedArray.splice(from, 1)[0]
//   );
//   return slicedArray;
// }


const SortableMultiValue = SortableElement(
  (props) => {
    // this prevents the menu from being opened/closed when the user clicks
    // on a value to begin dragging it. ideally, detecting a click (instead of
    // a drag) would still focus the control and toggle the menu, but that
    // requires some magic with refs that are out of scope for this example
    const onMouseDown = (e) => {
      e.preventDefault();
      e.stopPropagation();
    };
    const innerProps = { ...props.innerProps, onMouseDown };
    return <components.MultiValue {...props} innerProps={innerProps} />;
  }
);

const SortableMultiValueLabel = SortableHandle(
  (props) => <components.MultiValueLabel {...props} />
);

const SortableSelect = SortableContainer(Select);

export const MetricsSelect = ({ cube }) => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const { values } = useFormikContext();

  // eslint-disable-next-line no-unused-vars
  const [field, _, helpers] = useField('metrics');
  const { setValue } = helpers;

  // All metrics options
  const [metrics, setMetrics] = useState([]);
  const [selected, setSelected] = useState([]);

  // The existing cube's metrics, if editing a cube
  const [defaultMetrics, setDefaultMetrics] = useState([]);

  const onSortEnd = ({ oldIndex, newIndex }) => {
    const newSelected = arrayMove(selected, oldIndex, newIndex);
    setSelected(newSelected);
    setValue(newSelected.map((item) => item)); // Update formik state
  };

  // Get metrics
  useEffect(() => {
    const fetchData = async () => {
      if (cube && cube !== []) {
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
        <SortableSelect
          useDragHandle
          // react-sortable-hoc props:
          axis="xy"
          onSortEnd={onSortEnd}
          distance={10}
          defaultValue={defaultMetrics}
          getHelperDimensions={({ node }) => node.getBoundingClientRect()}
          // react-select props:
          options={metrics}
          name="metrics"
          placeholder={`${metrics.length} Available Metrics`}
          onBlur={field.onBlur}
          onChange={(selectedOptions) => {
            setSelected(selectedOptions || []); // Update local state
            setValue(getValue(selectedOptions));
          }}
          noOptionsMessage={() => 'No metrics found.'}
          isMulti
          isClearable
          components={{
            MultiValue: SortableMultiValue,
            MultiValueLabel: SortableMultiValueLabel,
          }}
          closeMenuOnSelect={false}
        />
      );
    }
  };
  return render();
};
