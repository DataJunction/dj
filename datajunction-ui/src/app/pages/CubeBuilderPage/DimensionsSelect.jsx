/**
 * A select component for picking dimensions
 */
import { useField, useFormikContext } from 'formik';
import Select from 'react-select';
import React, { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import { labelize } from '../../../utils/form';

export const DimensionsSelect = ({ cube }) => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const { values, setFieldValue } = useFormikContext();

  // eslint-disable-next-line no-unused-vars
  const [field, _, helpers] = useField('dimensions');
  const { setValue } = helpers;

  // All common dimensions for the selected metrics, grouped by the dimension node and path
  const [allDimensionsOptions, setAllDimensionsOptions] = useState([]);

  // The selected dimensions, also grouped by dimension node and path
  const [selectedDimensionsByGroup, setSelectedDimensionsByGroup] = useState(
    {},
  );

  // The existing cube node's dimensions, if editing a cube
  const [defaultDimensions, setDefaultDimensions] = useState([]);

  useEffect(() => {
    const fetchData = async () => {
      let cubeDimensions = undefined;
      if (cube) {
        cubeDimensions = cube?.cube_elements
          .filter(element => element.type === 'dimension')
          .map(cubeDim => {
            return {
              value: cubeDim.node_name + '.' + cubeDim.name,
              label: labelize(cubeDim.name),
            };
          });
        setDefaultDimensions(cubeDimensions);
        setValue(cubeDimensions.map(m => m.value));
      }

      if (values.metrics && values.metrics.length > 0) {
        // Populate the common dimensions list based on the selected metrics
        const commonDimensions = await djClient.commonDimensions(
          values.metrics,
        );
        const grouped = Object.entries(
          commonDimensions.reduce((group, dimension) => {
            group[dimension.node_name + dimension.path] =
              group[dimension.node_name + dimension.path] ?? [];
            group[dimension.node_name + dimension.path].push(dimension);
            return group;
          }, {}),
        );
        setAllDimensionsOptions(grouped);

        // Set the selected cube dimensions if an existing cube is being edited
        if (cube) {
          const currentSelectedDimensionsByGroup = selectedDimensionsByGroup;
          grouped.forEach(grouping => {
            const dimensionsInGroup = grouping[1];
            currentSelectedDimensionsByGroup[dimensionsInGroup[0].node_name] =
              getValue(
                cubeDimensions.filter(
                  dim =>
                    dimensionsInGroup.filter(x => {
                      return dim.value === x.name;
                    }).length > 0,
                ),
              );
            setSelectedDimensionsByGroup(currentSelectedDimensionsByGroup);
            setValue(Object.values(currentSelectedDimensionsByGroup).flat(2));
          });
        }
      } else {
        setAllDimensionsOptions([]);
      }
    };
    fetchData().catch(console.error);
  }, [djClient, setFieldValue, setValue, values.metrics, cube]);

  // Retrieves the selected values as a list (since it is a multi-select)
  const getValue = options => {
    if (options) {
      return options.map(option => option.value);
    } else {
      return [];
    }
  };

  // Builds the block of dimensions selectors, grouped by node name + path
  return allDimensionsOptions.map(grouping => {
    const dimensionsInGroup = grouping[1];
    const groupHeader = (
      <h5
        style={{
          fontWeight: 'normal',
          marginBottom: '5px',
          marginTop: '15px',
        }}
      >
        <a href={`/nodes/${dimensionsInGroup[0].node_name}`}>
          <b>{dimensionsInGroup[0].node_display_name}</b>
        </a>{' '}
        via{' '}
        <span className="HighlightPath">
          {dimensionsInGroup[0].path.join(' → ')}
        </span>
      </h5>
    );
    const dimensionGroupOptions = dimensionsInGroup.map(dim => {
      return {
        value: dim.name,
        label: labelize(dim.name.split('.').slice(-1)[0]),
      };
    });
    //
    const cubeDimensions = defaultDimensions.filter(
      dim =>
        dimensionGroupOptions.filter(x => {
          return dim.value === x.value;
        }).length > 0,
    );
    return (
      <>
        {groupHeader}
        <span data-testid={'dimensions-' + dimensionsInGroup[0].node_name}>
          <Select
            className=""
            name={'dimensions-' + dimensionsInGroup[0].node_name}
            defaultValue={cubeDimensions}
            options={dimensionGroupOptions}
            isMulti
            isClearable
            closeMenuOnSelect={false}
            onChange={selected => {
              selectedDimensionsByGroup[dimensionsInGroup[0].node_name] =
                getValue(selected);
              setSelectedDimensionsByGroup(selectedDimensionsByGroup);
              setValue(Object.values(selectedDimensionsByGroup).flat(2));
            }}
          />
        </span>
      </>
    );
  });
};
