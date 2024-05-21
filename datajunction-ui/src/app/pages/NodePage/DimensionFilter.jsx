import { useContext, useEffect, useState } from 'react';
import * as React from 'react';
import DJClientContext from '../../providers/djclient';
import CreatableSelect from 'react-select/creatable';

export default function DimensionFilter({ dimension, onChange }) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [dimensionValues, setDimensionValues] = useState([]);

  useEffect(() => {
    const fetchData = async () => {
      const dimensionNode = await djClient.node(dimension.metadata.node_name);

      // Only include the primary keys as filterable dimensions for now, until we figure out how
      // to build a manageable UI experience around all dimensional attributes
      if (dimensionNode && dimensionNode.type === 'dimension') {
        const primaryKey =
          dimensionNode.name +
          '.' +
          dimensionNode.columns
            .filter(col =>
              col.attributes.some(
                attr => attr.attribute_type.name === 'primary_key',
              ),
            )
            .map(col => col.name);
        const label =
          dimensionNode.name +
          '.' +
          dimensionNode.columns
            .filter(col =>
              col.attributes.some(attr => attr.attribute_type.name === 'label'),
            )
            .map(col => col.name);

        // TODO: we're disabling this for now because it's unclear how performant the dimensions node
        // data endpoints are. To re-enable, uncomment the following line:
        // const data = await djClient.nodeData(dimension.metadata.node_name);
        const data = { results: [] };

        // TODO: when the above is enabled, this will use each dimension node's 'Label' column
        // to build the display label for the dropdown, while continuing to pass the primary
        // key in for filtering
        /* istanbul ignore if */
        if (dimensionNode && data.results && data.results.length > 0) {
          const columnNames = data.results[0].columns.map(
            column => column.semantic_entity,
          );
          const dimValues = data.results[0].rows.map(row => {
            const rowData = { value: '', label: '' };
            row.forEach((value, index) => {
              if (columnNames[index] === primaryKey) {
                rowData.value = value;
                if (rowData.label === '') {
                  rowData.label = value;
                }
              } else if (columnNames[index] === label) {
                rowData.label = value;
              }
            });
            return rowData;
          });
          setDimensionValues(dimValues);
        }
      }
    };
    fetchData().catch(console.error);
  }, [dimension.metadata.node_name, djClient]);

  return (
    <div key={dimension.metadata.node_name}>
      {dimension.label.split('[').slice(0)[0]} (
      {
        <a href={`/nodes/${dimension.metadata.node_name}`}>
          {dimension.metadata.node_display_name}
        </a>
      }
      )
      <CreatableSelect
        name="dimensions"
        options={dimensionValues}
        isMulti
        isClearable
        onChange={event => {
          onChange({
            dimension: dimension.value,
            values: event,
          });
        }}
      />
    </div>
  );
}
