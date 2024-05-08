import { useContext, useEffect, useState } from 'react';
import * as React from 'react';
import DJClientContext from '../../providers/djclient';
import CreatableSelect from 'react-select/creatable';
import { Field } from 'formik';
import EditIcon from '../../icons/EditIcon';

export default function DimensionFilter({ dimension, onChange }) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [dimensionValues, setDimensionValues] = useState([]);
  const [selected, setSelected] = useState([]);

  useEffect(() => {
    const fetchData = async () => {
      const dimensionNode = await djClient.node(dimension.metadata.node_name);
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
        const data = { results: [] }; //await djClient.nodeData(dimension.metadata.node_name);
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
          console.log('dimensionValues', dimValues);
          setDimensionValues(dimValues);
        }
      }
    };
    fetchData().catch(console.error);
  }, [dimension.metadata.node_name, djClient]);

  return (
    <div>
      {dimension.label.split('[').slice(0)[0]} (
      {
        <a href={`/nodes/${dimension.metadata.node_name}`}>
          {dimension.metadata.node_display_name}
        </a>
      }
      ){/*<code className="DimensionAttribute">{dimension.value}</code>*/}
      {/*{*/}
      {/*  <div className="tooltip">*/}
      {/*    <EditIcon />*/}
      {/*    <span className="tooltiptext">*/}
      {/*      <code className="DimensionAttribute">*/}
      {/*      {dimension.value}*/}
      {/*      </code>*/}
      {/*      comes from the dimension link on{' '}*/}
      {/*      {dimension.metadata.path.map(path => {*/}
      {/*        return <a href={`/nodes/${path}`}>{path}</a>;*/}
      {/*      })}*/}
      {/*      /!*{console.log('metadata', dimension.metadata)}*!/*/}
      {/*    </span>*/}
      {/*  </div>*/}
      {/*}*/}
      <CreatableSelect
        name="dimensions"
        options={dimensionValues}
        isMulti
        isClearable
        onChange={event => {
          setSelected(event);
          onChange({
            dimension: dimension.value,
            values: event,
          });
        }}
      />
    </div>
  );
}
