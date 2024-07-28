import Select from 'react-select';
import Control from './FieldControl';

export default function NodeTypeSelect({ onChange }) {
  return (
    <span className="menu-link" style={{ marginLeft: '30px', width: '300px' }}>
      <Select
        name="node_type"
        isClearable
        // isMulti
        label="Node Type"
        components={{ Control }}
        onChange={e => onChange(e)}
        styles={{
          control: styles => ({ ...styles, backgroundColor: 'white' }),
          option: (styles, { data, isDisabled, isFocused, isSelected }) => {
            return {
              ...styles,
              color: data.color,
              cursor: isDisabled ? 'not-allowed' : 'default',
              className: `node_type__${data.value}`,
              ':active': {
                ...styles[':active'],
                backgroundColor: data.backgroundColor,
              },
            };
          },
          valueContainer: (provided, state) => ({
            ...provided,
            textOverflow: 'ellipsis',
            // maxWidth: "90%",
            whiteSpace: 'nowrap',
            overflow: 'hidden',
            // display: "initial"
          }),
          multiValue: (styles, { data }) => {
            return {
              ...styles,
              backgroundColor: data.backgroundColor,
              display: 'flex',
              verticalAlign: 'middle',
              borderRadius: '0.375rem',
              wordWrap: 'break-word',
              whiteSpace: 'break-spaces',
              textOverflow: 'ellipsis',
              marginRight: '0.5rem',
            };
          },
          multiValueLabel: (styles, { data }) => ({
            ...styles,
            color: data.color,
          }),
        }}
        options={[
          {
            value: 'source',
            label: 'Source',
            backgroundColor: '#ccf7e5',
            color: '#00b368',
          },
          {
            value: 'transform',
            label: 'Transform',
            backgroundColor: '#ccefff',
            color: '#0063b4',
          },
          {
            value: 'dimension',
            label: 'Dimension',
            backgroundColor: '#ffefd0',
            color: '#a96621',
          },
          {
            value: 'metric',
            label: 'Metric',
            backgroundColor: '#fad7dd',
            color: '#a2283e',
          },
          {
            value: 'cube',
            label: 'Cube',
            backgroundColor: '#dbafff',
            color: '#580076',
          },
        ]}
      />
    </span>
  );
}
