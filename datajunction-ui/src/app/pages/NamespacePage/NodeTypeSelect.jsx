import Select from 'react-select';
import Control from './FieldControl';

export default function NodeTypeSelect({ onChange }) {
  return (
    <span
      className="menu-link"
      style={{ marginLeft: '30px', width: '300px' }}
      data-testid="select-node-type"
    >
      <Select
        name="node_type"
        isClearable
        label="Type"
        components={{ Control }}
        onChange={e => onChange(e)}
        styles={{
          control: styles => ({ ...styles, backgroundColor: 'white' }),
        }}
        options={[
          { value: 'source', label: 'Source' },
          { value: 'transform', label: 'Transform' },
          { value: 'dimension', label: 'Dimension' },
          { value: 'metric', label: 'Metric' },
          { value: 'cube', label: 'Cube' },
        ]}
      />
    </span>
  );
}
