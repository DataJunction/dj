import Select from 'react-select';
import Control from './FieldControl';

const options = [
  { value: 'published', label: 'Published' },
  { value: 'draft', label: 'Draft' },
];

export default function NodeModeSelect({ onChange, value }) {
  return (
    <span
      className="menu-link"
      style={{ marginLeft: '30px', width: '300px' }}
      data-testid="select-node-mode"
    >
      <Select
        name="node_mode"
        isClearable
        label="Mode"
        components={{ Control }}
        onChange={e => onChange(e)}
        value={value ? options.find(o => o.value === value) : null}
        styles={{
          control: styles => ({ ...styles, backgroundColor: 'white' }),
        }}
        options={options}
      />
    </span>
  );
}
