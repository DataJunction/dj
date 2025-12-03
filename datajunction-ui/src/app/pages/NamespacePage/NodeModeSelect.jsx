import Select from 'react-select';
import Control from './FieldControl';

export default function NodeModeSelect({ onChange }) {
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
        styles={{
          control: styles => ({ ...styles, backgroundColor: 'white' }),
        }}
        options={[
          { value: 'published', label: 'Published' },
          { value: 'draft', label: 'Draft' },
        ]}
      />
    </span>
  );
}
