import Select from 'react-select';
// import {FieldControl} from './FieldControl';

import Select, {
  components,
} from 'react-select';

export default function GroupBySelect() {

  const Control = ({ children, ...props }) => {
    const { label, onLabelClick } = props.selectProps;
    const style = { cursor: 'pointer', padding: '10px 5px 10px 12px',
    color: 'rgb(112, 110, 115)',
  };
  
    return (
      <components.Control {...props}>
        <span onMouseDown={onLabelClick} style={style}>
          {label}
        </span>
        {children}
      </components.Control>
    );
  };
  
  return (
    <span className="menu-link" style={{width: '270px'}}>
      <Select
        name="groupby"
        isClearable
        label='Group By'
        components={{ Control }}
        defaultValue={{value: 'namespace', label: 'Namespace'}}
        options={[
          {value: 'cube', label: 'Cube'},
          {value: 'namespace', label: 'Namespace'},
          {value: 'type', label: 'Node Type'},
        ]}
      />
  </span>
  );
};