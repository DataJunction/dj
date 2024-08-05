import { components } from 'react-select';

const Control = ({ children, ...props }) => {
  const { label, onLabelClick } = props.selectProps;
  const style = {
    cursor: 'pointer',
    padding: '10px 5px 10px 12px',
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

export default Control;
