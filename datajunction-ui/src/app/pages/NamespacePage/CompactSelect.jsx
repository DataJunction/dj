import Select from 'react-select';

// Compact select with label above - saves horizontal space
export default function CompactSelect({
  label,
  name,
  options,
  value,
  onChange,
  isMulti = false,
  isClearable = true,
  placeholder = 'Select...',
  minWidth = '100px',
  flex = 1,
  isLoading = false,
}) {
  // For single select, find the matching option
  // For multi select, filter to matching options
  const selectedValue = isMulti
    ? value?.length
      ? options.filter(o => value.includes(o.value))
      : []
    : value
    ? options.find(o => o.value === value)
    : null;

  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        gap: '2px',
        flex,
        minWidth,
      }}
    >
      <label
        style={{
          fontSize: '10px',
          fontWeight: '600',
          color: '#666',
          textTransform: 'uppercase',
          letterSpacing: '0.5px',
        }}
      >
        {label}
      </label>
      <Select
        name={name}
        isClearable={isClearable}
        isMulti={isMulti}
        isLoading={isLoading}
        placeholder={placeholder}
        onChange={onChange}
        value={selectedValue}
        styles={{
          control: base => ({
            ...base,
            minHeight: '32px',
            height: isMulti ? 'auto' : '32px',
            fontSize: '12px',
            backgroundColor: 'white',
          }),
          valueContainer: base => ({
            ...base,
            padding: '0 6px',
          }),
          input: base => ({
            ...base,
            margin: 0,
            padding: 0,
          }),
          indicatorSeparator: () => ({
            display: 'none',
          }),
          dropdownIndicator: base => ({
            ...base,
            padding: '4px',
          }),
          clearIndicator: base => ({
            ...base,
            padding: '4px',
          }),
          option: base => ({
            ...base,
            fontSize: '12px',
            padding: '6px 10px',
          }),
          multiValue: base => ({
            ...base,
            fontSize: '11px',
          }),
        }}
        options={options}
      />
    </div>
  );
}
