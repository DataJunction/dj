import Select, { components } from 'react-select';

// react-select renders a DOM node per option with no virtualization, so a large
// list (e.g. all users) makes the menu sluggish to open/scroll. Cap how many
// options are rendered at once; react-select still filters the full list as the
// user types, so any option remains reachable by searching.
const MAX_RENDERED_OPTIONS = 100;

const WindowedMenuList = props => {
  const { children } = props;
  if (Array.isArray(children) && children.length > MAX_RENDERED_OPTIONS) {
    return (
      <components.MenuList {...props}>
        {children.slice(0, MAX_RENDERED_OPTIONS)}
        <div
          style={{
            padding: '6px 12px',
            fontSize: '11px',
            color: '#888',
          }}
        >
          Showing first {MAX_RENDERED_OPTIONS} of {children.length}. Type to
          narrow…
        </div>
      </components.MenuList>
    );
  }
  return <components.MenuList {...props}>{children}</components.MenuList>;
};

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
  testId = null,
  formatOptionLabel = undefined,
  onMenuOpen = undefined,
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
      data-testid={testId}
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
        onMenuOpen={onMenuOpen}
        value={selectedValue}
        formatOptionLabel={formatOptionLabel}
        components={{ MenuList: WindowedMenuList }}
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
