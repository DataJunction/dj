import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import Control from './FieldControl';

import Select from 'react-select';

export default function TagSelect({ onChange }) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;

  const [retrieved, setRetrieved] = useState(false);
  const [tags, setTags] = useState([]);

  useEffect(() => {
    const fetchData = async () => {
      const tags = await djClient.listTags();
      setTags(tags);
      setRetrieved(true);
    };
    fetchData().catch(console.error);
  }, [djClient]);

  return (
    <span className="menu-link" style={{ marginLeft: '30px', width: '350px' }}>
      <Select
        name="tags"
        isClearable
        isMulti
        label="Tags"
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
            whiteSpace: 'nowrap',
            overflow: 'hidden',
          }),
          multiValue: (styles, { data }) => {
            return {
              ...styles,
              backgroundColor: data.backgroundColor,
            };
          },
          multiValueLabel: (styles, { data }) => ({
            ...styles,
            color: data.color,
          }),
        }}
        options={tags?.map(tag => {
          return {
            value: tag.name,
            label: tag.display_name,
            backgroundColor: '#eee',
          };
        })}
      />
    </span>
  );
}
