import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import Control from './FieldControl';

import Select from 'react-select';

export default function TagSelect({ onChange, value }) {
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

  const options = tags?.map(tag => ({
    value: tag.name,
    label: tag.display_name,
  })) || [];

  // For multi-select, value is an array of tag names
  const selectedValues = value?.length 
    ? options.filter(o => value.includes(o.value))
    : [];

  return (
    <span
      className="menu-link"
      style={{ marginLeft: '30px', width: '350px' }}
      data-testid="select-tag"
    >
      <Select
        name="tags"
        isClearable
        isMulti
        label="Tags"
        components={{ Control }}
        onChange={e => onChange(e)}
        value={selectedValues}
        options={options}
      />
    </span>
  );
}
