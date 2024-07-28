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
    <span className="menu-link" style={{ marginLeft: '30px', width: '350px' }} data-testid="select-tag">
      <Select
        name="tags"
        isClearable
        isMulti
        label="Tags"
        components={{ Control }}
        onChange={e => onChange(e)}
        options={tags?.map(tag => {
          return {
            value: tag.name,
            label: tag.display_name,
          };
        })}
      />
    </span>
  );
}
