import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import Control from './FieldControl';

import Select from 'react-select';

export default function UserSelect({ onChange, value, currentUser }) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [retrieved, setRetrieved] = useState(false);
  const [users, setUsers] = useState([]);

  useEffect(() => {
    const fetchData = async () => {
      const users = await djClient.users();
      setUsers(users);
      setRetrieved(true);
    };
    fetchData().catch(console.error);
  }, [djClient]);

  const options = users?.map(user => ({
    value: user.username,
    label: user.username,
  })) || [];

  // Default to current user if no value specified and currentUser is provided
  const selectedValue = value 
    ? options.find(o => o.value === value) 
    : (currentUser ? options.find(o => o.value === currentUser) : null);

  return (
    <span
      className="menu-link"
      style={{ marginLeft: '30px', width: '400px' }}
      data-testid="select-user"
    >
      {retrieved ? (
        <Select
          name="edited_by"
          isClearable
          label="Edited By"
          components={{ Control }}
          onChange={e => onChange(e)}
          value={selectedValue}
          options={options}
        />
      ) : (
        ''
      )}
    </span>
  );
}
