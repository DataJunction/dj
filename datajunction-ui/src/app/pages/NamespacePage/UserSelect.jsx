import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import Control from './FieldControl';

import Select from 'react-select';

export default function UserSelect({ onChange, currentUser }) {
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

  return (
    <span className="menu-link" style={{ marginLeft: '30px', width: '400px' }} data-testid="select-user">
      {retrieved && currentUser ? (
        <Select
          name="edited_by"
          isClearable
          label="Edited By"
          components={{ Control }}
          onChange={e => onChange(e)}
          defaultValue={{
            value: currentUser,
            label: currentUser,
          }}
          options={users?.map(user => {
            return { value: user.username, label: user.username };
          })}
        />
      ) : (
        ''
      )}
    </span>
  );
}
