/**
 * Owner select field
 */
import { ErrorMessage } from 'formik';
import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import { FormikSelect } from './FormikSelect';

export const OwnersField = ({ defaultValue }) => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;

  const [availableUsers, setAvailableUsers] = useState([]);
  const [currentUser, setCurrentUser] = useState(null);

  useEffect(() => {
    async function fetchData() {
      const users = await djClient.users();
      setAvailableUsers(
        users.map(user => {
          return {
            value: user.username,
            label: user.username,
          };
        }),
      );
      const current = await djClient.whoami();
      setCurrentUser(current);
    }
    fetchData();
  }, [djClient]);

  return defaultValue || currentUser ? (
    <div className="NodeCreationInput">
      <ErrorMessage name="owners" component="span" />
      <label htmlFor="Owners">Owners</label>
      <span data-testid="select-owner">
        <FormikSelect
          className="MultiSelectInput"
          defaultValue={
            defaultValue || [
              { value: currentUser.username, label: currentUser.username },
            ]
          }
          selectOptions={availableUsers}
          formikFieldName="owners"
          placeholder="Select Owners"
          isMulti={true}
        />
      </span>
    </div>
  ) : (
    ''
  );
};
