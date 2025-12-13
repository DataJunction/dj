/**
 * Owner select field
 */
import { ErrorMessage } from 'formik';
import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import { useCurrentUser } from '../../providers/UserProvider';
import { FormikSelect } from './FormikSelect';

export const OwnersField = ({ defaultValue }) => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const { currentUser } = useCurrentUser();

  const [availableUsers, setAvailableUsers] = useState([]);

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
