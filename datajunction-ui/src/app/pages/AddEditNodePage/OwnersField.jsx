/**
 * Owner select field
 */
import { ErrorMessage, useField } from 'formik';
import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import { useCurrentUser } from '../../providers/UserProvider';
import { FormikSelect } from './FormikSelect';

export const OwnersField = ({ defaultValue }) => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const { currentUser } = useCurrentUser();
  const [, meta, helpers] = useField('owners');

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

  // Seed the Formik `owners` field — FormikSelect's `defaultValue` prop
  // is ignored because the inner <Select> uses Formik's field state.
  // - Edit mode: seed from `defaultValue` (option objects)
  // - Add mode: seed with currentUser so creators don't have to set
  //   themselves as an owner manually
  // Only seed if the field is still empty (the user hasn't touched it).
  useEffect(() => {
    if (meta.value && meta.value.length > 0) return;
    if (defaultValue && defaultValue.length > 0) {
      helpers.setValue(defaultValue.map(d => d.value));
    } else if (currentUser) {
      helpers.setValue([currentUser.username]);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [defaultValue, currentUser]);

  return defaultValue || currentUser ? (
    <div className="NodeCreationInput">
      <ErrorMessage name="owners" component="span" />
      <label htmlFor="Owners">Owners</label>
      <span data-testid="select-owner">
        <FormikSelect
          className=""
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
