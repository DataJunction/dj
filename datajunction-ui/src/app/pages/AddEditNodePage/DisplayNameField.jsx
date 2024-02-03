import { ErrorMessage, Field } from 'formik';

export const DisplayNameField = () => {
  return (
    <div className="DisplayNameInput NodeCreationInput">
      <ErrorMessage name="display_name" component="span" />
      <label htmlFor="displayName">Display Name *</label>
      <Field
        type="text"
        name="display_name"
        id="displayName"
        placeholder="Human readable display name"
      />
    </div>
  );
};
