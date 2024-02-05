import { ErrorMessage, Field } from 'formik';

export const NodeModeField = () => {
  return (
    <div className="NodeModeInput NodeCreationInput">
      <ErrorMessage name="mode" component="span" />
      <label htmlFor="Mode">Mode</label>
      <Field as="select" name="mode" id="Mode">
        <option value="draft">Draft</option>
        <option value="published">Published</option>
      </Field>
    </div>
  );
};
