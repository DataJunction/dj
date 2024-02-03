import { ErrorMessage, Field } from 'formik';

export const DescriptionField = () => {
  return (
    <div className="DescriptionInput NodeCreationInput">
      <ErrorMessage name="description" component="span" />
      <label htmlFor="Description">Description</label>
      <Field
        type="textarea"
        as="textarea"
        name="description"
        id="Description"
        placeholder="Describe your node"
      />
    </div>
  );
};
