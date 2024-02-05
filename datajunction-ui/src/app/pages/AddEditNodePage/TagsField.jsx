/**
 * Tags select field
 */
import { ErrorMessage } from 'formik';
import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import { FormikSelect } from './FormikSelect';

export const TagsField = ({ defaultValue }) => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;

  // All available tags
  const [tags, setTags] = useState([]);

  useEffect(() => {
    const fetchData = async () => {
      const tags = await djClient.listTags();
      setTags(
        tags.map(tag => ({
          value: tag.name,
          label: tag.display_name,
        })),
      );
    };
    fetchData().catch(console.error);
  }, [djClient]);

  return (
    <div
      className="TagsInput"
      style={{ width: '25%', margin: '1rem 0 1rem 1.2rem' }}
    >
      <ErrorMessage name="tags" component="span" />
      <label htmlFor="react-select-3-input">Tags</label>
      <span data-testid="select-tags">
        <FormikSelect
          isMulti={true}
          selectOptions={tags}
          formikFieldName="tags"
          className="MultiSelectInput"
          placeholder="Choose Tags"
          defaultValue={defaultValue}
        />
      </span>
    </div>
  );
};
