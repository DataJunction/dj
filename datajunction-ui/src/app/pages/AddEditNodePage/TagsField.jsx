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
    <div className="TagsInput NodeCreationInput">
      <ErrorMessage name="tags" component="span" />
      <label htmlFor="tags">Tags</label>
      <span data-testid="select-tags">
        <FormikSelect
          isMulti={true}
          selectOptions={tags}
          formikFieldName="tags"
          className=""
          placeholder="Choose Tags"
          defaultValue={defaultValue}
        />
      </span>
    </div>
  );
};
