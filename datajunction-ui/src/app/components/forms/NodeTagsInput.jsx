import { ErrorMessage } from 'formik';
import { FormikSelect } from '../../pages/AddEditNodePage/FormikSelect';
import { Action } from './Action';
import { useContext, useEffect, useState } from 'react';
import DJClientContext from '../../providers/djclient';

export default function NodeTagsInput({ action, node }) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [tags, setTags] = useState([]);

  // Get list of tags
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
  }, [djClient, djClient.listTags]);

  return (
    <div
      className="TagsInput"
      style={{ width: '25%', margin: '1rem 0 1rem 1.2rem' }}
    >
      <ErrorMessage name="tags" component="span" />
      <label htmlFor="react-select-3-input">Tags</label>
      <span data-testid="select-tags">
        {action === Action.Edit && node?.tags?.length >= 0 ? (
          <FormikSelect
            className=""
            isMulti={true}
            selectOptions={tags}
            formikFieldName="tags"
            placeholder="Choose Tags"
            defaultValue={node?.tags?.map(t => {
              return { value: t.name, label: t.display_name };
            })}
          />
        ) : (
          ''
        )}
        {action === Action.Add ? (
          <FormikSelect
            className=""
            isMulti={true}
            selectOptions={tags}
            formikFieldName="tags"
            placeholder="Choose Tags"
          />
        ) : (
          ''
        )}
      </span>
    </div>
  );
}
