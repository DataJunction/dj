/**
 * Query input field, which consists of a CodeMirror SQL editor with autocompletion
 * (for node names and columns) and syntax highlighting.
 */
import { useEffect, useState } from 'react';
import { Field, useFormikContext } from 'formik';
import CodeMirror from '@uiw/react-codemirror';
import { langs } from '@uiw/codemirror-extensions-langs';

export const NodeQueryField = ({ djClient }) => {
  const [schema, setSchema] = useState([]);
  const formik = useFormikContext();

  useEffect(() => {
    const fetchData = async () => {
      const nodes = await djClient.namespace('default');
      const schema = {};
      for (const node of nodes) {
        const nodeDetails = await djClient.node(node.name);
        schema[node.name] = nodeDetails.columns.map(col => col.name);
      }
      setSchema(schema);
    };
    fetchData().catch(console.error);
  }, [djClient, djClient.namespace]);

  return (
    <>
      <Field
        type="textarea"
        style={{ display: 'none' }}
        as="textarea"
        name="query"
        id="Query"
      />
      <div role="button" tabIndex={0} className="relative flex bg-[#282a36]">
        <CodeMirror
          id={'query'}
          name={'query'}
          extensions={[
            langs.sql({
              schema: schema,
            }),
          ]}
          options={{
            theme: 'default',
            lineNumbers: true,
          }}
          width="100%"
          height="400px"
          style={{
            margin: '0 0 23px 0',
            flex: 1,
            fontSize: '150%',
            textAlign: 'left',
          }}
          onChange={val => {
            formik.setFieldValue('query', val);
          }}
        />
      </div>
    </>
  );
};
