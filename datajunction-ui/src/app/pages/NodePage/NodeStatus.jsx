import { Component, useContext, useEffect, useRef, useState } from 'react';
import ValidIcon from '../../icons/ValidIcon';
import InvalidIcon from '../../icons/InvalidIcon';
import DJClientContext from '../../providers/djclient';
import { foundation } from 'react-syntax-highlighter/src/styles/hljs';
import { Light as SyntaxHighlighter } from 'react-syntax-highlighter';
import markdown from 'react-syntax-highlighter/dist/cjs/languages/hljs/markdown';
import * as React from 'react';
import { AlertMessage } from '../AddEditNodePage/AlertMessage';
import AlertIcon from '../../icons/AlertIcon';
import { labelize } from '../../../utils/form';

SyntaxHighlighter.registerLanguage('markdown', markdown);

export default function NodeStatus({ node, revalidate = true }) {
  const MAX_ERROR_LENGTH = 200;
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [validation, setValidation] = useState([]);

  const [codeAnchor, setCodeAnchor] = useState(false);
  const ref = useRef(null);

  useEffect(() => {
    if (revalidate) {
      const fetchData = async () => {
        setValidation(await djClient.revalidate(node.name));
      };
      fetchData().catch(console.error);
    }
  }, [djClient, node, revalidate]);

  const displayValidation =
    revalidate && validation?.errors?.length > 0 ? (
      <>
        <button
          className="badge"
          style={{
            backgroundColor: '#b34b00',
            fontSize: '15px',
            outline: '0',
            border: '0',
            cursor: 'pointer',
          }}
          onClick={() => setCodeAnchor(!codeAnchor)}
        >
          ⚠ {validation?.errors?.length} error
          {validation?.errors?.length > 1 ? 's' : ''}
        </button>
        <div
          className="popover"
          ref={ref}
          style={{
            display: codeAnchor === false ? 'none' : 'block',
            border: 'none',
            paddingTop: '0px !important',
            marginTop: '0px',
            backgroundColor: 'transparent',
          }}
        >
          {validation?.errors?.map((error, idx) => (
            <div className="validation_error">
              <b
                style={{
                  color: '#b34b00',
                }}
              >
                {labelize(error.type.toLowerCase())}:
              </b>{' '}
              {error.message.length > MAX_ERROR_LENGTH
                ? error.message.slice(0, MAX_ERROR_LENGTH - 1) + '...'
                : error.message}
            </div>
          ))}
        </div>
      </>
    ) : (
      <></>
    );

  return (
    <>
      {revalidate && validation?.errors?.length > 0 ? (
        displayValidation
      ) : validation?.status === 'valid' || node?.status === 'valid' ? (
        <span
          className="status__valid status"
          style={{ alignContent: 'center' }}
        >
          <ValidIcon />
        </span>
      ) : (
        <span
          className="status__invalid status"
          style={{ alignContent: 'center' }}
        >
          <InvalidIcon />
        </span>
      )}
    </>
  );
}
