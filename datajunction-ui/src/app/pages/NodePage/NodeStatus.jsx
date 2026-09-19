import { useContext, useEffect, useRef, useState } from 'react';
import ValidIcon from '../../icons/ValidIcon';
import InvalidIcon from '../../icons/InvalidIcon';
import DJClientContext from '../../providers/djclient';
import * as React from 'react';
import { labelize } from '../../../utils/form';
import Tooltip from '../../components/Tooltip';

export default function NodeStatus({
  node,
  revalidate = true,
  showLabel = false,
  validLabel = 'Valid',
  invalidLabel = 'Invalid',
  validTooltip = 'This node validates successfully.',
  invalidTooltip = 'This node has validation errors. Open it to see what needs to be fixed.',
}) {
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

  const isValid =
    validation?.status === 'valid' ||
    node?.status === 'valid' ||
    node?.current?.status === 'VALID';

  const badge = isValid ? (
    <span
      className="status__valid status"
      style={{
        alignContent: 'center',
        display: showLabel ? 'inline-flex' : undefined,
        alignItems: showLabel ? 'center' : undefined,
        gap: showLabel ? '6px' : undefined,
        fontWeight: showLabel ? 600 : undefined,
      }}
    >
      <ValidIcon
        width={showLabel ? '16px' : '25px'}
        height={showLabel ? '16px' : '25px'}
      />
      {showLabel ? validLabel : null}
    </span>
  ) : (
    <span
      className="status__invalid status"
      style={{
        alignContent: 'center',
        display: showLabel ? 'inline-flex' : undefined,
        alignItems: showLabel ? 'center' : undefined,
        gap: showLabel ? '6px' : undefined,
        fontWeight: showLabel ? 600 : undefined,
      }}
    >
      <InvalidIcon
        width={showLabel ? '16px' : '25px'}
        height={showLabel ? '16px' : '25px'}
      />
      {showLabel ? invalidLabel : null}
    </span>
  );

  return (
    <>
      {revalidate && validation?.errors?.length > 0 ? (
        displayValidation
      ) : showLabel ? (
        <Tooltip content={isValid ? validTooltip : invalidTooltip}>
          {badge}
        </Tooltip>
      ) : (
        badge
      )}
    </>
  );
}
