import { useState, useContext, useEffect } from 'react';
import { Light as SyntaxHighlighter } from 'react-syntax-highlighter';
import { foundation } from 'react-syntax-highlighter/src/styles/hljs';
import sql from 'react-syntax-highlighter/dist/esm/languages/hljs/sql';
import NodeStatus from './NodeStatus';
import ListGroupItem from '../../components/ListGroupItem';
import ToggleSwitch from '../../components/ToggleSwitch';
import DJClientContext from '../../providers/djclient';
import { labelize } from '../../../utils/form';

SyntaxHighlighter.registerLanguage('sql', sql);
foundation.hljs['padding'] = '2rem';

export default function NodeInfoTab({ node }) {
  const [compiledSQL, setCompiledSQL] = useState('');
  const [checked, setChecked] = useState(false);
  const nodeTags = node?.tags.map(tag => (
    <div className={'badge tag_value'}>
      <a href={`/tags/${tag.name}`}>{tag.display_name}</a>
    </div>
  ));
  const djClient = useContext(DJClientContext).DataJunctionAPI;

  useEffect(() => {
    const fetchData = async () => {
      if (checked === true) {
        const data = await djClient.compiledSql(node.name);
        if (data.sql) {
          setCompiledSQL(data.sql);
        } else {
          setCompiledSQL(
            '/* Ran into an issue while generating compiled SQL */',
          );
        }
      }
    };
    fetchData().catch(console.error);
  }, [node, djClient, checked]);
  function toggle(value) {
    return !value;
  }
  const metricsWarning =
    node?.type === 'metric' && node?.incompatible_druid_functions.length > 0 ? (
      <div className="message warning" style={{ marginTop: '0.7rem' }}>
        ⚠{' '}
        <small>
          The following functions used in the metric definition may not be
          compatible with Druid SQL:{' '}
          {node?.incompatible_druid_functions.map(func => (
            <li
              style={{ listStyleType: 'none', margin: '0.7rem 0.7rem' }}
              key={func}
            >
              ⇢{' '}
              <span style={{ background: '#fff', padding: '0.3rem' }}>
                {func}
              </span>
            </li>
          ))}
          If you need your metrics to be compatible with Druid, please use{' '}
          <a
            href={
              'https://druid.apache.org/docs/latest/querying/sql-functions/'
            }
          >
            these supported functions
          </a>
          .
        </small>
      </div>
    ) : (
      ''
    );

  const metricQueryDiv =
    node?.type === 'metric' ? (
      <div className="list-group-item d-flex">
        <div className="gap-2 w-100 justify-content-between py-3">
          <div style={{ marginBottom: '30px' }}>
            <h6 className="mb-0 w-100">Upstream Node</h6>
            <p>
              <a href={`/nodes/${node?.upstream_node}`}>
                {node?.upstream_node}
              </a>
            </p>
          </div>
          <div>
            <h6 className="mb-0 w-100">Aggregate Expression</h6>
            <SyntaxHighlighter language="sql" style={foundation}>
              {node?.expression}
            </SyntaxHighlighter>
          </div>
        </div>
      </div>
    ) : (
      ''
    );
  const queryDiv = node?.query ? (
    <div className="list-group-item d-flex">
      <div className="d-flex gap-2 w-100 justify-content-between py-3">
        <div
          style={{
            width: window.innerWidth * 0.8,
          }}
        >
          <h6 className="mb-0 w-100">Query</h6>
          {['metric', 'dimension', 'transform'].indexOf(node?.type) > -1 ? (
            <ToggleSwitch
              id="toggleSwitch"
              checked={checked}
              onChange={() => setChecked(toggle)}
              toggleName="Show Compiled SQL"
            />
          ) : (
            <></>
          )}
          <SyntaxHighlighter language="sql" style={foundation}>
            {checked ? compiledSQL : node?.query}
          </SyntaxHighlighter>
        </div>
      </div>
    </div>
  ) : (
    <></>
  );

  const displayCubeElement = cubeElem => {
    return (
      <div
        className="button-3 cube-element"
        key={cubeElem.name}
        role="cell"
        aria-label="CubeElement"
        aria-hidden="false"
      >
        <a href={`/nodes/${cubeElem.node_name}`}>
          {cubeElem.type === 'dimension'
            ? labelize(cubeElem.node_name.split('.').slice(-1)[0]) + ' → '
            : ''}
          {cubeElem.display_name}
        </a>
        <span
          className={`badge node_type__${
            cubeElem.type === 'metric' ? cubeElem.type : 'dimension'
          }`}
          style={{ fontSize: '100%', textTransform: 'capitalize' }}
        >
          {cubeElem.type === 'metric' ? cubeElem.type : 'dimension'}
        </span>
      </div>
    );
  };

  const metricMetadataDiv =
    node?.type === 'metric' ? (
      <div className="list-group-item d-flex">
        <div className="d-flex gap-2 w-100 py-3">
          <div>
            <h6 className="mb-0 w-100">Direction</h6>
            <p
              className="mb-0 opacity-75"
              role="dialog"
              aria-hidden="false"
              aria-label="MetricDirection"
            >
              {node?.metric_metadata?.direction
                ? labelize(node?.metric_metadata?.direction?.toLowerCase())
                : 'None'}
            </p>
          </div>
          <div style={{ marginRight: '2rem' }}>
            <h6 className="mb-0 w-100">Unit</h6>
            <p
              className="mb-0 opacity-75"
              role="dialog"
              aria-hidden="false"
              aria-label="MetricUnit"
            >
              {node?.metric_metadata?.unit?.name
                ? labelize(node?.metric_metadata?.unit?.name?.toLowerCase())
                : 'None'}
            </p>
          </div>
          {console.log('node?.metric_metadata', node?.metric_metadata)}
          <div style={{ marginRight: '2rem' }}>
            <h6 className="mb-0 w-100">Significant Digits</h6>
            <p
              className="mb-0 opacity-75"
              role="dialog"
              aria-hidden="false"
              aria-label="SignificantDigits"
            >
              {node?.metric_metadata?.significantDigits || 'None'}
            </p>
          </div>
        </div>
      </div>
    ) : (
      ''
    );

  const cubeElementsDiv = node?.cube_elements ? (
    <div className="list-group-item d-flex">
      <div className="d-flex gap-2 w-100 justify-content-between py-3">
        <div
          style={{
            width: window.innerWidth * 0.8,
          }}
        >
          <h6 className="mb-0 w-100">Cube Elements</h6>
          <div className={`list-group-item`}>
            {node.cube_elements.map(cubeElem =>
              cubeElem.type === 'metric' ? displayCubeElement(cubeElem) : '',
            )}
            {node.cube_elements.map(cubeElem =>
              cubeElem.type !== 'metric' ? displayCubeElement(cubeElem) : '',
            )}
          </div>
        </div>
      </div>
    </div>
  ) : (
    <></>
  );

  const primaryKeyOrRequiredDims = (
    <div style={{ maxWidth: '25%' }}>
      <h6 className="mb-0 w-100">
        {node?.type !== 'metric' ? 'Primary Key' : 'Required Dimensions'}
      </h6>
      <p
        className="mb-0 opacity-75"
        role="dialog"
        aria-hidden="false"
        aria-label={
          node?.type !== 'metric' ? 'PrimaryKey' : 'RequiredDimensions'
        }
      >
        {node?.type !== 'metric'
          ? node?.primary_key?.map(dim => (
              <span className="rounded-pill badge bg-secondary-soft PrimaryKey">
                <a href={`/nodes/${node?.name}`}>{dim}</a>
              </span>
            ))
          : node?.required_dimensions?.map(dim => (
              <span className="rounded-pill badge bg-secondary-soft PrimaryKey">
                <a href={`/nodes/${node?.upstream_node}`}>{dim.name}</a>
              </span>
            ))}
      </p>
    </div>
  );
  return (
    <div
      className="list-group align-items-center justify-content-between flex-md-row gap-2"
      style={{ minWidth: '700px' }}
    >
      {node?.type === 'metric' ? metricsWarning : ''}
      <ListGroupItem label="Description" value={node?.description} />
      <div className="list-group-item d-flex">
        <div className="d-flex gap-2 w-100 justify-content-between py-3">
          <div>
            <h6 className="mb-0 w-100">Version</h6>

            <p className="mb-0 opacity-75">
              <span
                className="rounded-pill badge bg-secondary-soft"
                style={{ marginLeft: '0.5rem', fontSize: '100%' }}
                role="dialog"
                aria-hidden="false"
                aria-label="Version"
              >
                {node?.version}
              </span>
            </p>
          </div>
          {node.type === 'source' ? (
            <div>
              <h6 className="mb-0 w-100">Table</h6>
              <p
                className="mb-0 opacity-75"
                role="dialog"
                aria-hidden="false"
                aria-label="Table"
              >
                {node?.catalog.name}.{node?.schema_}.{node?.table}
              </p>
            </div>
          ) : (
            <></>
          )}
          <div>
            <h6 className="mb-0 w-100">Status</h6>
            <p
              className="mb-0 opacity-75"
              role="dialog"
              aria-hidden="false"
              aria-label="NodeStatus"
            >
              <NodeStatus node={node} />
            </p>
          </div>
          <div>
            <h6 className="mb-0 w-100">Mode</h6>
            <p className="mb-0 opacity-75">
              <span
                className="status"
                role="dialog"
                aria-hidden="false"
                aria-label="Mode"
              >
                {node?.mode}
              </span>
            </p>
          </div>
          <div>
            <h6 className="mb-0 w-100">Tags</h6>
            <p
              className="mb-0 opacity-75"
              role="dialog"
              aria-hidden="false"
              aria-label="Tags"
            >
              {nodeTags}
            </p>
          </div>
          {primaryKeyOrRequiredDims}
          <div>
            <h6 className="mb-0 w-100">Last Updated</h6>
            <p
              className="mb-0 opacity-75"
              role="dialog"
              aria-hidden="false"
              aria-label="UpdatedAt"
            >
              {new Date(node?.updated_at).toDateString()}
            </p>
          </div>
        </div>
      </div>
      {metricMetadataDiv}
      {node?.type !== 'cube' && node?.type !== 'metric' ? queryDiv : ''}
      {node?.type === 'metric' ? metricQueryDiv : ''}
      {cubeElementsDiv}
    </div>
  );
}
