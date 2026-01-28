import { useState, useContext, useEffect } from 'react';
import { Light as SyntaxHighlighter } from 'react-syntax-highlighter';
import { foundation } from 'react-syntax-highlighter/src/styles/hljs';
import sql from 'react-syntax-highlighter/dist/esm/languages/hljs/sql';
import NodeStatus from './NodeStatus';
import ListGroupItem from '../../components/ListGroupItem';
import DJClientContext from '../../providers/djclient';
import { labelize } from '../../../utils/form';

SyntaxHighlighter.registerLanguage('sql', sql);
foundation.hljs['padding'] = '2rem';

// interface MetricInfo {
//   name: string;
//   current: MetricRevision;
// }

// interface MetricRevision {
//   parents: array<string>;
//   metricMetadata:
// }

// interface MetricMetadata {
//   direction: string;
//   unit: string;
//   expression: string;
//   significantDigits: string;
//   incompatibleDruidFunctions: array<string>;
// }

export default function NodeInfoTab({ node }) {
  // For metrics
  const [metricInfo, setMetricInfo] = useState(null);

  const nodeTags = node?.tags.map(tag => (
    <span
      key={tag.name}
      className={'badge tag_value'}
      style={{ marginRight: '4px' }}
    >
      <a href={`/tags/${tag.name}`}>{tag.display_name}</a>
    </span>
  ));
  const djClient = useContext(DJClientContext).DataJunctionAPI;

  useEffect(() => {
    const fetchData = async () => {
      const metric = await djClient.getMetric(node.name);
      // For derived metrics, don't show upstream_node
      const parents = metric.current.parents || [];
      const upstreamNode = !metric.current.isDerivedMetric && parents.length === 1
        ? parents[0]?.name
        : null;
      setMetricInfo({
        metric_metadata: metric.current.metricMetadata,
        required_dimensions: metric.current.requiredDimensions,
        upstream_node: upstreamNode,
        expression: metric.current.metricMetadata?.expression,
        incompatible_druid_functions:
          metric.current.metricMetadata?.incompatibleDruidFunctions || [],
      });
    };
    if (node.type === 'metric') {
      fetchData().catch(console.error);
    }
  }, [node, djClient]);

  // For cubes
  const [cubeElements, setCubeElements] = useState(null);

  useEffect(() => {
    const fetchData = async () => {
      const cube = await djClient.cube(node.name);
      setCubeElements(cube.cube_elements);
    };
    if (node.type === 'cube') {
      fetchData().catch(console.error);
    }
  }, [node, djClient]);

  const metricsWarning =
    node?.type === 'metric' &&
    metricInfo?.incompatible_druid_functions?.length > 0 ? (
      <div className="message warning" style={{ marginTop: '0.7rem' }}>
        ⚠{' '}
        <small>
          The following functions used in the metric definition may not be
          compatible with Druid SQL:{' '}
          {metricInfo?.incompatible_druid_functions.map(func => (
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
          {metricInfo?.upstream_node && (
            <div style={{ marginBottom: '30px' }}>
              <h6 className="mb-0 w-100">Upstream Node</h6>
              <p>
                <a href={`/nodes/${metricInfo?.upstream_node}`}>
                  {metricInfo?.upstream_node}
                </a>
              </p>
            </div>
          )}
          <div>
            <h6 className="mb-0 w-100">Aggregate Expression</h6>
            <SyntaxHighlighter
              language="sql"
              style={foundation}
              wrapLongLines={true}
            >
              {metricInfo?.expression}
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
          <SyntaxHighlighter
            language="sql"
            style={foundation}
            wrapLongLines={true}
          >
            {node?.query}
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
          <div style={{ marginRight: '2rem' }}>
            <h6 className="mb-0 w-100">Output Type</h6>
            <p
              className="mb-0 opacity-75"
              role="dialog"
              aria-hidden="false"
              aria-label="OutputType"
            >
              <code
                style={{
                  background: '#f5f5f5',
                  padding: '2px 6px',
                  borderRadius: '3px',
                }}
              >
                {node?.columns?.[0]?.type || 'Unknown'}
              </code>
            </p>
          </div>
          <div style={{ marginRight: '2rem' }}>
            <h6 className="mb-0 w-100">Direction</h6>
            <p
              className="mb-0 opacity-75"
              role="dialog"
              aria-hidden="false"
              aria-label="MetricDirection"
            >
              {metricInfo?.metric_metadata?.direction
                ? labelize(
                    metricInfo?.metric_metadata?.direction?.toLowerCase(),
                  )
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
              {metricInfo?.metric_metadata?.unit?.name
                ? labelize(
                    metricInfo?.metric_metadata?.unit?.name?.toLowerCase(),
                  )
                : 'None'}
            </p>
          </div>
          <div style={{ marginRight: '2rem' }}>
            <h6 className="mb-0 w-100">Significant Digits</h6>
            <p
              className="mb-0 opacity-75"
              role="dialog"
              aria-hidden="false"
              aria-label="SignificantDigits"
            >
              {metricInfo?.metric_metadata?.significantDigits || 'None'}
            </p>
          </div>
        </div>
      </div>
    ) : (
      ''
    );

  const customMetadataDiv =
    node?.custom_metadata && Object.keys(node.custom_metadata).length > 0 ? (
      <div className="list-group-item d-flex">
        <div className="d-flex gap-2 w-100 justify-content-between py-3">
          <div
            style={{
              width: window.innerWidth * 0.8,
            }}
          >
            <h6 className="mb-0 w-100">Custom Metadata</h6>
            <SyntaxHighlighter
              language="json"
              style={foundation}
              wrapLongLines={true}
            >
              {JSON.stringify(node.custom_metadata, null, 2)}
            </SyntaxHighlighter>
          </div>
        </div>
      </div>
    ) : (
      ''
    );

  const cubeElementsDiv = cubeElements ? (
    <div className="list-group-item d-flex">
      <div className="d-flex gap-2 w-100 justify-content-between py-3">
        <div
          style={{
            width: window.innerWidth * 0.8,
          }}
        >
          <h6 className="mb-0 w-100">Cube Elements</h6>
          <div className={`list-group-item`}>
            {cubeElements.map(cubeElem =>
              cubeElem.type === 'metric' ? displayCubeElement(cubeElem) : '',
            )}
            {cubeElements.map(cubeElem =>
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
          ? node?.primary_key?.map((dim, idx) => (
              <span
                key={`pk-${idx}`}
                className="rounded-pill badge bg-secondary-soft PrimaryKey"
              >
                <a href={`/nodes/${node?.name}`}>{dim}</a>
              </span>
            ))
          : metricInfo?.required_dimensions?.map((dim, idx) => (
              <span
                key={`rd-${idx}`}
                className="rounded-pill badge bg-secondary-soft PrimaryKey"
              >
                {metricInfo?.upstream_node ? (
                  <a href={`/nodes/${metricInfo?.upstream_node}`}>{dim.name}</a>
                ) : (
                  dim.name
                )}
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
            <h6 className="mb-0 w-100">Owners</h6>
            <p className="mb-0 opacity-75">
              {node?.owners.map(owner => (
                <span
                  key={owner.username}
                  className="badge node_type__transform"
                  style={{ margin: '2px', fontSize: '16px', cursor: 'pointer' }}
                >
                  {owner.username}
                </span>
              ))}
            </p>
          </div>
        </div>
      </div>
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
      {node.type !== 'cube' && node.type !== 'metric' ? queryDiv : ''}
      {node.type === 'metric' ? metricQueryDiv : ''}
      {customMetadataDiv}
      {cubeElementsDiv}
    </div>
  );
}
