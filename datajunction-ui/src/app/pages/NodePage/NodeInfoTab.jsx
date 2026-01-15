import { useState, useContext, useEffect } from 'react';
import { Light as SyntaxHighlighter } from 'react-syntax-highlighter';
import { foundation } from 'react-syntax-highlighter/src/styles/hljs';
import sql from 'react-syntax-highlighter/dist/esm/languages/hljs/sql';
import NodeStatus from './NodeStatus';
import ToggleSwitch from '../../components/ToggleSwitch';
import DJClientContext from '../../providers/djclient';
import { labelize } from '../../../utils/form';

SyntaxHighlighter.registerLanguage('sql', sql);

export default function NodeInfoTab({ node }) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [compiledSQL, setCompiledSQL] = useState('');
  const [showCompiledSQL, setShowCompiledSQL] = useState(false);
  const [metricInfo, setMetricInfo] = useState(null);
  const [cubeElements, setCubeElements] = useState(null);

  useEffect(() => {
    const fetchCompiledSQL = async () => {
      if (showCompiledSQL) {
        const data = await djClient.compiledSql(node.name);
        setCompiledSQL(data.sql || '/* Error generating compiled SQL */');
      }
    };
    fetchCompiledSQL().catch(console.error);
  }, [node, djClient, showCompiledSQL]);

  useEffect(() => {
    const fetchMetricInfo = async () => {
      const metric = await djClient.getMetric(node.name);
      setMetricInfo({
        metric_metadata: metric.current.metricMetadata,
        required_dimensions: metric.current.requiredDimensions,
        upstream_node: metric.current.parents[0]?.name,
        expression: metric.current.metricMetadata?.expression,
        incompatible_druid_functions:
          metric.current.metricMetadata?.incompatibleDruidFunctions || [],
      });
    };
    if (node.type === 'metric') {
      fetchMetricInfo().catch(console.error);
    }
  }, [node, djClient]);

  useEffect(() => {
    const fetchCubeElements = async () => {
      const cube = await djClient.cube(node.name);
      setCubeElements(cube.cube_elements);
    };
    if (node.type === 'cube') {
      fetchCubeElements().catch(console.error);
    }
  }, [node, djClient]);

  const formatDate = dateString => {
    const date = new Date(dateString);
    return date.toLocaleDateString('en-US', {
      day: 'numeric',
      month: 'long',
      year: 'numeric',
    });
  };

  const DruidWarning = () => {
    if (
      node?.type !== 'metric' ||
      !metricInfo?.incompatible_druid_functions?.length
    ) {
      return null;
    }

    return (
      <div className="metadata-warning">
        <span className="metadata-warning-icon">⚠️</span>
        <div className="metadata-warning-content">
          The following functions may not be compatible with Druid SQL:
          <ul>
            {metricInfo.incompatible_druid_functions.map(func => (
              <li key={func}>
                <code>{func}</code>
              </li>
            ))}
          </ul>
          See{' '}
          <a href="https://druid.apache.org/docs/latest/querying/sql-functions/">
            Druid supported functions
          </a>
          .
        </div>
      </div>
    );
  };

  const renderOwners = () => {
    if (!node?.owners?.length) return '—';
    return (
      <div className="metadata-tags-inline">
        {node.owners.map(owner => (
          <span key={owner.username} className="metadata-tag owner">
            @{owner.username}
          </span>
        ))}
      </div>
    );
  };

  const renderTags = () => {
    if (!node?.tags?.length) return '—';
    return (
      <div className="metadata-tags-inline">
        {node.tags.map(tag => (
          <a key={tag.name} href={`/tags/${tag.name}`} className="metadata-tag">
            {tag.display_name}
          </a>
        ))}
      </div>
    );
  };

  const renderPrimaryKey = () => {
    if (node?.type === 'metric') {
      if (!node?.required_dimensions?.length) return '—';
      return (
        <div className="metadata-tags-inline">
          {node.required_dimensions.map(dim => (
            <span key={dim.name} className="metadata-tag primary-key">
              {dim.name}
            </span>
          ))}
        </div>
      );
    }
    if (!node?.primary_key?.length) return '—';
    return (
      <div className="metadata-tags-inline">
        {node.primary_key.map(pk => (
          <span key={pk} className="metadata-tag primary-key">
            {pk}
          </span>
        ))}
      </div>
    );
  };

  return (
    <div className="metadata-section">
      <DruidWarning />

      {/* General Information Card */}
      <div className="metadata-card">
        <div className="metadata-card-title">General Information</div>
        
        {/* Summary row with key stats */}
        <div className="metadata-summary-row">
          <div className="metadata-summary-item">
            <NodeStatus node={node} />
          </div>
          <span className="metadata-summary-divider">·</span>
          <div className="metadata-summary-item">
            <span className="summary-label">Mode</span>
            <span className="summary-value">{node?.mode || '—'}</span>
          </div>
          <span className="metadata-summary-divider">·</span>
          <div className="metadata-summary-item">
            <span className="summary-label">Version</span>
            <span className="summary-value monospace">{node?.version}</span>
          </div>
          <span className="metadata-summary-divider">·</span>
          <div className="metadata-summary-item">
            <span className="summary-label">Updated</span>
            <span className="summary-value">{formatDate(node?.updated_at)}</span>
          </div>
          {node?.type === 'source' && (
            <>
              <span className="metadata-summary-divider">·</span>
              <div className="metadata-summary-item">
                <span className="summary-label">Table</span>
                <span className="summary-value monospace">
                  {node?.catalog?.name}.{node?.schema_}.{node?.table}
                </span>
              </div>
            </>
          )}
        </div>

        {/* Details grid */}
        <div className="metadata-grid">
          <div className="metadata-field-stacked">
            <span className="field-label">Owners</span>
            <span className="field-value">{renderOwners()}</span>
          </div>
          <div className="metadata-field-stacked">
            <span className="field-label">Tags</span>
            <span className="field-value">{renderTags()}</span>
          </div>
          <div className="metadata-field-stacked metadata-field-full">
            <span className="field-label">
              {node?.type === 'metric' ? 'Required Dimensions' : 'Primary Key'}
            </span>
            <span className="field-value">{renderPrimaryKey()}</span>
          </div>
        </div>
      </div>

      {/* Metric Properties Card (metrics only) */}
      {node?.type === 'metric' && metricInfo && (
        <div className="metadata-card">
          <div className="metadata-card-title">Metric Properties</div>
          <div className="metadata-grid">
            <div className="metadata-field-stacked">
              <span className="field-label">Direction</span>
              <span className="field-value">
                {metricInfo?.metric_metadata?.direction
                  ? labelize(metricInfo.metric_metadata.direction.toLowerCase())
                  : '—'}
              </span>
            </div>
            <div className="metadata-field-stacked">
              <span className="field-label">Unit</span>
              <span className="field-value">
                {metricInfo?.metric_metadata?.unit?.name
                  ? labelize(metricInfo.metric_metadata.unit.name.toLowerCase())
                  : '—'}
              </span>
            </div>
            <div className="metadata-field-stacked">
              <span className="field-label">Significant Digits</span>
              <span className="field-value">
                {metricInfo?.metric_metadata?.significantDigits || '—'}
              </span>
            </div>
            <div className="metadata-field-stacked">
              <span className="field-label">Upstream Node</span>
              <span className="field-value">
                <a href={`/nodes/${metricInfo?.upstream_node}`}>
                  {metricInfo?.upstream_node}
                </a>
              </span>
            </div>
          </div>
        </div>
      )}

      {/* Description Card */}
      <div className="metadata-text-card">
        <div className="card-header">
          <span className="card-title">Description</span>
        </div>
        <div className="card-content">{node?.description}</div>
      </div>

      {/* Aggregate Expression Card (metrics only) */}
      {node?.type === 'metric' && metricInfo?.expression && (
        <div className="metadata-text-card">
          <div className="card-header">
            <span className="card-title">Aggregate Expression</span>
          </div>
          <div className="metadata-code-block">
            <SyntaxHighlighter
              language="sql"
              style={foundation}
              customStyle={{
                margin: 0,
                padding: '16px',
                fontSize: '13px',
              }}
            >
              {metricInfo.expression}
            </SyntaxHighlighter>
          </div>
        </div>
      )}

      {/* Query Card (non-cube nodes) */}
      {node?.query && node?.type !== 'cube' && (
        <div className="metadata-text-card">
          <div className="card-header">
            <span className="card-title">Query</span>
            {['metric', 'dimension', 'transform'].includes(node?.type) && (
              <ToggleSwitch
                id="compiledSqlToggle"
                checked={showCompiledSQL}
                onChange={() => setShowCompiledSQL(prev => !prev)}
                toggleName="Show Compiled"
              />
            )}
          </div>
          <div className="metadata-code-block">
            <SyntaxHighlighter
              language="sql"
              style={foundation}
              customStyle={{
                margin: 0,
                padding: '16px',
                fontSize: '13px',
                maxHeight: '400px',
                overflow: 'auto',
              }}
            >
              {showCompiledSQL ? compiledSQL : node.query}
            </SyntaxHighlighter>
          </div>
        </div>
      )}

      {/* Custom Metadata Card */}
      {node?.custom_metadata &&
        Object.keys(node.custom_metadata).length > 0 && (
          <div className="metadata-text-card">
            <div className="card-header">
              <span className="card-title">Custom Metadata</span>
            </div>
            <div className="metadata-code-block">
              <SyntaxHighlighter
                language="json"
                style={foundation}
                customStyle={{
                  margin: 0,
                  padding: '16px',
                  fontSize: '13px',
                }}
              >
                {JSON.stringify(node.custom_metadata, null, 2)}
              </SyntaxHighlighter>
            </div>
          </div>
        )}

      {/* Cube Elements Card */}
      {cubeElements && (
        <div className="metadata-card">
          <div className="metadata-card-title">Cube Elements</div>
          <div className="metadata-grid">
            {cubeElements.filter(el => el.type === 'metric').length > 0 && (
              <div className="metadata-field-stacked metadata-field-full">
                <span className="field-label">
                  Metrics ({cubeElements.filter(el => el.type === 'metric').length})
                </span>
                <div className="cube-elements-grid">
                  {cubeElements
                    .filter(el => el.type === 'metric')
                    .map(el => (
                      <a
                        key={el.name}
                        href={`/nodes/${el.node_name}`}
                        className="cube-element-chip"
                      >
                        <span>{el.display_name}</span>
                        <span className="chip-type metric">metric</span>
                      </a>
                    ))}
                </div>
              </div>
            )}
            {cubeElements.filter(el => el.type !== 'metric').length > 0 && (
              <div className="metadata-field-stacked metadata-field-full">
                <span className="field-label">
                  Dimensions ({cubeElements.filter(el => el.type !== 'metric').length})
                </span>
                <div className="cube-elements-grid">
                  {cubeElements
                    .filter(el => el.type !== 'metric')
                    .map(el => (
                      <a
                        key={el.name}
                        href={`/nodes/${el.node_name}`}
                        className="cube-element-chip"
                      >
                        <span>
                          {labelize(el.node_name.split('.').slice(-1)[0])} →{' '}
                          {el.display_name}
                        </span>
                        <span className="chip-type dimension">dimension</span>
                      </a>
                    ))}
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
