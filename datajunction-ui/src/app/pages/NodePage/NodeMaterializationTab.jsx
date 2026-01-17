import { useEffect, useState, useMemo } from 'react';
import TableIcon from '../../icons/TableIcon';
import AddMaterializationPopover from './AddMaterializationPopover';
import * as React from 'react';
import AddBackfillPopover from './AddBackfillPopover';
import { labelize } from '../../../utils/form';
import NodeMaterializationDelete from '../../components/NodeMaterializationDelete';
import Tab from '../../components/Tab';
import NodeRevisionMaterializationTab from './NodeRevisionMaterializationTab';
import AvailabilityStateBlock from './AvailabilityStateBlock';

const cronstrue = require('cronstrue');

/**
 * Cube materialization tab - shows cube-specific materializations.
 * For non-cube nodes, the parent component (index.jsx) renders
 * NodePreAggregationsTab instead.
 */
export default function NodeMaterializationTab({ node, djClient }) {
  const [rawMaterializations, setRawMaterializations] = useState([]);
  const [selectedRevisionTab, setSelectedRevisionTab] = useState(null);
  const [showInactive, setShowInactive] = useState(false);
  const [availabilityStates, setAvailabilityStates] = useState([]);
  const [availabilityStatesByRevision, setAvailabilityStatesByRevision] =
    useState({});
  const [isRebuilding, setIsRebuilding] = useState(() => {
    // Check if we're in the middle of a rebuild operation
    return localStorage.getItem(`rebuilding-${node?.name}`) === 'true';
  });

  const filteredMaterializations = useMemo(() => {
    return showInactive
      ? rawMaterializations
      : rawMaterializations.filter(mat => !mat.deactivated_at);
  }, [rawMaterializations, showInactive]);

  const materializationsByRevision = useMemo(() => {
    return filteredMaterializations.reduce((acc, mat) => {
      // Extract version from materialization config
      const matVersion = mat.config?.cube?.version || node?.version;

      if (!acc[matVersion]) {
        acc[matVersion] = [];
      }
      acc[matVersion].push(mat);
      return acc;
    }, {});
  }, [filteredMaterializations, node?.version]);

  useEffect(() => {
    const fetchData = async () => {
      if (node) {
        const data = await djClient.materializations(node.name);

        // Store raw data
        setRawMaterializations(data);

        // Fetch availability states
        const availabilityData = await djClient.availabilityStates(node.name);
        setAvailabilityStates(availabilityData);

        // Group availability states by version
        const availabilityGrouped = availabilityData.reduce((acc, avail) => {
          const version = avail.node_version || node.version;
          if (!acc[version]) {
            acc[version] = [];
          }
          acc[version].push(avail);
          return acc;
        }, {});

        setAvailabilityStatesByRevision(availabilityGrouped);

        // Clear rebuilding state once data is loaded after a page reload
        if (localStorage.getItem(`rebuilding-${node.name}`) === 'true') {
          localStorage.removeItem(`rebuilding-${node.name}`);
          setIsRebuilding(false);
        }
      }
    };
    fetchData().catch(console.error);
  }, [djClient, node]);

  // Separate useEffect to set default selected tab
  useEffect(() => {
    if (
      !selectedRevisionTab &&
      Object.keys(materializationsByRevision).length > 0
    ) {
      // First try to find current node version
      if (materializationsByRevision[node?.version]) {
        setSelectedRevisionTab(node.version);
      } else {
        // Otherwise, select the most recent version (sort by version string)
        const sortedVersions = Object.keys(materializationsByRevision).sort(
          (a, b) => b.localeCompare(a),
        );
        setSelectedRevisionTab(sortedVersions[0]);
      }
    }
  }, [materializationsByRevision, selectedRevisionTab, node?.version]);

  const partitionColumnsMap = node
    ? Object.fromEntries(
        node?.columns
          .filter(col => col.partition !== null)
          .map(col => [col.name, col.display_name]),
      )
    : {};
  const cron = materialization => {
    var parsedCron = '';
    try {
      parsedCron = cronstrue.toString(materialization.schedule);
    } catch (e) {}
    return parsedCron;
  };

  const onClickRevisionTab = revisionId => () => {
    setSelectedRevisionTab(revisionId);
  };

  const buildRevisionTabs = () => {
    const versions = Object.keys(materializationsByRevision);

    // Check if there are any materializations at all (including inactive ones)
    const hasAnyMaterializations = rawMaterializations.length > 0;

    // Determine which versions have only inactive materializations
    const versionHasOnlyInactive = {};
    rawMaterializations.forEach(mat => {
      const matVersion = mat.config?.cube?.version || node.version;
      if (!versionHasOnlyInactive[matVersion]) {
        versionHasOnlyInactive[matVersion] = {
          hasActive: false,
          hasInactive: false,
        };
      }
      if (mat.deactivated_at) {
        versionHasOnlyInactive[matVersion].hasInactive = true;
      } else {
        versionHasOnlyInactive[matVersion].hasActive = true;
      }
    });

    // If no active versions but there are inactive materializations, show checkbox and button
    if (versions.length === 0) {
      return (
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'flex-end',
            marginBottom: '20px',
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', gap: '15px' }}>
            {hasAnyMaterializations && (
              <label
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: '5px',
                  fontSize: '14px',
                  color: '#333',
                  padding: '4px 8px',
                  borderRadius: '12px',
                  backgroundColor: '#f5f5f5',
                  border: '1px solid #ddd',
                }}
                title="Shows inactive materializations for the latest cube."
              >
                <input
                  type="checkbox"
                  checked={showInactive}
                  onChange={e => setShowInactive(e.target.checked)}
                />
                Show Inactive
              </label>
            )}
            {node && <AddMaterializationPopover node={node} />}
          </div>
        </div>
      );
    }

    // Sort versions: current version first, then by version string (most recent first)
    const sortedVersions = versions.sort((a, b) => {
      // Current node version always comes first
      if (a === node?.version) return -1;
      if (b === node?.version) return 1;

      // Then sort by version string (descending)
      return b.localeCompare(a);
    });

    // Check if latest version has active materializations
    const hasLatestVersionMaterialization =
      materializationsByRevision[node?.version] &&
      materializationsByRevision[node?.version].length > 0;

    // Refresh latest materialization function
    const refreshLatestMaterialization = async () => {
      if (
        !window.confirm(
          'This will create a new version of the cube and build new materialization workflows. The previous version of the cube and its materialization will be accessible using a specific version label. Would you like to continue?',
        )
      ) {
        return;
      }

      // Set loading state in both React state and localStorage
      setIsRebuilding(true);
      localStorage.setItem(`rebuilding-${node.name}`, 'true');

      try {
        const { status, json } = await djClient.refreshLatestMaterialization(
          node.name,
        );

        if (status === 200 || status === 201) {
          // Keep the loading state during page reload
          window.location.reload(); // Reload to show the updated materialization
        } else {
          alert(`Failed to rebuild materialization: ${json.message}`);
          // Clear loading state on error
          localStorage.removeItem(`rebuilding-${node.name}`);
          setIsRebuilding(false);
        }
      } catch (error) {
        alert(`Error rebuilding materialization: ${error.message}`);
        // Clear loading state on error
        localStorage.removeItem(`rebuilding-${node.name}`);
        setIsRebuilding(false);
      }
    };

    return (
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          marginBottom: '20px',
        }}
      >
        <div className="align-items-center row">
          {sortedVersions.map(version => (
            <NodeRevisionMaterializationTab
              key={version}
              version={version}
              node={node}
              selectedRevisionTab={selectedRevisionTab}
              onClickRevisionTab={onClickRevisionTab}
              showInactive={showInactive}
              versionHasOnlyInactive={versionHasOnlyInactive}
            />
          ))}
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '15px' }}>
          <label
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: '5px',
              fontSize: '14px',
              color: '#333',
              padding: '4px 8px',
              borderRadius: '12px',
              backgroundColor: '#f5f5f5',
              border: '1px solid #ddd',
            }}
            title="Shows inactive materializations for the latest cube."
          >
            <input
              type="checkbox"
              checked={showInactive}
              onChange={e => setShowInactive(e.target.checked)}
            />
            Show Inactive
          </label>
          {node &&
            (hasLatestVersionMaterialization ? (
              <button
                className="edit_button"
                aria-label="RefreshLatestMaterialization"
                tabIndex="0"
                onClick={refreshLatestMaterialization}
                disabled={isRebuilding}
                title="Create a new version of the cube and re-create its materialization workflows."
                style={{
                  opacity: isRebuilding ? 0.7 : 1,
                  cursor: isRebuilding ? 'not-allowed' : 'pointer',
                }}
              >
                <span className="add_node">
                  Rebuild (latest) Materialization
                </span>
              </button>
            ) : (
              <AddMaterializationPopover node={node} />
            ))}
        </div>
      </div>
    );
  };

  const materializationRows = materializations => {
    return materializations.map((materialization, index) => (
      <div key={`${materialization.name}-${index}`}>
        <div className="tr">
          <div key={materialization.name} style={{ fontSize: 'large' }}>
            <div
              className="text-start node_name td"
              style={{ fontWeight: '600' }}
            >
              {materialization.job
                ?.replace('MaterializationJob', '')
                .match(/[A-Z][a-z]+/g)
                .join(' ')}
            </div>
            <div className="td">
              <NodeMaterializationDelete
                nodeName={node.name}
                materializationName={materialization.name}
                nodeVersion={selectedRevisionTab}
              />
            </div>
            <div className="td">
              <span className={`badge cron`}>{materialization.schedule}</span>
              <div className={`cron-description`}>{cron(materialization)} </div>
            </div>
            <div className="td">
              <span className={`badge strategy`}>
                {labelize(materialization.strategy)}
              </span>
            </div>
          </div>
        </div>
        <div style={{ display: 'table-row' }}>
          <div style={{ display: 'inline-flex' }}>
            <ul className="backfills">
              <li className="backfill">
                <div className="backfills_header">Output Tables</div>{' '}
                {materialization.output_tables.map(table => (
                  <div className={`table__full`} key={table}>
                    <div className="table__header">
                      <TableIcon />{' '}
                      <span className={`entity-info`}>
                        {table.split('.')[0] + '.' + table.split('.')[1]}
                      </span>
                    </div>
                    <div className={`table__body upstream_tables`}>
                      {table.split('.')[2]}
                    </div>
                  </div>
                ))}
              </li>
            </ul>
          </div>

          <div style={{ display: 'inline-flex' }}>
            <ul className="backfills">
              <li>
                <div className="backfills_header">Workflows</div>{' '}
                <ul>
                  {materialization.urls.map((url, idx) => (
                    <li style={{ listStyle: 'none' }} key={idx}>
                      <div
                        className="partitionLink"
                        style={{ fontSize: 'revert' }}
                      >
                        <a
                          href={url}
                          key={`url-${idx}`}
                          className=""
                          target="blank"
                        >
                          {idx === 0 ? 'main' : 'backfill'}
                        </a>
                      </div>
                    </li>
                  ))}
                </ul>
              </li>
            </ul>
          </div>

          <div style={{ display: 'inline-flex' }}>
            <ul className="backfills">
              <li className="backfill">
                <details open>
                  <summary>
                    <span className="backfills_header">Backfills</span>{' '}
                  </summary>
                  {materialization.strategy === 'incremental_time' ? (
                    <ul>
                      <li>
                        <AddBackfillPopover
                          node={node}
                          materialization={materialization}
                        />
                      </li>
                      {materialization.backfills.map(backfill => (
                        <li className="backfill">
                          <div className="partitionLink">
                            <a href={backfill.urls[0]}>
                              {backfill.spec.map(partition => {
                                const partitionBody =
                                  'range' in partition &&
                                  partition['range'] !== null ? (
                                    <>
                                      <span className="badge partition_value">
                                        {partition.range[0]}
                                      </span>
                                      to
                                      <span className="badge partition_value">
                                        {partition.range[1]}
                                      </span>
                                    </>
                                  ) : (
                                    <span className="badge partition_value">
                                      {partition.values.join(', ')}
                                    </span>
                                  );
                                return (
                                  <>
                                    <div>
                                      {
                                        partitionColumnsMap[
                                          partition.column_name.replaceAll(
                                            '_DOT_',
                                            '.',
                                          )
                                        ]
                                      }{' '}
                                      {partitionBody}
                                    </div>
                                  </>
                                );
                              })}
                            </a>
                          </div>
                        </li>
                      ))}
                    </ul>
                  ) : (
                    <ul>
                      <li>N/A</li>
                    </ul>
                  )}
                </details>
              </li>
            </ul>
          </div>
          <div className="td">
            <ul className="backfills">
              <li className="backfill">
                <div className="backfills_header">Partitions</div>{' '}
                <ul>
                  {node.columns
                    .filter(col => col.partition !== null)
                    .map(column => {
                      return (
                        <li key={column.name}>
                          <div className="partitionLink">
                            {column.display_name}
                            <span className="badge partition_value">
                              {column.partition.type_}
                            </span>
                          </div>
                        </li>
                      );
                    })}
                </ul>
              </li>
            </ul>
          </div>
        </div>
      </div>
    ));
  };
  const currentRevisionMaterializations = selectedRevisionTab
    ? materializationsByRevision[selectedRevisionTab] || []
    : filteredMaterializations;

  const currentRevisionAvailability = selectedRevisionTab
    ? availabilityStatesByRevision[selectedRevisionTab] || []
    : availabilityStates;

  const renderMaterializedDatasets = availabilityStates => {
    if (!availabilityStates || availabilityStates.length === 0) {
      return (
        <div className="message alert" style={{ marginTop: '10px' }}>
          No materialized datasets available for this revision.
        </div>
      );
    }

    return availabilityStates.map((availability, index) => (
      <AvailabilityStateBlock
        key={`availability-${index}`}
        availability={availability}
      />
    ));
  };

  return (
    <>
      <div
        className="table-vertical"
        role="table"
        aria-label="Materializations"
        style={{ position: 'relative' }}
      >
        {/* Loading overlay */}
        {isRebuilding && (
          <div
            style={{
              position: 'absolute',
              top: 0,
              left: 0,
              right: 0,
              bottom: 0,
              backgroundColor: 'rgba(255, 255, 255, 0.8)',
              display: 'flex',
              flexDirection: 'column',
              justifyContent: 'center',
              alignItems: 'center',
              zIndex: 1000,
              minHeight: '200px',
            }}
          >
            <div
              style={{
                width: '40px',
                height: '40px',
                border: '4px solid #f3f3f3',
                borderTop: '4px solid #3498db',
                borderRadius: '50%',
                animation: 'spin 1s linear infinite',
                marginBottom: '16px',
              }}
            />
            <div
              style={{ fontSize: '16px', color: '#666', textAlign: 'center' }}
            >
              Rebuilding materialization...
              <br />
              <small style={{ fontSize: '14px' }}>
                This may take a few moments
              </small>
            </div>
          </div>
        )}

        <div>
          {buildRevisionTabs()}
          {currentRevisionMaterializations.length > 0 ? (
            <div
              className="card-inner-table table"
              aria-label="Materializations"
              aria-hidden="false"
            >
              <div style={{ display: 'table' }}>
                {materializationRows(
                  currentRevisionMaterializations.filter(
                    materialization =>
                      !(
                        materialization.name === 'default' &&
                        node.type === 'cube'
                      ),
                  ),
                )}
              </div>
            </div>
          ) : (
            <div className="message alert" style={{ marginTop: '10px' }}>
              No materialization workflows configured for this revision.
            </div>
          )}
          {Object.keys(materializationsByRevision).length > 0 && (
            <div style={{ marginTop: '30px' }}>
              {renderMaterializedDatasets(currentRevisionAvailability)}
            </div>
          )}
        </div>
      </div>
    </>
  );
}
