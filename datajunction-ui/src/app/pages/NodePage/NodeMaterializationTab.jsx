import { useCallback, useEffect, useState, useMemo } from 'react';
import AddMaterializationPopover from './AddMaterializationPopover';
import * as React from 'react';
import MaterializationStatePanel from './MaterializationStatePanel';
import { toMaterializationState } from './materializationState';

function ShowInactiveToggle({ checked, onChange }) {
  return (
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
        checked={checked}
        onChange={e => onChange(e.target.checked)}
      />
      Show Inactive
    </label>
  );
}

/**
 * Cube materialization tab - shows cube-specific materializations.
 * For non-cube nodes, the parent component (index.jsx) renders
 * NodePreAggregationsTab instead.
 */
export default function NodeMaterializationTab({
  node,
  djClient,
  readOnly = false,
}) {
  const [rawMaterializations, setRawMaterializations] = useState([]);
  const [selectedRevisionTab, setSelectedRevisionTab] = useState(null);
  const [showInactive, setShowInactive] = useState(false);
  const [availabilityStates, setAvailabilityStates] = useState([]);
  const [isRebuilding, setIsRebuilding] = useState(() => {
    // Check if we're in the middle of a rebuild operation
    return localStorage.getItem(`rebuilding-${node?.name}`) === 'true';
  });

  const filteredMaterializations = useMemo(() => {
    return showInactive
      ? rawMaterializations
      : rawMaterializations.filter(mat => !mat.deactivated_at);
  }, [rawMaterializations, showInactive]);

  const availabilityStatesByRevision = useMemo(() => {
    return availabilityStates.reduce((acc, avail) => {
      const version = avail.node_version || node?.version;
      if (!acc[version]) {
        acc[version] = [];
      }
      acc[version].push(avail);
      return acc;
    }, {});
  }, [availabilityStates, node?.version]);

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

  const fetchData = useCallback(async () => {
    if (node) {
      const data = await djClient.materializations(node.name);

      // Store raw data
      setRawMaterializations(data);

      // Fetch availability states
      const availabilityData = await djClient.availabilityStates(node.name);
      setAvailabilityStates(availabilityData);

      // Clear rebuilding state once data is loaded after a page reload
      if (localStorage.getItem(`rebuilding-${node.name}`) === 'true') {
        localStorage.removeItem(`rebuilding-${node.name}`);
        setIsRebuilding(false);
      }
    }
  }, [djClient, node]);

  useEffect(() => {
    fetchData().catch(console.error);
  }, [fetchData]);

  // Set default selected tab, or reset if current tab is no longer visible
  useEffect(() => {
    const versions = Object.keys(materializationsByRevision);
    if (versions.length === 0) return;

    if (
      !selectedRevisionTab ||
      !materializationsByRevision[selectedRevisionTab]
    ) {
      // First try to find current node version
      if (materializationsByRevision[node?.version]) {
        setSelectedRevisionTab(node.version);
      } else {
        // Otherwise, select the most recent version (sort by version string)
        const sortedVersions = versions.sort((a, b) => b.localeCompare(a));
        setSelectedRevisionTab(sortedVersions[0]);
      }
    }
  }, [materializationsByRevision, selectedRevisionTab, node?.version]);

  /**
   * The revision picker. A select rather than a tab strip: as tabs it was the same
   * blue-underline control as the page's own Info/Columns/... nav, sitting directly
   * beneath it, so neither row read as the primary navigation.
   */
  const renderVersionSelect = () => {
    const versions = Object.keys(materializationsByRevision).sort((a, b) => {
      if (a === node?.version) return -1;
      if (b === node?.version) return 1;
      return b.localeCompare(a);
    });
    if (!versions.length) {
      return null;
    }
    return (
      <label
        style={{ display: 'flex', alignItems: 'center', gap: '8px' }}
        htmlFor="materialization-version"
      >
        <span className="mat-version-label">Version</span>
        <select
          id="materialization-version"
          className="mat-version-select"
          value={selectedRevisionTab || ''}
          onChange={event => setSelectedRevisionTab(event.target.value)}
        >
          {versions.map(version => (
            <option key={version} value={version}>
              {version === node?.version ? `${version} (latest)` : version}
            </option>
          ))}
        </select>
      </label>
    );
  };

  const renderToolbar = () => {
    const versions = Object.keys(materializationsByRevision);

    // Check if there are any materializations at all (including inactive ones)
    const hasAnyMaterializations = rawMaterializations.length > 0;

    // If no active versions but there are inactive materializations, show checkbox and button
    if (versions.length === 0) {
      return (
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'flex-end',
            marginBottom: '20px',
            paddingTop: '16px',
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', gap: '15px' }}>
            {hasAnyMaterializations && (
              <ShowInactiveToggle
                checked={showInactive}
                onChange={setShowInactive}
              />
            )}
            {node && !readOnly && <AddMaterializationPopover node={node} />}
          </div>
        </div>
      );
    }

    // Check if latest version has any materializations (including inactive ones)
    const hasLatestVersionMaterialization = rawMaterializations.some(mat => {
      const matVersion = mat.config?.cube?.version || node?.version;
      return matVersion === node?.version;
    });

    // Refresh latest materialization function
    const refreshLatestMaterialization = async () => {
      if (
        !window.confirm(
          'This will rebuild the materialization workflows for the current cube version without creating a new version. Would you like to continue?',
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
          marginTop: '16px',
          marginBottom: '20px',
        }}
      >
        {/* The version select lives in the panel's own header, where the
            version's scope actually is. */}
        <span />
        <div style={{ display: 'flex', alignItems: 'center', gap: '15px' }}>
          <ShowInactiveToggle
            checked={showInactive}
            onChange={setShowInactive}
          />
          {node &&
            !readOnly &&
            (hasLatestVersionMaterialization ? (
              <button
                className="edit_button"
                aria-label="RefreshLatestMaterialization"
                tabIndex="0"
                onClick={refreshLatestMaterialization}
                disabled={isRebuilding}
                title="Rebuild the materialization workflows for the current cube version (no version bump)."
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

  const currentRevisionMaterializations = useMemo(
    () =>
      selectedRevisionTab
        ? materializationsByRevision[selectedRevisionTab] || []
        : filteredMaterializations,
    [selectedRevisionTab, materializationsByRevision, filteredMaterializations],
  );

  const currentRevisionAvailability = useMemo(
    () =>
      selectedRevisionTab
        ? availabilityStatesByRevision[selectedRevisionTab] || []
        : availabilityStates,
    [selectedRevisionTab, availabilityStatesByRevision, availabilityStates],
  );

  // The panel renders from this read model alone; the two responses are stitched
  // together here rather than in the component.
  const materializationState = useMemo(
    () =>
      toMaterializationState({
        node,
        materializations: currentRevisionMaterializations,
        availabilityStates: currentRevisionAvailability,
      }),
    [node, currentRevisionMaterializations, currentRevisionAvailability],
  );

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
          {renderToolbar()}
          <MaterializationStatePanel
            state={materializationState}
            versionSelect={renderVersionSelect()}
          />
        </div>
      </div>
    </>
  );
}
