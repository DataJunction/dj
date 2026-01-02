import { useState, useMemo, useEffect, useRef } from 'react';

/**
 * SelectionPanel - Browse and select metrics and dimensions
 */
export function SelectionPanel({
  metrics,
  selectedMetrics,
  onMetricsChange,
  dimensions,
  selectedDimensions,
  onDimensionsChange,
  loading,
}) {
  const [metricsSearch, setMetricsSearch] = useState('');
  const [dimensionsSearch, setDimensionsSearch] = useState('');
  const [expandedNamespaces, setExpandedNamespaces] = useState(new Set());
  const prevSearchRef = useRef('');

  // Get short name from full metric name
  const getShortName = fullName => {
    const parts = fullName.split('.');
    return parts[parts.length - 1];
  };

  // Get namespace from full metric name
  const getNamespace = fullName => {
    const parts = fullName.split('.');
    return parts.length > 1 ? parts.slice(0, -1).join('.') : 'default';
  };

  // Group metrics by namespace (e.g., "default.cube" -> "default")
  const groupedMetrics = useMemo(() => {
    const groups = {};
    metrics.forEach(metric => {
      const namespace = getNamespace(metric);
      if (!groups[namespace]) {
        groups[namespace] = [];
      }
      groups[namespace].push(metric);
    });
    return groups;
  }, [metrics]);

  // Filter and sort namespaces/metrics by search relevance
  // Namespaces matching the search term appear first, then sorted by metric matches
  const { filteredGroups, sortedNamespaces } = useMemo(() => {
    const search = metricsSearch.trim().toLowerCase();

    if (!search) {
      // No search - return original groups, sorted alphabetically
      const namespaces = Object.keys(groupedMetrics).sort();
      return { filteredGroups: groupedMetrics, sortedNamespaces: namespaces };
    }

    // Filter to groups that have matching metrics
    const filtered = {};
    Object.entries(groupedMetrics).forEach(([namespace, items]) => {
      const matchingItems = items.filter(m => m.toLowerCase().includes(search));
      if (matchingItems.length > 0) {
        // Sort metrics within namespace: prefix matches first
        matchingItems.sort((a, b) => {
          const aShort = getShortName(a).toLowerCase();
          const bShort = getShortName(b).toLowerCase();

          const aPrefix = aShort.startsWith(search);
          const bPrefix = bShort.startsWith(search);
          if (aPrefix && !bPrefix) return -1;
          if (!aPrefix && bPrefix) return 1;

          return aShort.localeCompare(bShort);
        });
        filtered[namespace] = matchingItems;
      }
    });

    // Sort namespaces by relevance
    const namespaces = Object.keys(filtered).sort((a, b) => {
      const aLower = a.toLowerCase();
      const bLower = b.toLowerCase();

      // Priority 1: Namespace starts with search term
      const aPrefix = aLower.startsWith(search);
      const bPrefix = bLower.startsWith(search);
      if (aPrefix && !bPrefix) return -1;
      if (!aPrefix && bPrefix) return 1;

      // Priority 2: Namespace contains search term
      const aContains = aLower.includes(search);
      const bContains = bLower.includes(search);
      if (aContains && !bContains) return -1;
      if (!aContains && bContains) return 1;

      // Priority 3: Has more matching metrics
      const aCount = filtered[a].length;
      const bCount = filtered[b].length;
      if (aCount !== bCount) return bCount - aCount;

      // Priority 4: Alphabetical
      return aLower.localeCompare(bLower);
    });

    return { filteredGroups: filtered, sortedNamespaces: namespaces };
  }, [groupedMetrics, metricsSearch]);

  // Auto-expand all matching namespaces when search changes
  useEffect(() => {
    const currentSearch = metricsSearch.trim();
    const prevSearch = prevSearchRef.current;

    // Only auto-expand when starting a new search or search term changes
    if (currentSearch && currentSearch !== prevSearch) {
      setExpandedNamespaces(new Set(sortedNamespaces));
    }

    prevSearchRef.current = currentSearch;
  }, [metricsSearch, sortedNamespaces]);

  // Dedupe dimensions by name, keeping shortest path for each
  const dedupedDimensions = useMemo(() => {
    const byName = new Map();
    dimensions.forEach(d => {
      if (!d.name) return;
      const existing = byName.get(d.name);
      if (
        !existing ||
        (d.path?.length || 0) < (existing.path?.length || Infinity)
      ) {
        byName.set(d.name, d);
      }
    });
    return Array.from(byName.values());
  }, [dimensions]);

  // Filter and sort dimensions by search (prefix matches first)
  const filteredDimensions = useMemo(() => {
    const search = dimensionsSearch.trim().toLowerCase();
    if (!search) return dedupedDimensions;

    // Search in both full name and short display name
    const matches = dedupedDimensions.filter(d => {
      if (!d.name) return false;
      const fullName = d.name.toLowerCase();
      const parts = d.name.split('.');
      const shortDisplay = parts.slice(-2).join('.').toLowerCase();
      return fullName.includes(search) || shortDisplay.includes(search);
    });

    // Sort: prefix matches on short name first
    matches.sort((a, b) => {
      const aParts = (a.name || '').split('.');
      const bParts = (b.name || '').split('.');
      const aShort = aParts.slice(-2).join('.').toLowerCase();
      const bShort = bParts.slice(-2).join('.').toLowerCase();

      const aPrefix = aShort.startsWith(search);
      const bPrefix = bShort.startsWith(search);
      if (aPrefix && !bPrefix) return -1;
      if (!aPrefix && bPrefix) return 1;

      return aShort.localeCompare(bShort);
    });

    return matches;
  }, [dedupedDimensions, dimensionsSearch]);

  // Get display name for dimension (last 2 segments: dim_node.column)
  const getDimDisplayName = fullName => {
    const parts = (fullName || '').split('.');
    return parts.slice(-2).join('.');
  };

  const toggleNamespace = namespace => {
    setExpandedNamespaces(prev => {
      const next = new Set(prev);
      if (next.has(namespace)) {
        next.delete(namespace);
      } else {
        next.add(namespace);
      }
      return next;
    });
  };

  const toggleMetric = metric => {
    if (selectedMetrics.includes(metric)) {
      onMetricsChange(selectedMetrics.filter(m => m !== metric));
    } else {
      onMetricsChange([...selectedMetrics, metric]);
    }
  };

  const toggleDimension = dimName => {
    if (selectedDimensions.includes(dimName)) {
      onDimensionsChange(selectedDimensions.filter(d => d !== dimName));
    } else {
      onDimensionsChange([...selectedDimensions, dimName]);
    }
  };

  const selectAllInNamespace = (namespace, items) => {
    const newSelection = [...new Set([...selectedMetrics, ...items])];
    onMetricsChange(newSelection);
  };

  const deselectAllInNamespace = (namespace, items) => {
    onMetricsChange(selectedMetrics.filter(m => !items.includes(m)));
  };

  return (
    <div className="selection-panel">
      {/* Metrics Section */}
      <div className="selection-section">
        <div className="section-header">
          <h3>Metrics</h3>
          <span className="selection-count">
            {selectedMetrics.length} selected
          </span>
        </div>

        <div className="search-box">
          <input
            type="text"
            placeholder="Search metrics..."
            value={metricsSearch}
            onChange={e => setMetricsSearch(e.target.value)}
          />
          {metricsSearch && (
            <button
              className="clear-search"
              onClick={() => setMetricsSearch('')}
            >
              ×
            </button>
          )}
        </div>

        <div className="selection-list">
          {sortedNamespaces.map(namespace => {
            const items = filteredGroups[namespace];
            const isExpanded = expandedNamespaces.has(namespace);
            const selectedInNamespace = items.filter(m =>
              selectedMetrics.includes(m),
            ).length;

            return (
              <div key={namespace} className="namespace-group">
                <div
                  className="namespace-header"
                  onClick={() => toggleNamespace(namespace)}
                >
                  <span className="expand-icon">{isExpanded ? '▼' : '▶'}</span>
                  <span className="namespace-name">{namespace}</span>
                  <span className="namespace-count">
                    {selectedInNamespace > 0 && (
                      <span className="selected-badge">
                        {selectedInNamespace}
                      </span>
                    )}
                    {items.length}
                  </span>
                </div>

                {isExpanded && (
                  <div className="namespace-items">
                    <div className="namespace-actions">
                      <button
                        type="button"
                        className="select-all-btn"
                        onClick={() => selectAllInNamespace(namespace, items)}
                      >
                        Select all
                      </button>
                      <button
                        type="button"
                        className="select-all-btn"
                        onClick={() => deselectAllInNamespace(namespace, items)}
                      >
                        Clear
                      </button>
                    </div>
                    {items.map(metric => (
                      <label key={metric} className="selection-item">
                        <input
                          type="checkbox"
                          checked={selectedMetrics.includes(metric)}
                          onChange={() => toggleMetric(metric)}
                        />
                        <span className="item-name">
                          {getShortName(metric)}
                        </span>
                      </label>
                    ))}
                  </div>
                )}
              </div>
            );
          })}

          {sortedNamespaces.length === 0 && (
            <div className="empty-list">
              {metricsSearch
                ? 'No metrics match your search'
                : 'No metrics available'}
            </div>
          )}
        </div>
      </div>

      {/* Divider */}
      <div className="section-divider" />

      {/* Dimensions Section */}
      <div className="selection-section">
        <div className="section-header">
          <h3>Dimensions</h3>
          <span className="selection-count">
            {selectedDimensions.length} selected
            {dimensions.length > 0 && ` / ${dimensions.length} available`}
          </span>
        </div>

        {selectedMetrics.length === 0 ? (
          <div className="empty-list hint">
            Select metrics to see available dimensions
          </div>
        ) : loading ? (
          <div className="empty-list">Loading dimensions...</div>
        ) : (
          <>
            <div className="search-box">
              <input
                type="text"
                placeholder="Search dimensions..."
                value={dimensionsSearch}
                onChange={e => setDimensionsSearch(e.target.value)}
              />
              {dimensionsSearch && (
                <button
                  className="clear-search"
                  onClick={() => setDimensionsSearch('')}
                >
                  ×
                </button>
              )}
            </div>

            <div className="selection-list dimensions-list">
              {filteredDimensions.map(dim => (
                <label key={dim.name} className="selection-item dimension-item">
                  <input
                    type="checkbox"
                    checked={selectedDimensions.includes(dim.name)}
                    onChange={() => toggleDimension(dim.name)}
                  />
                  <div className="dimension-info">
                    <span className="item-name">
                      {getDimDisplayName(dim.name)}
                    </span>
                    {dim.path && dim.path.length > 1 && (
                      <span className="dimension-path">
                        {dim.path.slice(1).join(' ▶ ')}
                      </span>
                    )}
                  </div>
                </label>
              ))}

              {filteredDimensions.length === 0 && (
                <div className="empty-list">
                  {dimensionsSearch
                    ? 'No dimensions match your search'
                    : 'No shared dimensions'}
                </div>
              )}
            </div>
          </>
        )}
      </div>
    </div>
  );
}

export default SelectionPanel;
