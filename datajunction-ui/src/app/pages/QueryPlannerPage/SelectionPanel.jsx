import { useState, useMemo, useEffect, useRef, useCallback } from 'react';

const ENGINE_OPTIONS = [
  { value: null, label: 'Auto' },
  { value: 'druid', label: 'Druid' },
  { value: 'trino', label: 'Trino' },
];

/**
 * SelectionPanel - Browse and select metrics and dimensions
 * Features selected items as chips at the top for visibility
 * Includes cube preset loading for quick configuration
 */
export function SelectionPanel({
  metrics,
  selectedMetrics,
  onMetricsChange,
  dimensions,
  selectedDimensions,
  onDimensionsChange,
  loading,
  cubes = [],
  onLoadCubePreset,
  loadedCubeName = null, // Managed by parent for URL persistence
  onClearSelection,
  filters = [],
  onFiltersChange,
  selectedEngine = null,
  onEngineChange,
  onRunQuery,
  canRunQuery = false,
  queryLoading = false,
}) {
  const [metricsSearch, setMetricsSearch] = useState('');
  const [dimensionsSearch, setDimensionsSearch] = useState('');
  const [expandedNamespaces, setExpandedNamespaces] = useState(new Set());
  const [expandedDimGroups, setExpandedDimGroups] = useState(new Set());
  const [expandedRolePaths, setExpandedRolePaths] = useState(new Set());
  const [showCubeDropdown, setShowCubeDropdown] = useState(false);
  const [cubeSearch, setCubeSearch] = useState('');
  const [metricsChipsExpanded, setMetricsChipsExpanded] = useState(false);
  const [dimensionsChipsExpanded, setDimensionsChipsExpanded] = useState(false);
  const [filterInput, setFilterInput] = useState('');
  const [split1, setSplit1] = useState(35); // metrics / dims boundary (%)
  const [split2, setSplit2] = useState(70); // dims / filters boundary (%)
  const prevSearchRef = useRef('');
  const cubeDropdownRef = useRef(null);
  const metricsSearchRef = useRef(null);
  const dimensionsSearchRef = useRef(null);
  const filterInputRef = useRef(null);
  const sectionsRef = useRef(null);
  const dragRef = useRef(null);
  const splitRef = useRef({ split1: 35, split2: 70 });
  useEffect(() => {
    splitRef.current = { split1, split2 };
  }, [split1, split2]);

  // Threshold for showing expand/collapse button
  const CHIPS_COLLAPSE_THRESHOLD = 8;

  // Find the loaded cube object from the name
  const loadedCube = useMemo(() => {
    if (!loadedCubeName) return null;
    return (
      cubes.find(c => c.name === loadedCubeName) || { name: loadedCubeName }
    );
  }, [loadedCubeName, cubes]);

  // Close cube dropdown when clicking outside
  useEffect(() => {
    const handleClickOutside = event => {
      if (
        cubeDropdownRef.current &&
        !cubeDropdownRef.current.contains(event.target)
      ) {
        setShowCubeDropdown(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  // Filter cubes by search (GraphQL returns display_name)
  const filteredCubes = useMemo(() => {
    const search = cubeSearch.toLowerCase().trim();
    if (!search) return cubes;
    return cubes.filter(cube => {
      const name = cube.name || '';
      const displayName = cube.display_name || '';
      return (
        name.toLowerCase().includes(search) ||
        displayName.toLowerCase().includes(search)
      );
    });
  }, [cubes, cubeSearch]);

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
  const { filteredGroups, sortedNamespaces } = useMemo(() => {
    const search = metricsSearch.trim().toLowerCase();

    if (!search) {
      const namespaces = Object.keys(groupedMetrics).sort();
      return { filteredGroups: groupedMetrics, sortedNamespaces: namespaces };
    }

    const filtered = {};
    Object.entries(groupedMetrics).forEach(([namespace, items]) => {
      const matchingItems = items.filter(m => m.toLowerCase().includes(search));
      if (matchingItems.length > 0) {
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

    const namespaces = Object.keys(filtered).sort((a, b) => {
      const aLower = a.toLowerCase();
      const bLower = b.toLowerCase();
      const aPrefix = aLower.startsWith(search);
      const bPrefix = bLower.startsWith(search);
      if (aPrefix && !bPrefix) return -1;
      if (!aPrefix && bPrefix) return 1;
      const aContains = aLower.includes(search);
      const bContains = bLower.includes(search);
      if (aContains && !bContains) return -1;
      if (!aContains && bContains) return 1;
      const aCount = filtered[a].length;
      const bCount = filtered[b].length;
      if (aCount !== bCount) return bCount - aCount;
      return aLower.localeCompare(bLower);
    });

    return { filteredGroups: filtered, sortedNamespaces: namespaces };
  }, [groupedMetrics, metricsSearch]);

  // Auto-expand all matching namespaces when search changes
  useEffect(() => {
    const currentSearch = metricsSearch.trim();
    const prevSearch = prevSearchRef.current;
    if (currentSearch && currentSearch !== prevSearch) {
      setExpandedNamespaces(new Set(sortedNamespaces));
    }
    prevSearchRef.current = currentSearch;
  }, [metricsSearch, sortedNamespaces]);

  // Dedupe dimensions by name, keeping shortest path per name
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

  // Extract role path from a dimension name, e.g. "foo.bar[a->b->c]" → "a->b->c" (or null)
  const getRolePath = name => {
    const match = name.match(/\[([^\]]+)\]$/);
    return match ? match[1] : null;
  };

  // Format a role path for display: "a->b->c" → "via a → b → c"
  const formatRolePath = roleKey =>
    roleKey ? 'via ' + roleKey.replace(/->/g, ' → ') : 'direct';

  // Count hops in a role path string
  const rolePathHops = roleKey => (roleKey ? roleKey.split('->').length : 0);

  // Group dimensions by node, then by role path within each node
  const groupedDimensions = useMemo(() => {
    const search = dimensionsSearch.trim().toLowerCase();
    const nodeMap = new Map();

    dedupedDimensions.forEach(d => {
      if (!d.name) return;
      const nodeKey =
        d.path?.length > 0
          ? d.path[d.path.length - 1]
          : d.name.split('.').slice(0, -1).join('.');
      const distance = Math.max(0, d.path ? d.path.length - 1 : 0);
      const roleKey = getRolePath(d.name); // e.g. "title_deal_window->title" or null

      if (!nodeMap.has(nodeKey)) {
        nodeMap.set(nodeKey, {
          nodeKey,
          minDistance: distance,
          rolePathMap: new Map(),
        });
      }
      const node = nodeMap.get(nodeKey);
      node.minDistance = Math.min(node.minDistance, distance);

      if (!node.rolePathMap.has(roleKey)) {
        node.rolePathMap.set(roleKey, { roleKey, dimensions: [] });
      }
      node.rolePathMap.get(roleKey).dimensions.push(d);
    });

    let groupsArray = Array.from(nodeMap.values()).map(node => {
      // Sort role paths: direct (null) first, then by hop count, then alphabetically
      const rolePaths = Array.from(node.rolePathMap.values()).sort(
        (a, b) =>
          rolePathHops(a.roleKey) - rolePathHops(b.roleKey) ||
          (a.roleKey || '').localeCompare(b.roleKey || ''),
      );
      // Sort dims within each role path alphabetically
      rolePaths.forEach(rp => {
        rp.dimensions.sort((a, b) => a.name.localeCompare(b.name));
      });
      return {
        nodeKey: node.nodeKey,
        minDistance: node.minDistance,
        rolePaths,
        totalCount: rolePaths.reduce((n, rp) => n + rp.dimensions.length, 0),
      };
    });

    // Apply search: filter within role paths, prune empty role paths and nodes
    if (search) {
      groupsArray = groupsArray
        .map(group => ({
          ...group,
          rolePaths: group.rolePaths
            .map(rp => ({
              ...rp,
              dimensions: rp.dimensions.filter(d => {
                const lower = d.name.toLowerCase();
                return (
                  lower.includes(search) ||
                  d.name.split('.').pop().toLowerCase().includes(search)
                );
              }),
            }))
            .filter(
              rp =>
                rp.dimensions.length > 0 ||
                (rp.roleKey || '').toLowerCase().includes(search),
            ),
        }))
        .filter(
          group =>
            group.rolePaths.length > 0 ||
            group.nodeKey.toLowerCase().includes(search),
        )
        .map(group => ({
          ...group,
          totalCount: group.rolePaths.reduce(
            (n, rp) => n + rp.dimensions.length,
            0,
          ),
        }));
    }

    // Sort node groups by distance, then alphabetically
    groupsArray.sort(
      (a, b) =>
        a.minDistance - b.minDistance || a.nodeKey.localeCompare(b.nodeKey),
    );

    return groupsArray;
  }, [dedupedDimensions, dimensionsSearch]);

  // Auto-expand on first load and when searching
  useEffect(() => {
    if (groupedDimensions.length === 0) return;
    if (dimensionsSearch.trim()) {
      // Expand everything with matches
      setExpandedDimGroups(new Set(groupedDimensions.map(g => g.nodeKey)));
      setExpandedRolePaths(
        new Set(
          groupedDimensions.flatMap(g =>
            g.rolePaths.map(rp => `${g.nodeKey}::${rp.roleKey}`),
          ),
        ),
      );
    } else {
      // On first load: expand distance-0 node groups and their direct (null) role paths
      setExpandedDimGroups(prev => {
        if (prev.size > 0) return prev;
        return new Set(
          groupedDimensions
            .filter(g => g.minDistance === 0)
            .map(g => g.nodeKey),
        );
      });
      setExpandedRolePaths(prev => {
        if (prev.size > 0) return prev;
        return new Set(
          groupedDimensions
            .filter(g => g.minDistance === 0)
            .map(g => `${g.nodeKey}::null`),
        );
      });
    }
  }, [groupedDimensions, dimensionsSearch]);

  // Get display name for dimension (last 2 segments)
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

  const toggleDimGroup = nodeKey => {
    setExpandedDimGroups(prev => {
      const next = new Set(prev);
      if (next.has(nodeKey)) {
        next.delete(nodeKey);
      } else {
        next.add(nodeKey);
      }
      return next;
    });
  };

  const toggleRolePath = (nodeKey, roleKey) => {
    const key = `${nodeKey}::${roleKey}`;
    setExpandedRolePaths(prev => {
      const next = new Set(prev);
      if (next.has(key)) {
        next.delete(key);
      } else {
        next.add(key);
      }
      return next;
    });
  };

  const getDimGroupShortName = nodeKey => nodeKey.split('.').pop();

  const handleDividerMouseDown = useCallback((e, divider) => {
    e.preventDefault();
    const startSplit =
      divider === 1 ? splitRef.current.split1 : splitRef.current.split2;
    dragRef.current = { divider, startY: e.clientY, startSplit };
  }, []);

  useEffect(() => {
    const onMouseMove = e => {
      if (!dragRef.current || !sectionsRef.current) return;
      const height = sectionsRef.current.getBoundingClientRect().height;
      const deltaPct = ((e.clientY - dragRef.current.startY) / height) * 100;
      const { divider, startSplit } = dragRef.current;
      const { split1, split2 } = splitRef.current;
      if (divider === 1) {
        setSplit1(Math.max(10, Math.min(split2 - 15, startSplit + deltaPct)));
      } else {
        setSplit2(Math.max(split1 + 15, Math.min(90, startSplit + deltaPct)));
      }
    };
    const onMouseUp = () => {
      dragRef.current = null;
    };
    window.addEventListener('mousemove', onMouseMove);
    window.addEventListener('mouseup', onMouseUp);
    return () => {
      window.removeEventListener('mousemove', onMouseMove);
      window.removeEventListener('mouseup', onMouseUp);
    };
  }, []);

  const toggleMetric = metric => {
    if (selectedMetrics.includes(metric)) {
      onMetricsChange(selectedMetrics.filter(m => m !== metric));
    } else {
      onMetricsChange([...selectedMetrics, metric]);
    }
  };

  const removeMetric = metric => {
    onMetricsChange(selectedMetrics.filter(m => m !== metric));
  };

  const toggleDimension = dimName => {
    if (selectedDimensions.includes(dimName)) {
      onDimensionsChange(selectedDimensions.filter(d => d !== dimName));
    } else {
      onDimensionsChange([...selectedDimensions, dimName]);
    }
  };

  const removeDimension = dimName => {
    onDimensionsChange(selectedDimensions.filter(d => d !== dimName));
  };

  const selectAllInNamespace = (namespace, items) => {
    const newSelection = [...new Set([...selectedMetrics, ...items])];
    onMetricsChange(newSelection);
  };

  const deselectAllInNamespace = (namespace, items) => {
    onMetricsChange(selectedMetrics.filter(m => !items.includes(m)));
  };

  const handleCubeSelect = cube => {
    if (onLoadCubePreset) {
      onLoadCubePreset(cube.name);
    }
    // loadedCubeName is now managed by parent via onLoadCubePreset
    setShowCubeDropdown(false);
    setCubeSearch('');
  };

  const clearSelection = () => {
    if (onClearSelection) {
      onClearSelection(); // Parent handles clearing metrics, dimensions, and cube
    } else {
      onMetricsChange([]);
      onDimensionsChange([]);
    }
  };

  const handleAddFilter = () => {
    const trimmed = filterInput.trim();
    if (trimmed && !filters.includes(trimmed) && onFiltersChange) {
      onFiltersChange([...filters, trimmed]);
      setFilterInput('');
    }
  };

  const handleFilterKeyDown = e => {
    if (e.key === 'Enter') {
      e.preventDefault();
      handleAddFilter();
    }
  };

  const handleRemoveFilter = filterToRemove => {
    if (onFiltersChange) {
      onFiltersChange(filters.filter(f => f !== filterToRemove));
    }
  };

  const addDimAsFilter = (e, dimName) => {
    e.preventDefault();
    e.stopPropagation();
    const prefix = filterInput.trim() ? filterInput.trimEnd() + ' AND ' : '';
    setFilterInput(prefix + dimName + ' ');
    filterInputRef.current?.focus();
  };

  return (
    <div className="selection-panel">
      {/* Cube Preset Dropdown */}
      {cubes.length > 0 && (
        <div className="cube-preset-section" ref={cubeDropdownRef}>
          <div className="preset-row">
            <button
              className={`preset-button ${loadedCube ? 'has-preset' : ''}`}
              onClick={() => setShowCubeDropdown(!showCubeDropdown)}
            >
              <span className="preset-icon">{loadedCube ? '📦' : '📂'}</span>
              <span className="preset-label">
                {loadedCube
                  ? loadedCube.display_name ||
                    (loadedCube.name
                      ? loadedCube.name.split('.').pop()
                      : 'Cube')
                  : 'Load from Cube'}
              </span>
              <span className="dropdown-arrow">
                {showCubeDropdown ? '▲' : '▼'}
              </span>
            </button>
            {(selectedMetrics.length > 0 || selectedDimensions.length > 0) && (
              <button className="clear-all-btn" onClick={clearSelection}>
                Clear
              </button>
            )}
          </div>

          {showCubeDropdown && (
            <div className="cube-dropdown">
              <input
                type="text"
                className="cube-search"
                placeholder="Search cubes..."
                value={cubeSearch}
                onChange={e => setCubeSearch(e.target.value)}
                autoFocus
              />
              <div className="cube-list">
                {filteredCubes.length === 0 ? (
                  <div className="cube-empty">
                    {cubeSearch
                      ? 'No cubes match your search'
                      : 'No cubes available'}
                  </div>
                ) : (
                  filteredCubes.map(cube => (
                    <button
                      key={cube.name}
                      className={`cube-option ${
                        loadedCubeName === cube.name ? 'selected' : ''
                      }`}
                      onClick={() => handleCubeSelect(cube)}
                    >
                      <span className="cube-name">
                        {cube.display_name ||
                          (cube.name ? cube.name.split('.').pop() : 'Unknown')}
                      </span>
                      <span className="cube-info">{cube.name}</span>
                      {loadedCubeName === cube.name && (
                        <span className="cube-selected-icon">✓</span>
                      )}
                    </button>
                  ))
                )}
              </div>
            </div>
          )}
        </div>
      )}

      {/* Resizable sections */}
      <div className="resizable-sections" ref={sectionsRef}>
        {/* Metrics Section */}
        <div className="selection-section" style={{ flex: split1 }}>
          <div className="section-header">
            <h3>Metrics</h3>
            <span className="selection-count">
              {selectedMetrics.length} selected
            </span>
          </div>

          {/* Combined Chips + Search Input */}
          <div
            className="combobox-input"
            onClick={() => metricsSearchRef.current?.focus()}
          >
            {selectedMetrics.length > 0 && (
              <div
                className={`combobox-chips ${
                  selectedMetrics.length > CHIPS_COLLAPSE_THRESHOLD
                    ? metricsChipsExpanded
                      ? 'expanded'
                      : 'collapsed'
                    : ''
                }`}
              >
                {selectedMetrics.map(metric => (
                  <span key={metric} className="selected-chip metric-chip">
                    {getShortName(metric)}
                    <button
                      className="chip-remove"
                      onClick={e => {
                        e.stopPropagation();
                        removeMetric(metric);
                      }}
                      title={`Remove ${getShortName(metric)}`}
                    >
                      ×
                    </button>
                  </span>
                ))}
              </div>
            )}
            <div className="combobox-input-row">
              <input
                ref={metricsSearchRef}
                type="text"
                className="combobox-search"
                placeholder="Search metrics..."
                value={metricsSearch}
                onChange={e => setMetricsSearch(e.target.value)}
                onClick={e => e.stopPropagation()}
              />
              {selectedMetrics.length > CHIPS_COLLAPSE_THRESHOLD && (
                <button
                  className="combobox-action"
                  onClick={e => {
                    e.stopPropagation();
                    setMetricsChipsExpanded(!metricsChipsExpanded);
                  }}
                >
                  {metricsChipsExpanded ? 'Show less' : 'Show all'}
                </button>
              )}
              {selectedMetrics.length > 0 && (
                <button
                  className="combobox-action"
                  onClick={e => {
                    e.stopPropagation();
                    onMetricsChange([]);
                  }}
                >
                  Clear
                </button>
              )}
            </div>
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
                    <span className="expand-icon">
                      {isExpanded ? '▼' : '▶'}
                    </span>
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
                          onClick={() =>
                            deselectAllInNamespace(namespace, items)
                          }
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

        {/* Draggable Divider 1: metrics / dims */}
        <div
          className="section-divider draggable-divider"
          onMouseDown={e => handleDividerMouseDown(e, 1)}
        />

        {/* Dimensions Section */}
        <div className="selection-section" style={{ flex: split2 - split1 }}>
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
              {/* Combined Chips + Search Input */}
              <div
                className="combobox-input"
                onClick={() => dimensionsSearchRef.current?.focus()}
              >
                {selectedDimensions.length > 0 && (
                  <div
                    className={`combobox-chips ${
                      selectedDimensions.length > CHIPS_COLLAPSE_THRESHOLD
                        ? dimensionsChipsExpanded
                          ? 'expanded'
                          : 'collapsed'
                        : ''
                    }`}
                  >
                    {selectedDimensions.map(dimName => (
                      <span
                        key={dimName}
                        className="selected-chip dimension-chip"
                        title={dimName}
                      >
                        <span className="chip-label">
                          {getDimDisplayName(dimName)}
                        </span>
                        <button
                          className="chip-remove"
                          onClick={e => {
                            e.stopPropagation();
                            removeDimension(dimName);
                          }}
                          title={`Remove ${getDimDisplayName(dimName)}`}
                        >
                          ×
                        </button>
                      </span>
                    ))}
                  </div>
                )}
                <div className="combobox-input-row">
                  <input
                    ref={dimensionsSearchRef}
                    type="text"
                    className="combobox-search"
                    placeholder="Search dimensions..."
                    value={dimensionsSearch}
                    onChange={e => setDimensionsSearch(e.target.value)}
                    onClick={e => e.stopPropagation()}
                  />
                  {selectedDimensions.length > CHIPS_COLLAPSE_THRESHOLD && (
                    <button
                      className="combobox-action"
                      onClick={e => {
                        e.stopPropagation();
                        setDimensionsChipsExpanded(!dimensionsChipsExpanded);
                      }}
                    >
                      {dimensionsChipsExpanded ? 'Show less' : 'Show all'}
                    </button>
                  )}
                  {selectedDimensions.length > 0 && (
                    <button
                      className="combobox-action"
                      onClick={e => {
                        e.stopPropagation();
                        onDimensionsChange([]);
                      }}
                    >
                      Clear
                    </button>
                  )}
                </div>
              </div>

              <div className="selection-list dimensions-list">
                {groupedDimensions.map(group => {
                  const nodeExpanded = expandedDimGroups.has(group.nodeKey);
                  return (
                    <div key={group.nodeKey} className="dim-group">
                      <div
                        className="dim-group-header"
                        onClick={() => toggleDimGroup(group.nodeKey)}
                        title={group.nodeKey}
                      >
                        <span className="expand-icon">
                          {nodeExpanded ? '▼' : '▶'}
                        </span>
                        <span className="dim-group-name">
                          {getDimGroupShortName(group.nodeKey)}
                        </span>
                        <span className="dim-group-count">
                          {group.totalCount}
                        </span>
                      </div>
                      {nodeExpanded &&
                        group.rolePaths.map(rp => {
                          const rpKey = `${group.nodeKey}::${rp.roleKey}`;
                          const rpExpanded = expandedRolePaths.has(rpKey);
                          const hops = rolePathHops(rp.roleKey);
                          const hopsLabel =
                            hops === 0
                              ? 'direct'
                              : `${hops} hop${hops > 1 ? 's' : ''}`;
                          return (
                            <div key={rpKey} className="dim-role-group">
                              <div
                                className="dim-role-header"
                                onClick={() =>
                                  toggleRolePath(group.nodeKey, rp.roleKey)
                                }
                              >
                                <span className="expand-icon">
                                  {rpExpanded ? '▼' : '▶'}
                                </span>
                                <span className="dim-role-label">
                                  {formatRolePath(rp.roleKey)}
                                </span>
                                <span className="dim-group-meta">
                                  {hopsLabel}
                                </span>
                                <span className="dim-group-count">
                                  {rp.dimensions.length}
                                </span>
                              </div>
                              {rpExpanded &&
                                rp.dimensions.map(dim => (
                                  <label
                                    key={dim.name}
                                    className="selection-item dimension-item dim-role-item"
                                    title={dim.name}
                                  >
                                    <input
                                      type="checkbox"
                                      checked={selectedDimensions.includes(
                                        dim.name,
                                      )}
                                      onChange={() => toggleDimension(dim.name)}
                                    />
                                    <div className="dimension-info">
                                      <span className="item-name">
                                        {dim.name
                                          .replace(/\[[^\]]*\]$/, '')
                                          .split('.')
                                          .pop()}
                                      </span>
                                    </div>
                                    <button
                                      className="dim-filter-btn"
                                      title={`Add "${dim.name}" to filters`}
                                      onClick={e => addDimAsFilter(e, dim.name)}
                                    >
                                      + filter
                                    </button>
                                  </label>
                                ))}
                            </div>
                          );
                        })}
                    </div>
                  );
                })}

                {groupedDimensions.length === 0 && (
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

        {/* Draggable Divider 2: dims / filters */}
        <div
          className="section-divider draggable-divider"
          onMouseDown={e => handleDividerMouseDown(e, 2)}
        />

        {/* Filters Section */}
        <div
          className="selection-section filters-section"
          style={{ flex: 100 - split2 }}
        >
          <div className="section-header">
            <h3>Filters</h3>
            <span className="selection-count">{filters.length} applied</span>
          </div>

          {/* Filter chips */}
          {filters.length > 0 && (
            <div className="filter-chips-container">
              {filters.map((filter, idx) => (
                <span key={idx} className="filter-chip">
                  <span className="filter-chip-text">{filter}</span>
                  <button
                    className="filter-chip-remove"
                    onClick={() => handleRemoveFilter(filter)}
                    title="Remove filter"
                  >
                    ×
                  </button>
                </span>
              ))}
            </div>
          )}

          {/* Filter input */}
          <div className="filter-input-container">
            <input
              ref={filterInputRef}
              type="text"
              className="filter-input"
              placeholder="e.g. v3.date.date_id >= '2024-01-01'"
              value={filterInput}
              onChange={e => setFilterInput(e.target.value)}
              onKeyDown={handleFilterKeyDown}
            />
            <button
              className="filter-add-btn"
              onClick={handleAddFilter}
              disabled={!filterInput.trim()}
            >
              Add
            </button>
          </div>
        </div>
      </div>
      {/* end resizable-sections */}

      {/* Engine Selection */}
      <div className="engine-section">
        <span className="engine-label">Engine</span>
        <div className="engine-pills">
          {ENGINE_OPTIONS.map(({ value, label }) => (
            <button
              key={label}
              className={`engine-pill${
                selectedEngine === value ? ' active' : ''
              }`}
              onClick={() => onEngineChange && onEngineChange(value)}
            >
              {label}
            </button>
          ))}
        </div>
      </div>

      {/* Run Query Section */}
      <div className="run-query-section">
        <button
          className="run-query-btn"
          onClick={onRunQuery}
          disabled={!canRunQuery || queryLoading}
        >
          {queryLoading ? (
            <>
              <span className="spinner small" />
              Running...
            </>
          ) : (
            <>
              <span className="run-icon">▶</span>
              Run Query
            </>
          )}
        </button>
        {!canRunQuery && selectedMetrics.length > 0 && (
          <span className="run-hint">Select at least one dimension</span>
        )}
        {!canRunQuery && selectedMetrics.length === 0 && (
          <span className="run-hint">
            Select metrics and dimensions to run a query
          </span>
        )}
      </div>
    </div>
  );
}

export default SelectionPanel;
