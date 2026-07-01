import { useParams, useSearchParams, useNavigate } from 'react-router-dom';
import { useContext, useEffect, useState, useCallback } from 'react';
import NodeStatus from '../NodePage/NodeStatus';
import DJClientContext from '../../providers/djclient';
import { useCurrentUser } from '../../providers/UserProvider';
import AddNodeDropdown from '../../components/AddNodeDropdown';
import NodeListActions from '../../components/NodeListActions';
import NamespaceHeader from '../../components/NamespaceHeader';
import Tooltip from '../../components/Tooltip';
import {
  secondaryButtonStyle,
  onSecondaryHover,
  onSecondaryOut,
} from '../../components/buttonStyles';
import LoadingIcon from '../../icons/LoadingIcon';
import CompactSelect from './CompactSelect';
import NamespaceNav from './NamespaceNav';
import { isHiddenNamespace } from './namespaceOptions';
import { NODE_TYPE_ORDER, NODE_TYPE_COLORS } from './nodeTypes';
import { getDJUrl } from '../../services/DJService';

import 'styles/node-list.css';
import 'styles/sorted-table.css';

const AVATAR_COLORS = [
  ['#dbeafe', '#1e40af'], // blue
  ['#dcfce7', '#166534'], // green
  ['#fef3c7', '#92400e'], // amber
  ['#fce7f3', '#9d174d'], // pink
  ['#ede9fe', '#5b21b6'], // purple
  ['#ffedd5', '#9a3412'], // orange
  ['#fee2e2', '#991b1b'], // red
  ['#d1fae5', '#065f46'], // teal
];

function avatarColorIndex(username) {
  let hash = 0;
  for (let i = 0; i < username.length; i++) {
    hash = (hash * 31 + username.charCodeAt(i)) >>> 0;
  }
  return hash % AVATAR_COLORS.length;
}

export function NamespacePage() {
  const ASC = 'ascending';
  const DESC = 'descending';

  const fields = [
    'name',
    'displayName',
    'type',
    'status',
    'mode',
    'owners',
    'updatedAt',
  ];

  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const { currentUser } = useCurrentUser();
  var { namespace } = useParams();
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();

  // Data for select options
  const [users, setUsers] = useState([]);
  const [tags, setTags] = useState([]);
  const [usersLoading, setUsersLoading] = useState(true);
  const [tagsLoading, setTagsLoading] = useState(true);

  // Load users and tags for dropdowns
  useEffect(() => {
    const fetchUsers = async () => {
      const data = await djClient.users();
      setUsers(data || []);
      setUsersLoading(false);
    };
    const fetchTags = async () => {
      const data = await djClient.listTags();
      setTags(data || []);
      setTagsLoading(false);
    };
    fetchUsers().catch(console.error);
    fetchTags().catch(console.error);
  }, [djClient]);

  // Parse all filters from URL
  const getFiltersFromUrl = useCallback(
    () => ({
      node_type: searchParams.get('type') || '',
      tags: searchParams.get('tags') ? searchParams.get('tags').split(',') : [],
      edited_by: searchParams.get('editedBy') || '',
      mode: searchParams.get('mode') || '',
      ownedBy: searchParams.get('ownedBy') || '',
      statuses: searchParams.get('statuses') || '',
      missingDescription: searchParams.get('missingDescription') === 'true',
      hasMaterialization: searchParams.get('hasMaterialization') === 'true',
      orphanedDimension: searchParams.get('orphanedDimension') === 'true',
    }),
    [searchParams],
  );

  const [filters, setFilters] = useState(getFiltersFromUrl);
  const [moreFiltersOpen, setMoreFiltersOpen] = useState(false);

  // Sync filters state when URL changes
  useEffect(() => {
    setFilters(getFiltersFromUrl());
  }, [searchParams, getFiltersFromUrl]);

  // Update URL when filters change
  const updateFilters = useCallback(
    newFilters => {
      const params = new URLSearchParams();

      if (newFilters.node_type) params.set('type', newFilters.node_type);
      if (newFilters.tags?.length)
        params.set('tags', newFilters.tags.join(','));
      if (newFilters.edited_by) params.set('editedBy', newFilters.edited_by);
      if (newFilters.mode) params.set('mode', newFilters.mode);
      if (newFilters.ownedBy) params.set('ownedBy', newFilters.ownedBy);
      if (newFilters.statuses) params.set('statuses', newFilters.statuses);
      if (newFilters.missingDescription)
        params.set('missingDescription', 'true');
      if (newFilters.hasMaterialization)
        params.set('hasMaterialization', 'true');
      if (newFilters.orphanedDimension) params.set('orphanedDimension', 'true');

      setSearchParams(params);
    },
    [setSearchParams],
  );

  const clearAllFilters = () => {
    setSearchParams(new URLSearchParams());
  };

  // Check if any filters are active
  const hasActiveFilters =
    filters.node_type ||
    filters.tags?.length ||
    filters.edited_by ||
    filters.mode ||
    filters.ownedBy ||
    filters.statuses ||
    filters.missingDescription ||
    filters.hasMaterialization ||
    filters.orphanedDimension;

  // Quick presets
  const presets = [
    {
      id: 'my-nodes',
      label: 'My Nodes',
      filters: { ownedBy: currentUser?.username },
    },
    {
      id: 'needs-attention',
      label: 'Needs Attention',
      filters: { ownedBy: currentUser?.username, statuses: 'INVALID' },
    },
    {
      id: 'drafts',
      label: 'Drafts',
      filters: { ownedBy: currentUser?.username, mode: 'draft' },
    },
  ];

  const applyPreset = preset => {
    const newFilters = {
      node_type: '',
      tags: [],
      edited_by: '',
      mode: preset.filters.mode || '',
      ownedBy: preset.filters.ownedBy || '',
      statuses: preset.filters.statuses || '',
      missingDescription: preset.filters.missingDescription || false,
      hasMaterialization: preset.filters.hasMaterialization || false,
      orphanedDimension: preset.filters.orphanedDimension || false,
    };
    updateFilters(newFilters);
  };

  // Check if a preset is active
  const isPresetActive = preset => {
    const pf = preset.filters;
    return (
      (pf.ownedBy || '') === (filters.ownedBy || '') &&
      (pf.statuses || '') === (filters.statuses || '') &&
      (pf.mode || '') === (filters.mode || '') &&
      !filters.node_type &&
      !filters.tags?.length &&
      !filters.edited_by &&
      !filters.missingDescription &&
      !filters.hasMaterialization &&
      !filters.orphanedDimension
    );
  };

  const [state, setState] = useState({
    namespace: namespace ? namespace : '',
    nodes: [],
  });
  const [retrieved, setRetrieved] = useState(false);

  const [namespaceHierarchy, setNamespaceHierarchy] = useState([]);
  const [rawNamespaces, setRawNamespaces] = useState([]);
  const [gitRoots, setGitRoots] = useState(new Set());
  // Use undefined to indicate "not yet loaded", null means "loaded but no config"
  const [gitConfig, setGitConfig] = useState(undefined);

  const [sortConfig, setSortConfig] = useState({
    key: 'updatedAt',
    direction: DESC,
  });

  const [before, setBefore] = useState(null);
  const [after, setAfter] = useState(null);
  const [prevCursor, setPrevCursor] = useState(true);
  const [nextCursor, setNextCursor] = useState(true);

  const [hasNextPage, setHasNextPage] = useState(true);
  const [hasPrevPage, setHasPrevPage] = useState(true);

  const [nodeSearch, setNodeSearch] = useState('');
  const [debouncedSearch, setDebouncedSearch] = useState('');
  useEffect(() => {
    const t = setTimeout(() => setDebouncedSearch(nodeSearch.trim()), 300);
    return () => clearTimeout(t);
  }, [nodeSearch]);
  // Changing the search term restarts paging from the first page; a cursor from
  // a previous page would otherwise be sent with the new query.
  useEffect(() => {
    setBefore(null);
    setAfter(null);
  }, [debouncedSearch]);

  const [typeCounts, setTypeCounts] = useState(null);

  // Only show edit/add controls once git config has loaded and namespace is not git-only
  const gitConfigLoaded = gitConfig !== undefined;
  // Descendants inherit github_repo_path via cascade, so compare against
  // git_root_namespace to know if this namespace IS the git root.
  const isGitRoot =
    gitConfigLoaded &&
    !!gitConfig?.github_repo_path &&
    gitConfig?.git_root_namespace === namespace;
  const showEditControls =
    gitConfigLoaded && !gitConfig?.git_only && !isGitRoot;
  // Sub-namespaces can be created from the rail only for plain (non-git-backed)
  // namespaces; git-backed ones are managed via git, not the UI. The git config
  // endpoint returns an object with null fields (not null) for non-git
  // namespaces, so check the actual git markers — a repo (root or cascaded
  // descendant) or a branch namespace.
  const isGitBacked = !!(
    gitConfig?.github_repo_path || gitConfig?.branch_namespace
  );
  const canCreateNamespace = gitConfigLoaded && !isGitBacked;
  const createSubNamespace = async fullNamespace => {
    const response = await djClient.addNamespace(fullNamespace);
    if (response.status === 200 || response.status === 201) {
      navigate(`/namespaces/${fullNamespace}`);
      return {};
    }
    return {
      _error: true,
      message: response.json?.message || 'Failed to create namespace',
    };
  };
  // A git root has no nodes of its own — they live on its default branch. The node
  // table (and its filters/keyword search) therefore browse `<root>.<default_branch>`,
  // so a git root shows the same browsable table as any other namespace.
  const tableNamespace =
    isGitRoot && gitConfig?.default_branch
      ? `${namespace}.${gitConfig.default_branch}`
      : namespace;

  // A git root has no nodes of its own and isn't a browsable branch — redirect to
  // its default branch so the URL is the branch (giving the breadcrumb its branch
  // switcher) and everything is scoped consistently.
  useEffect(() => {
    if (isGitRoot && gitConfig?.default_branch) {
      navigate(`/namespaces/${namespace}.${gitConfig.default_branch}`, {
        replace: true,
      });
    }
  }, [isGitRoot, gitConfig, namespace, navigate]);

  // Per-type node counts (recursive) for the current namespace, shown inline in
  // the TYPE filter options (e.g. "Metric (342)").
  useEffect(() => {
    if (isGitRoot && gitConfig?.default_branch) return;
    let cancelled = false;
    if (!tableNamespace) {
      setTypeCounts(null);
      return;
    }
    djClient
      .nodeTypeCounts(tableNamespace, NODE_TYPE_ORDER)
      .then(byType => {
        if (!cancelled) setTypeCounts(byType);
      })
      .catch(() => {
        if (!cancelled) setTypeCounts(null);
      });
    return () => {
      cancelled = true;
    };
  }, [djClient, tableNamespace]);

  const requestSort = key => {
    let direction = ASC;
    if (sortConfig.key === key && sortConfig.direction === ASC) {
      direction = DESC;
    }
    if (sortConfig.key !== key || sortConfig.direction !== direction) {
      setSortConfig({ key, direction });
    }
  };

  const getClassNamesFor = name => {
    if (sortConfig.key === name) {
      return sortConfig.direction;
    }
    return undefined;
  };

  const createNamespaceHierarchy = namespaceList => {
    const hierarchy = [];

    for (const item of namespaceList) {
      const namespaces = item.namespace.split('.');
      let currentLevel = hierarchy;

      let path = '';
      for (const ns of namespaces) {
        path += ns;

        let existingNamespace = currentLevel.find(el => el.namespace === ns);
        if (!existingNamespace) {
          existingNamespace = {
            namespace: ns,
            children: [],
            path: path,
          };
          currentLevel.push(existingNamespace);
          currentLevel.sort((a, b) => a.namespace.localeCompare(b.namespace));
        }

        currentLevel = existingNamespace.children;
        path += '.';
      }
    }
    return hierarchy;
  };

  useEffect(() => {
    const fetchData = async () => {
      const all = await djClient.listNamespacesWithGit();
      // Hide system/internal scratch namespaces from all browse navigation
      // (rail list, header switcher, folders).
      const namespaces = all.filter(ns => !isHiddenNamespace(ns.namespace));
      setRawNamespaces(namespaces);
      const hierarchy = createNamespaceHierarchy(namespaces);
      setNamespaceHierarchy(hierarchy);

      const roots = new Set(
        namespaces
          .filter(ns => ns.git?.__typename === 'GitRootConfig')
          .map(ns => ns.namespace),
      );
      setGitRoots(roots);
    };
    fetchData().catch(console.error);
  }, [djClient]);

  useEffect(() => {
    if (isGitRoot && gitConfig?.default_branch) return;
    const fetchData = async () => {
      setRetrieved(false);

      // Build extended filters for API
      const extendedFilters = {
        ownedBy: filters.ownedBy || null,
        statuses: filters.statuses ? [filters.statuses] : null,
        missingDescription: filters.missingDescription,
        hasMaterialization: filters.hasMaterialization,
        orphanedDimension: filters.orphanedDimension,
        // The table shows every node under this namespace (all descendants),
        // matching the rail's per-type counts. The rail's Folders drill DOWN to
        // re-scope; search narrows within the current scope.
        search: debouncedSearch || null,
      };

      const nodes = await djClient.listNodesForLanding(
        tableNamespace,
        filters.node_type ? [filters.node_type.toUpperCase()] : [],
        filters.tags,
        filters.edited_by,
        before,
        after,
        50,
        sortConfig,
        filters.mode ? filters.mode.toUpperCase() : null,
        extendedFilters,
      );

      setState({
        namespace: namespace,
        nodes: nodes.data
          ? nodes.data.findNodesPaginated.edges.map(n => n.node)
          : [],
      });
      if (nodes.data) {
        setPrevCursor(
          nodes.data ? nodes.data.findNodesPaginated.pageInfo.startCursor : '',
        );
        setNextCursor(
          nodes.data ? nodes.data.findNodesPaginated.pageInfo.endCursor : '',
        );
        setHasPrevPage(
          nodes.data
            ? nodes.data.findNodesPaginated.pageInfo.hasPrevPage
            : false,
        );
        setHasNextPage(
          nodes.data
            ? nodes.data.findNodesPaginated.pageInfo.hasNextPage
            : false,
        );
      }
      setRetrieved(true);
    };
    fetchData().catch(console.error);
  }, [
    djClient,
    filters,
    before,
    after,
    sortConfig.key,
    sortConfig.direction,
    tableNamespace,
    namespace,
    debouncedSearch,
  ]);

  const loadNext = () => {
    if (nextCursor) {
      setAfter(nextCursor);
      setBefore(null);
    }
  };
  const loadPrev = () => {
    if (prevCursor) {
      setAfter(null);
      setBefore(prevCursor);
    }
  };

  // Select options. TYPE options carry the recursive per-type node count for the
  // current namespace (e.g. "Metric (342)") — this replaces the old rail counts.
  const TYPE_LABELS = {
    metric: 'Metric',
    cube: 'Cube',
    dimension: 'Dimension',
    transform: 'Transform',
    source: 'Source',
  };
  const typeOptions = NODE_TYPE_ORDER.map(type => ({
    value: type,
    label: TYPE_LABELS[type],
    count: typeCounts?.[type] ?? null,
  }));

  // Renders a TYPE option as "<name> <colored count pill>" (pill colors match the
  // node-type badges). Right-aligns the pill in the menu; inline in the control.
  const formatTypeOption = (option, meta) => {
    const pill =
      option.count != null ? (
        <span
          style={{
            backgroundColor: NODE_TYPE_COLORS[option.value]?.bg ?? '#f1f5f9',
            color: NODE_TYPE_COLORS[option.value]?.color ?? '#475569',
            borderRadius: '8px',
            padding: '1px 8px',
            fontSize: '11px',
            fontWeight: 600,
            fontVariantNumeric: 'tabular-nums',
          }}
        >
          {option.count}
        </span>
      ) : null;
    return (
      <span
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: '8px',
          justifyContent:
            meta?.context === 'menu' ? 'space-between' : 'flex-start',
          width: meta?.context === 'menu' ? '100%' : 'auto',
        }}
      >
        <span>{option.label}</span>
        {pill}
      </span>
    );
  };

  const modeOptions = [
    { value: 'published', label: 'Published' },
    { value: 'draft', label: 'Draft' },
  ];

  const statusOptions = [
    { value: 'VALID', label: 'Valid' },
    { value: 'INVALID', label: 'Invalid' },
  ];

  const userOptions = users.map(u => ({
    value: u.username,
    label: u.username,
  }));
  const tagOptions = tags.map(t => ({ value: t.name, label: t.display_name }));

  const nodesList = retrieved ? (
    state.nodes.length > 0 ? (
      state.nodes.map(node => (
        <tr key={node.name}>
          <td
            style={{
              maxWidth: '300px',
              overflow: 'hidden',
              whiteSpace: 'nowrap',
              textOverflow: 'ellipsis',
            }}
          >
            <a
              href={'/nodes/' + node.name}
              className="link-table"
              title={node.name}
            >
              {/* Show names relative to the namespace in view — the table is already
                  scoped to tableNamespace, so the prefix is redundant on every row. */}
              {tableNamespace && node.name.startsWith(tableNamespace + '.')
                ? node.name.slice(tableNamespace.length + 1)
                : node.name}
            </a>
            <span
              className="rounded-pill badge bg-secondary-soft"
              style={{ marginLeft: '0.5rem' }}
            >
              {node.currentVersion}
            </span>
          </td>
          <td
            style={{
              maxWidth: '250px',
              overflow: 'hidden',
              whiteSpace: 'nowrap',
              textOverflow: 'ellipsis',
            }}
          >
            <a href={'/nodes/' + node.name} className="link-table">
              {node.type !== 'source' ? node.current.displayName : ''}
            </a>
          </td>
          <td>
            <span
              className={
                'node_type__' + node.type.toLowerCase() + ' badge node_type'
              }
            >
              {node.type}
            </span>
          </td>
          <td>
            <NodeStatus node={node} revalidate={false} />
          </td>
          <td>
            <span
              style={{
                display: 'inline-flex',
                alignItems: 'center',
                justifyContent: 'center',
                width: '24px',
                height: '24px',
                borderRadius: '50%',
                border: `2px solid ${
                  node.current.mode === 'PUBLISHED' ? '#28a745' : '#ffc107'
                }`,
                backgroundColor: 'transparent',
                color:
                  node.current.mode === 'PUBLISHED' ? '#28a745' : '#d39e00',
                fontWeight: '600',
                fontSize: '12px',
              }}
              title={node.current.mode === 'PUBLISHED' ? 'Published' : 'Draft'}
            >
              {node.current.mode === 'PUBLISHED' ? 'P' : 'D'}
            </span>
          </td>
          <td>
            {node.owners?.length > 0 && (
              <div style={{ display: 'flex', gap: '2px' }}>
                {node.owners.slice(0, 3).map(owner => {
                  const initials = owner.username
                    .split('@')[0]
                    .slice(0, 2)
                    .toUpperCase();
                  const [bg, fg] =
                    AVATAR_COLORS[avatarColorIndex(owner.username)];
                  return (
                    <span
                      key={owner.username}
                      title={owner.username}
                      style={{
                        display: 'inline-flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        width: '24px',
                        height: '24px',
                        borderRadius: '50%',
                        backgroundColor: bg,
                        color: fg,
                        fontSize: '9px',
                        fontWeight: '600',
                        flexShrink: 0,
                      }}
                    >
                      {initials}
                    </span>
                  );
                })}
              </div>
            )}
          </td>
          <td>
            <span className="status">
              {new Date(node.current.updatedAt).toLocaleDateString('en-us')}
            </span>
          </td>
          {showEditControls && (
            <td>
              <NodeListActions nodeName={node?.name} />
            </td>
          )}
        </tr>
      ))
    ) : (
      <tr>
        <td colSpan={8}>
          <span
            style={{
              display: 'block',
              marginTop: '2rem',
              marginLeft: '2rem',
              fontSize: '16px',
            }}
          >
            {hasActiveFilters ? (
              <>
                No nodes match the current filters.
                <a
                  href="#"
                  onClick={e => {
                    e.preventDefault();
                    clearAllFilters();
                  }}
                  style={{ marginLeft: '0.5rem' }}
                >
                  Clear filters
                </a>
              </>
            ) : (
              'No nodes in this namespace yet.'
            )}
          </span>
        </td>
      </tr>
    )
  ) : (
    <tr>
      <td>
        <span style={{ display: 'block', marginTop: '2rem' }}>
          <LoadingIcon />
        </span>
      </td>
    </tr>
  );

  // Count active quality filters (the ones in the "More" dropdown)
  const moreFiltersCount = [
    filters.missingDescription,
    filters.hasMaterialization,
    filters.orphanedDimension,
  ].filter(Boolean).length;

  return (
    <div className="mid">
      <div className="card">
        <div className="card-header">
          <div
            style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              marginBottom: '1rem',
            }}
          >
            <h2 style={{ margin: 0 }}>Browse</h2>
          </div>

          {/* Unified Filter Bar — always shown; a git root browses its default branch */}
          {gitConfigLoaded && (
            <div
              style={{
                marginBottom: '1rem',
                padding: '1rem',
                backgroundColor: '#f8fafc',
                borderRadius: '8px',
              }}
            >
              {/* Top row: Quick presets + Clear all */}
              <div
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: '12px',
                  marginBottom: '12px',
                }}
              >
                <div
                  style={{ display: 'flex', alignItems: 'center', gap: '6px' }}
                >
                  <span
                    style={{
                      fontSize: '11px',
                      fontWeight: '600',
                      textTransform: 'uppercase',
                      letterSpacing: '0.5px',
                      color: '#64748b',
                    }}
                  >
                    Quick
                  </span>
                  {presets.map(preset => (
                    <button
                      key={preset.id}
                      onClick={() => applyPreset(preset)}
                      style={{
                        padding: '4px 10px',
                        fontSize: '11px',
                        border: '1px solid',
                        borderColor: isPresetActive(preset)
                          ? '#1976d2'
                          : '#ddd',
                        borderRadius: '12px',
                        backgroundColor: isPresetActive(preset)
                          ? '#e3f2fd'
                          : 'white',
                        color: isPresetActive(preset) ? '#1976d2' : '#666',
                        cursor: 'pointer',
                        fontWeight: isPresetActive(preset) ? '600' : '400',
                      }}
                    >
                      {preset.label}
                    </button>
                  ))}
                  {hasActiveFilters && (
                    <button
                      onClick={clearAllFilters}
                      style={{
                        padding: '4px 10px',
                        fontSize: '11px',
                        border: 'none',
                        backgroundColor: 'transparent',
                        color: '#dc3545',
                        cursor: 'pointer',
                      }}
                    >
                      Clear all ×
                    </button>
                  )}
                </div>
              </div>

              {/* Bottom row: Dropdowns */}
              <div
                style={{
                  display: 'flex',
                  alignItems: 'flex-end',
                  gap: '12px',
                }}
              >
                <CompactSelect
                  label="Type"
                  name="type"
                  options={typeOptions}
                  value={filters.node_type}
                  onChange={e =>
                    updateFilters({ ...filters, node_type: e?.value || '' })
                  }
                  formatOptionLabel={formatTypeOption}
                  flex={1}
                  minWidth="80px"
                  testId="select-node-type"
                />
                <CompactSelect
                  label="Tags"
                  name="tags"
                  options={tagOptions}
                  value={filters.tags}
                  onChange={e =>
                    updateFilters({
                      ...filters,
                      tags: e ? e.map(t => t.value) : [],
                    })
                  }
                  isMulti
                  isLoading={tagsLoading}
                  flex={1.5}
                  minWidth="100px"
                  testId="select-tag"
                />
                <CompactSelect
                  label="Edited By"
                  name="editedBy"
                  options={userOptions}
                  value={filters.edited_by}
                  onChange={e =>
                    updateFilters({ ...filters, edited_by: e?.value || '' })
                  }
                  isLoading={usersLoading}
                  flex={1}
                  minWidth="80px"
                  testId="select-user"
                />
                <CompactSelect
                  label="Mode"
                  name="mode"
                  options={modeOptions}
                  value={filters.mode}
                  onChange={e =>
                    updateFilters({ ...filters, mode: e?.value || '' })
                  }
                  flex={1}
                  minWidth="80px"
                />
                <CompactSelect
                  label="Owner"
                  name="owner"
                  options={userOptions}
                  value={filters.ownedBy}
                  onChange={e =>
                    updateFilters({ ...filters, ownedBy: e?.value || '' })
                  }
                  isLoading={usersLoading}
                  flex={1}
                  minWidth="80px"
                />
                <CompactSelect
                  label="Status"
                  name="status"
                  options={statusOptions}
                  value={filters.statuses}
                  onChange={e =>
                    updateFilters({ ...filters, statuses: e?.value || '' })
                  }
                  flex={1}
                  minWidth="80px"
                />

                {/* More Filters (Quality) */}
                <div
                  style={{ position: 'relative', flex: 0, minWidth: 'auto' }}
                >
                  <div
                    style={{
                      display: 'flex',
                      flexDirection: 'column',
                      gap: '2px',
                    }}
                  >
                    <label
                      style={{
                        fontSize: '10px',
                        fontWeight: '600',
                        color: '#666',
                        textTransform: 'uppercase',
                        letterSpacing: '0.5px',
                      }}
                    >
                      Quality
                    </label>
                    <button
                      onClick={() => setMoreFiltersOpen(!moreFiltersOpen)}
                      style={{
                        height: '32px',
                        padding: '0 12px',
                        fontSize: '12px',
                        border: '1px solid #ccc',
                        borderRadius: '4px',
                        backgroundColor:
                          moreFiltersCount > 0 ? '#e3f2fd' : 'white',
                        color: '#666',
                        cursor: 'pointer',
                        display: 'flex',
                        alignItems: 'center',
                        gap: '4px',
                        whiteSpace: 'nowrap',
                      }}
                    >
                      {moreFiltersCount > 0
                        ? `${moreFiltersCount} active`
                        : 'Issues'}
                      <span style={{ fontSize: '8px' }}>
                        {moreFiltersOpen ? '▲' : '▼'}
                      </span>
                    </button>
                  </div>

                  {moreFiltersOpen && (
                    <div
                      style={{
                        position: 'absolute',
                        top: '100%',
                        right: 0,
                        marginTop: '4px',
                        padding: '12px',
                        backgroundColor: 'white',
                        border: '1px solid #ddd',
                        borderRadius: '8px',
                        boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
                        zIndex: 1000,
                        minWidth: '200px',
                      }}
                    >
                      <label
                        style={{
                          display: 'flex',
                          alignItems: 'center',
                          gap: '8px',
                          fontSize: '12px',
                          color: '#444',
                          marginBottom: '8px',
                          cursor: 'pointer',
                        }}
                      >
                        <input
                          type="checkbox"
                          checked={filters.missingDescription}
                          onChange={e =>
                            updateFilters({
                              ...filters,
                              missingDescription: e.target.checked,
                            })
                          }
                        />
                        Missing Description
                      </label>
                      <label
                        style={{
                          display: 'flex',
                          alignItems: 'center',
                          gap: '8px',
                          fontSize: '12px',
                          color: '#444',
                          marginBottom: '8px',
                          cursor: 'pointer',
                        }}
                      >
                        <input
                          type="checkbox"
                          checked={filters.orphanedDimension}
                          onChange={e =>
                            updateFilters({
                              ...filters,
                              orphanedDimension: e.target.checked,
                            })
                          }
                        />
                        Orphaned Dimensions
                      </label>
                      <label
                        style={{
                          display: 'flex',
                          alignItems: 'center',
                          gap: '8px',
                          fontSize: '12px',
                          color: '#444',
                          cursor: 'pointer',
                        }}
                      >
                        <input
                          type="checkbox"
                          checked={filters.hasMaterialization}
                          onChange={e =>
                            updateFilters({
                              ...filters,
                              hasMaterialization: e.target.checked,
                            })
                          }
                        />
                        Has Materialization
                      </label>
                    </div>
                  )}
                </div>
              </div>
            </div>
          )}

          <NamespaceHeader
            namespace={namespace}
            onGitConfigLoaded={setGitConfig}
            namespaceOptions={rawNamespaces}
            currentNamespace={namespace}
          >
            {namespace && (
              <Tooltip
                content={`Download every node in "${namespace}" as a YAML project (.zip) you can version in git and re-deploy with the DJ client.`}
              >
                <button
                  type="button"
                  onClick={async () => {
                    const response = await fetch(
                      `${getDJUrl()}/namespaces/${namespace}/export/yaml`,
                      { method: 'POST', credentials: 'include' },
                    );
                    if (!response.ok) {
                      return;
                    }
                    const blob = await response.blob();
                    const url = URL.createObjectURL(blob);
                    const link = document.createElement('a');
                    const safeName = namespace.replace(/\./g, '_');
                    link.href = url;
                    link.download = `${safeName}_export.zip`;
                    document.body.appendChild(link);
                    link.click();
                    document.body.removeChild(link);
                    URL.revokeObjectURL(url);
                  }}
                  style={secondaryButtonStyle}
                  onMouseOver={onSecondaryHover}
                  onMouseOut={onSecondaryOut}
                  aria-label="Export namespace to YAML"
                >
                  <svg
                    width="14"
                    height="14"
                    viewBox="0 0 24 24"
                    fill="none"
                    stroke="currentColor"
                    strokeWidth="2"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  >
                    <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path>
                    <polyline points="7 10 12 15 17 10"></polyline>
                    <line x1="12" y1="15" x2="12" y2="3"></line>
                  </svg>
                  Export YAML
                </button>
              </Tooltip>
            )}
            {showEditControls && <AddNodeDropdown namespace={namespace} />}
          </NamespaceHeader>

          <div className="table-responsive">
            {/* The rail is always present for layout consistency, even on a leaf
                namespace with no sub-namespaces (where its Folders list is empty). */}
            <div
              className="sidebar"
              style={{
                borderRight: '1px solid #e2e8f0',
                paddingRight: '1rem',
              }}
            >
              <NamespaceNav
                namespaces={rawNamespaces}
                hierarchy={namespaceHierarchy}
                currentNamespace={namespace}
                gitRoots={gitRoots}
                onSelect={value =>
                  navigate(value ? `/namespaces/${value}` : '/')
                }
                canCreateNamespace={canCreateNamespace}
                onCreateNamespace={createSubNamespace}
              />
            </div>
            <div
              style={{
                flex: 1,
                minWidth: 0,
                marginLeft: '1.5rem',
              }}
            >
              {gitConfigLoaded && (
                <input
                  type="text"
                  className="dj-node-search"
                  value={nodeSearch}
                  onChange={e => setNodeSearch(e.target.value)}
                  placeholder="Search nodes in this namespace…"
                  aria-label="Search nodes in this namespace"
                />
              )}

              {/* NODES: nodes that live directly in this namespace */}
              {!gitConfigLoaded ? null : (
                <table className="card-table table" style={{ marginBottom: 0 }}>
                  <thead>
                    <tr>
                      {fields.map(field => {
                        const thStyle = {
                          fontFamily:
                            "'Inter', -apple-system, BlinkMacSystemFont, sans-serif",
                          fontSize: '11px',
                          fontWeight: '600',
                          textTransform: 'uppercase',
                          letterSpacing: '0.5px',
                          color: '#64748b',
                          padding: '12px 16px',
                          borderBottom: '1px solid #e2e8f0',
                          backgroundColor: 'transparent',
                        };
                        return (
                          <th key={field} style={thStyle}>
                            <button
                              type="button"
                              onClick={() => requestSort(field)}
                              className={'sortable ' + getClassNamesFor(field)}
                              style={{
                                fontSize: 'inherit',
                                fontWeight: 'inherit',
                                letterSpacing: 'inherit',
                                textTransform: 'inherit',
                                fontFamily: 'inherit',
                              }}
                            >
                              {field.replace(/([a-z](?=[A-Z]))/g, '$1 ')}
                            </button>
                          </th>
                        );
                      })}
                      {showEditControls && (
                        <th
                          style={{
                            fontFamily:
                              "'Inter', -apple-system, BlinkMacSystemFont, sans-serif",
                            fontSize: '11px',
                            fontWeight: '600',
                            textTransform: 'uppercase',
                            letterSpacing: '0.5px',
                            color: '#64748b',
                            padding: '12px 16px',
                            borderBottom: '1px solid #e2e8f0',
                            backgroundColor: 'transparent',
                          }}
                        >
                          Actions
                        </th>
                      )}
                    </tr>
                  </thead>
                  <tbody className="nodes-table-body">{nodesList}</tbody>
                  <tfoot>
                    <tr>
                      <td>
                        {retrieved && hasPrevPage ? (
                          <a
                            onClick={loadPrev}
                            className="previous round pagination"
                          >
                            ← Previous
                          </a>
                        ) : (
                          ''
                        )}
                        {retrieved && hasNextPage ? (
                          <a
                            onClick={loadNext}
                            className="next round pagination"
                          >
                            Next →
                          </a>
                        ) : (
                          ''
                        )}
                      </td>
                    </tr>
                  </tfoot>
                </table>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
