import * as React from 'react';
import { useParams, useSearchParams } from 'react-router-dom';
import { useContext, useEffect, useState, useCallback } from 'react';
import NodeStatus from '../NodePage/NodeStatus';
import DJClientContext from '../../providers/djclient';
import { useCurrentUser } from '../../providers/UserProvider';
import Explorer from '../NamespacePage/Explorer';
import AddNodeDropdown from '../../components/AddNodeDropdown';
import NodeListActions from '../../components/NodeListActions';
import NamespaceHeader from '../../components/NamespaceHeader';
import LoadingIcon from '../../icons/LoadingIcon';
import CompactSelect from './CompactSelect';
import { NodeBadge, NodeLink } from '../../components/NodeComponents';
import { getDJUrl } from '../../services/DJService';

const NODE_TYPE_ORDER = ['metric', 'cube', 'dimension', 'transform', 'source'];

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
const MAX_PER_TYPE = 8;

const NODE_TYPE_COLORS = {
  metric: { bg: '#fad7dd', color: '#a2283e' },
  cube: { bg: '#dbafff', color: '#580076' },
  dimension: { bg: '#ffefd0', color: '#a96621' },
  transform: { bg: '#ccefff', color: '#0063b4' },
  source: { bg: '#ccf7e5', color: '#00b368' },
};

function DefaultBranchPreview({ nodes, defaultBranchNs }) {
  const groups = {};
  nodes.forEach(node => {
    const type = (node.type || 'unknown').toLowerCase();
    if (!groups[type]) groups[type] = [];
    groups[type].push(node);
  });
  const grouped = NODE_TYPE_ORDER.filter(t => groups[t]?.length > 0).map(t => ({
    type: t,
    nodes: groups[t],
  }));

  if (grouped.length === 0) return null;

  return (
    <div
      style={{
        display: 'grid',
        gridTemplateColumns: '1fr 1fr',
        gap: '0 0',
        margin: '20px',
      }}
    >
      {grouped.map(({ type, nodes: typeNodes }, idx) => {
        const shown = typeNodes.slice(0, MAX_PER_TYPE);
        const remaining = typeNodes.length - shown.length;
        const isLeftCol = idx % 2 === 0;
        return (
          <div
            key={type}
            style={{
              borderTop: '1px solid #e2e8f0',
              paddingTop: '16px',
              paddingBottom: '28px',
              paddingRight: isLeftCol ? '32px' : '0',
              paddingLeft: isLeftCol ? '0' : '32px',
              borderLeft: isLeftCol ? 'none' : '1px solid #e2e8f0',
            }}
          >
            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                marginBottom: '8px',
              }}
            >
              <span
                style={{
                  fontSize: '11px',
                  fontWeight: '700',
                  textTransform: 'uppercase',
                  letterSpacing: '0.6px',
                  color: '#64748b',
                }}
              >
                {type}s
                <span
                  style={{
                    marginLeft: '6px',
                    fontWeight: '600',
                    fontSize: '10px',
                    padding: '3px 7px',
                    backgroundColor: NODE_TYPE_COLORS[type]?.bg ?? '#f1f5f9',
                    color: NODE_TYPE_COLORS[type]?.color ?? '#475569',
                    borderRadius: '8px',
                  }}
                >
                  {typeNodes.length}
                </span>
              </span>
              {remaining > 0 && (
                <a
                  href={`/namespaces/${defaultBranchNs}?type=${type}`}
                  style={{
                    fontSize: '11px',
                    color: '#3b82f6',
                    textDecoration: 'none',
                    fontWeight: '500',
                  }}
                >
                  +{remaining} more →
                </a>
              )}
            </div>
            {shown.map((node, idx) => (
              <div
                key={node.name}
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: '8px',
                  padding: '6px 0',
                  borderBottom:
                    idx < shown.length - 1 ? '1px solid #f1f5f9' : 'none',
                }}
              >
                <NodeLink
                  node={node}
                  size="large"
                  ellipsis={true}
                  style={{ flex: 1, minWidth: 0 }}
                />
                {node.owners?.length > 0 && (
                  <div style={{ display: 'flex', gap: '2px', flexShrink: 0 }}>
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
                            width: '26px',
                            height: '26px',
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
                {node.current?.updatedAt && (
                  <span
                    style={{
                      fontSize: '11px',
                      color: '#94a3b8',
                      whiteSpace: 'nowrap',
                    }}
                  >
                    {formatRelativeTime(node.current.updatedAt)}
                  </span>
                )}
              </div>
            ))}
          </div>
        );
      })}
    </div>
  );
}

import 'styles/node-list.css';
import 'styles/sorted-table.css';

function formatRelativeTime(isoString) {
  const seconds = Math.floor((Date.now() - new Date(isoString)) / 1000);
  if (seconds < 60) return 'just now';
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  if (days < 30) return `${days}d ago`;
  const months = Math.floor(days / 30);
  if (months < 12) return `${months}mo ago`;
  return `${Math.floor(months / 12)}y ago`;
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
  const [namespaceSources, setNamespaceSources] = useState({});
  // Use undefined to indicate "not yet loaded", null means "loaded but no config"
  const [gitConfig, setGitConfig] = useState(undefined);

  // Branch landing state (for git-root namespaces)
  const [branches, setBranches] = useState(null); // null = not yet fetched
  const [branchesLoading, setBranchesLoading] = useState(false);
  const [defaultBranchNodes, setDefaultBranchNodes] = useState([]);
  const [defaultBranchNodesLoading, setDefaultBranchNodesLoading] =
    useState(false);

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

  // Only show edit/add controls once git config has loaded and namespace is not git-only
  const gitConfigLoaded = gitConfig !== undefined;
  const isGitRoot =
    gitConfigLoaded &&
    !!gitConfig?.github_repo_path &&
    !gitConfig?.parent_namespace;
  const isBranchNamespace = gitConfigLoaded && !!gitConfig?.parent_namespace;
  const showEditControls =
    gitConfigLoaded && !gitConfig?.git_only && !isGitRoot;

  // Reset branches when namespace changes
  useEffect(() => {
    setBranches(null);
  }, [namespace]);

  // Fetch branches when this is a git-root namespace
  useEffect(() => {
    if (!isGitRoot) return;
    setBranchesLoading(true);
    djClient
      .getNamespaceBranches(namespace)
      .then(data => setBranches(data || []))
      .catch(() => setBranches([]))
      .finally(() => setBranchesLoading(false));
  }, [djClient, namespace, isGitRoot]);

  // Fetch default branch nodes for the TypeGroupGrid preview
  useEffect(() => {
    if (!isGitRoot || !gitConfig?.default_branch) return;
    const defaultBranchNs = `${namespace}.${gitConfig.default_branch}`;
    setDefaultBranchNodesLoading(true);
    djClient
      .listNodesForLanding(
        defaultBranchNs,
        [],
        [],
        null,
        null,
        null,
        200,
        { key: 'updatedAt', direction: 'descending' },
        null,
        {},
      )
      .then(result => {
        const nodes =
          result?.data?.findNodesPaginated?.edges?.map(e => ({
            ...e.node,
            // TypeGroupGrid reads status/mode at top level; normalize from current
            status: e.node.current?.status,
            mode: e.node.current?.mode,
          })) || [];
        setDefaultBranchNodes(nodes);
      })
      .catch(() => setDefaultBranchNodes([]))
      .finally(() => setDefaultBranchNodesLoading(false));
  }, [djClient, namespace, isGitRoot, gitConfig?.default_branch]);

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
      const namespaces = await djClient.namespaces();
      const hierarchy = createNamespaceHierarchy(namespaces);
      setNamespaceHierarchy(hierarchy);

      // Fetch sources for all namespaces in bulk
      const allNamespaceNames = namespaces.map(ns => ns.namespace);
      if (allNamespaceNames.length > 0) {
        const sourcesResponse = await djClient.namespaceSourcesBulk(
          allNamespaceNames,
        );
        if (sourcesResponse && sourcesResponse.sources) {
          setNamespaceSources(sourcesResponse.sources);
        }
      }
    };
    fetchData().catch(console.error);
  }, [djClient, djClient.namespaces]);

  useEffect(() => {
    const fetchData = async () => {
      setRetrieved(false);

      // Build extended filters for API
      const extendedFilters = {
        ownedBy: filters.ownedBy || null,
        statuses: filters.statuses ? [filters.statuses] : null,
        missingDescription: filters.missingDescription,
        hasMaterialization: filters.hasMaterialization,
        orphanedDimension: filters.orphanedDimension,
      };

      const nodes = await djClient.listNodesForLanding(
        namespace,
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
    namespace,
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

  // Select options
  const typeOptions = [
    { value: 'source', label: 'Source' },
    { value: 'transform', label: 'Transform' },
    { value: 'dimension', label: 'Dimension' },
    { value: 'metric', label: 'Metric' },
    { value: 'cube', label: 'Cube' },
  ];

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
            <a href={'/nodes/' + node.name} className="link-table">
              {isBranchNamespace && node.name.startsWith(namespace + '.')
                ? node.name.slice(namespace.length + 1)
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
            No nodes found with the current filters.
            {hasActiveFilters && (
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

          {/* Unified Filter Bar — hidden on git-root branch landing */}
          {!(isGitRoot && branches?.length > 0) && (
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

          <div className="table-responsive">
            <div
              className={`sidebar`}
              style={{ borderRight: '1px solid #e2e8f0', paddingRight: '1rem' }}
            >
              <div
                style={{
                  paddingBottom: '12px',
                  marginBottom: '8px',
                }}
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
                  Namespaces
                </span>
              </div>
              {namespaceHierarchy
                ? namespaceHierarchy.map(child => (
                    <Explorer
                      item={child}
                      current={state.namespace}
                      defaultExpand={true}
                      isTopLevel={true}
                      key={child.namespace}
                      namespaceSources={namespaceSources}
                    />
                  ))
                : null}
            </div>
            <div style={{ flex: 1, minWidth: 0, marginLeft: '1.5rem' }}>
              <NamespaceHeader
                namespace={namespace}
                onGitConfigLoaded={setGitConfig}
              >
                <a
                  href={`${getDJUrl()}/namespaces/${namespace}/export/yaml`}
                  download
                  style={{
                    display: 'inline-flex',
                    alignItems: 'center',
                    gap: '4px',
                    // padding: '6px 12px',
                    fontSize: '13px',
                    fontWeight: '500',
                    color: '#475569',
                    // backgroundColor: '#f8fafc',
                    // border: '1px solid #e2e8f0',
                    borderRadius: '6px',
                    textDecoration: 'none',
                    cursor: 'pointer',
                    transition: 'all 0.15s ease',
                    margin: '0.5em 0px 0px 1em',
                  }}
                  onMouseOver={e => {
                    e.currentTarget.style.color = '#333333';
                  }}
                  onMouseOut={e => {
                    e.currentTarget.style.color = '#475569';
                  }}
                  title="Export namespace to YAML"
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
                </a>
                {showEditControls && <AddNodeDropdown namespace={namespace} />}
              </NamespaceHeader>

              {/* Branch landing page for git-root namespaces */}
              {!gitConfigLoaded ? null : isGitRoot &&
                (branchesLoading || (branches && branches.length > 0)) ? (
                <div style={{ padding: '8px 0' }}>
                  <div
                    style={{
                      display: 'flex',
                      alignItems: 'center',
                      gap: '8px',
                      marginBottom: '16px',
                      padding: '0 4px',
                    }}
                  >
                    <svg
                      xmlns="http://www.w3.org/2000/svg"
                      width="14"
                      height="14"
                      viewBox="0 0 24 24"
                      fill="none"
                      stroke="#64748b"
                      strokeWidth="2"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                    >
                      <line x1="6" y1="3" x2="6" y2="15" />
                      <circle cx="18" cy="6" r="3" />
                      <circle cx="6" cy="18" r="3" />
                      <path d="M18 9a9 9 0 0 1-9 9" />
                    </svg>
                    <span
                      style={{
                        fontSize: '12px',
                        fontWeight: '600',
                        textTransform: 'uppercase',
                        letterSpacing: '0.5px',
                        color: '#64748b',
                      }}
                    >
                      Branches
                    </span>
                    {!branchesLoading && branches && (
                      <span
                        style={{
                          fontSize: '11px',
                          color: '#94a3b8',
                          fontWeight: 400,
                        }}
                      >
                        {branches.length}
                      </span>
                    )}
                  </div>

                  {branchesLoading ? (
                    <div
                      style={{
                        padding: '20px 4px',
                        color: '#94a3b8',
                        fontSize: '13px',
                      }}
                    >
                      <LoadingIcon />
                    </div>
                  ) : (
                    <div
                      style={{
                        display: 'grid',
                        gridTemplateColumns:
                          'repeat(auto-fill, minmax(280px, 1fr))',
                        gap: '12px',
                      }}
                    >
                      {branches.map(b => {
                        const isDefault =
                          b.git_branch === gitConfig?.default_branch ||
                          b.namespace ===
                            `${namespace}.${gitConfig?.default_branch}`;
                        return (
                          <a
                            key={b.namespace}
                            href={`/namespaces/${b.namespace}`}
                            style={{ textDecoration: 'none' }}
                          >
                            <div
                              style={{
                                padding: '14px 16px',
                                border: `1px solid ${
                                  isDefault ? '#bfdbfe' : '#e2e8f0'
                                }`,
                                borderRadius: '8px',
                                backgroundColor: isDefault
                                  ? '#f0f7ff'
                                  : '#ffffff',
                                cursor: 'pointer',
                                transition:
                                  'box-shadow 0.15s ease, border-color 0.15s ease',
                              }}
                              onMouseOver={e => {
                                e.currentTarget.style.boxShadow =
                                  '0 2px 8px rgba(0,0,0,0.08)';
                                e.currentTarget.style.borderColor = isDefault
                                  ? '#93c5fd'
                                  : '#cbd5e1';
                              }}
                              onMouseOut={e => {
                                e.currentTarget.style.boxShadow = 'none';
                                e.currentTarget.style.borderColor = isDefault
                                  ? '#bfdbfe'
                                  : '#e2e8f0';
                              }}
                            >
                              <div
                                style={{
                                  display: 'flex',
                                  alignItems: 'center',
                                  justifyContent: 'space-between',
                                  marginBottom: '8px',
                                }}
                              >
                                <div
                                  style={{
                                    display: 'flex',
                                    alignItems: 'center',
                                    gap: '6px',
                                  }}
                                >
                                  <svg
                                    xmlns="http://www.w3.org/2000/svg"
                                    width="13"
                                    height="13"
                                    viewBox="0 0 24 24"
                                    fill="none"
                                    stroke={isDefault ? '#1e40af' : '#475569'}
                                    strokeWidth="2"
                                    strokeLinecap="round"
                                    strokeLinejoin="round"
                                  >
                                    <line x1="6" y1="3" x2="6" y2="15" />
                                    <circle cx="18" cy="6" r="3" />
                                    <circle cx="6" cy="18" r="3" />
                                    <path d="M18 9a9 9 0 0 1-9 9" />
                                  </svg>
                                  <span
                                    style={{
                                      fontWeight: '600',
                                      fontSize: '14px',
                                      color: isDefault ? '#1e40af' : '#1e293b',
                                    }}
                                  >
                                    {b.git_branch || b.namespace}
                                  </span>
                                  {isDefault && (
                                    <span
                                      style={{
                                        fontSize: '10px',
                                        padding: '1px 6px',
                                        backgroundColor: '#1e40af',
                                        color: 'white',
                                        borderRadius: '10px',
                                        fontWeight: '600',
                                      }}
                                    >
                                      default
                                    </span>
                                  )}
                                </div>
                                {b.git_only && (
                                  <span
                                    style={{
                                      fontSize: '10px',
                                      padding: '1px 6px',
                                      backgroundColor: '#fef3c7',
                                      color: '#92400e',
                                      borderRadius: '10px',
                                    }}
                                  >
                                    read-only
                                  </span>
                                )}
                              </div>
                              <div
                                style={{
                                  display: 'flex',
                                  alignItems: 'center',
                                  gap: '12px',
                                  fontSize: '12px',
                                  color: '#64748b',
                                }}
                              >
                                <span
                                  style={{
                                    display: 'flex',
                                    alignItems: 'center',
                                    gap: '4px',
                                  }}
                                >
                                  <svg
                                    xmlns="http://www.w3.org/2000/svg"
                                    width="11"
                                    height="11"
                                    viewBox="0 0 24 24"
                                    fill="none"
                                    stroke="currentColor"
                                    strokeWidth="2"
                                    strokeLinecap="round"
                                    strokeLinejoin="round"
                                  >
                                    <rect x="3" y="3" width="7" height="7" />
                                    <rect x="14" y="3" width="7" height="7" />
                                    <rect x="14" y="14" width="7" height="7" />
                                    <rect x="3" y="14" width="7" height="7" />
                                  </svg>
                                  {b.num_nodes} nodes
                                </span>
                                {b.invalid_node_count > 0 && (
                                  <span
                                    style={{
                                      display: 'flex',
                                      alignItems: 'center',
                                      gap: '3px',
                                      color: '#dc2626',
                                    }}
                                  >
                                    <svg
                                      xmlns="http://www.w3.org/2000/svg"
                                      width="11"
                                      height="11"
                                      viewBox="0 0 24 24"
                                      fill="none"
                                      stroke="currentColor"
                                      strokeWidth="2"
                                      strokeLinecap="round"
                                      strokeLinejoin="round"
                                    >
                                      <circle cx="12" cy="12" r="10" />
                                      <line x1="12" y1="8" x2="12" y2="12" />
                                      <line
                                        x1="12"
                                        y1="16"
                                        x2="12.01"
                                        y2="16"
                                      />
                                    </svg>
                                    {b.invalid_node_count} invalid
                                  </span>
                                )}
                                {b.last_deployed_at && (
                                  <span
                                    style={{
                                      display: 'flex',
                                      alignItems: 'center',
                                      gap: '3px',
                                      color: '#94a3b8',
                                    }}
                                    title={new Date(
                                      b.last_deployed_at,
                                    ).toLocaleString()}
                                  >
                                    <svg
                                      xmlns="http://www.w3.org/2000/svg"
                                      width="11"
                                      height="11"
                                      viewBox="0 0 24 24"
                                      fill="none"
                                      stroke="currentColor"
                                      strokeWidth="2"
                                      strokeLinecap="round"
                                      strokeLinejoin="round"
                                    >
                                      <circle cx="12" cy="12" r="10" />
                                      <polyline points="12 6 12 12 16 14" />
                                    </svg>
                                    {formatRelativeTime(b.last_deployed_at)}
                                  </span>
                                )}
                              </div>
                            </div>
                          </a>
                        );
                      })}
                    </div>
                  )}

                  {/* Default branch node preview grouped by type */}
                  {gitConfig?.default_branch && (
                    <div style={{ marginTop: '28px' }}>
                      <div
                        style={{
                          borderTop: '1px solid #e2e8f0',
                          marginBottom: '20px',
                        }}
                      />
                      <div
                        style={{
                          display: 'flex',
                          alignItems: 'center',
                          justifyContent: 'space-between',
                          marginBottom: '12px',
                          padding: '0 4px',
                        }}
                      >
                        <div
                          style={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: '8px',
                          }}
                        >
                          <span
                            style={{
                              fontSize: '12px',
                              fontWeight: '600',
                              textTransform: 'uppercase',
                              letterSpacing: '0.5px',
                              color: '#64748b',
                            }}
                          >
                            {gitConfig.default_branch}
                          </span>
                          <span
                            style={{
                              fontSize: '10px',
                              padding: '1px 6px',
                              backgroundColor: '#1e40af',
                              color: 'white',
                              borderRadius: '10px',
                              fontWeight: '600',
                            }}
                          >
                            default
                          </span>
                        </div>
                        <a
                          href={`/namespaces/${namespace}.${gitConfig.default_branch}`}
                          style={{
                            fontSize: '12px',
                            color: '#3b82f6',
                            textDecoration: 'none',
                          }}
                        >
                          View all →
                        </a>
                      </div>
                      {defaultBranchNodesLoading ? (
                        <LoadingIcon />
                      ) : (
                        <DefaultBranchPreview
                          nodes={defaultBranchNodes}
                          defaultBranchNs={`${namespace}.${gitConfig.default_branch}`}
                        />
                      )}
                    </div>
                  )}
                </div>
              ) : (
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
