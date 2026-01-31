import { useContext, useEffect, useState, useRef } from 'react';
import DJClientContext from '../providers/djclient';
import {
  GitSettingsModal,
  CreateBranchModal,
  SyncToGitModal,
  CreatePRModal,
  DeleteBranchModal,
} from './git';

export default function NamespaceHeader({
  namespace,
  children,
  onGitConfigLoaded,
}) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [sources, setSources] = useState(null);
  const [recentDeployments, setRecentDeployments] = useState([]);
  const [deploymentsDropdownOpen, setDeploymentsDropdownOpen] = useState(false);
  const dropdownRef = useRef(null);

  // Git config state
  const [gitConfig, setGitConfig] = useState(null);
  const [gitConfigLoading, setGitConfigLoading] = useState(true);
  const [parentGitConfig, setParentGitConfig] = useState(null);
  const [existingPR, setExistingPR] = useState(null);

  // Modal states
  const [showGitSettings, setShowGitSettings] = useState(false);
  const [showCreateBranch, setShowCreateBranch] = useState(false);
  const [showSyncToGit, setShowSyncToGit] = useState(false);
  const [showCreatePR, setShowCreatePR] = useState(false);
  const [showDeleteBranch, setShowDeleteBranch] = useState(false);

  useEffect(() => {
    // Reset loading state when namespace changes
    setGitConfigLoading(true);

    const fetchData = async () => {
      if (namespace) {
        // Fetch deployment sources
        try {
          const data = await djClient.namespaceSources(namespace);
          setSources(data);

          try {
            const deployments = await djClient.listDeployments(namespace, 5);
            setRecentDeployments(deployments || []);
          } catch (err) {
            console.error('Failed to fetch deployments:', err);
            setRecentDeployments([]);
          }
        } catch (e) {
          // Silently fail - badge just won't show
        }

        // Fetch git config
        try {
          const config = await djClient.getNamespaceGitConfig(namespace);
          setGitConfig(config);
          if (onGitConfigLoaded) {
            onGitConfigLoaded(config);
          }

          // If this is a branch namespace, fetch parent's git config and check for existing PR
          if (config?.parent_namespace) {
            try {
              const parentConfig = await djClient.getNamespaceGitConfig(
                config.parent_namespace,
              );
              setParentGitConfig(parentConfig);
            } catch (e) {
              console.error('Failed to fetch parent git config:', e);
            }

            // Check for existing PR
            try {
              const pr = await djClient.getPullRequest(namespace);
              setExistingPR(pr);
            } catch (e) {
              // No PR or error - that's fine
              setExistingPR(null);
            }
          }
        } catch (e) {
          // Git config not available
          setGitConfig(null);
          if (onGitConfigLoaded) {
            onGitConfigLoaded(null);
          }
        } finally {
          setGitConfigLoading(false);
        }
      }
    };
    fetchData();
  }, [djClient, namespace]);

  // Close dropdown when clicking outside
  useEffect(() => {
    const handleClickOutside = event => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target)) {
        setDeploymentsDropdownOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const namespaceParts = namespace ? namespace.split('.') : [];

  const hasGitConfig = gitConfig?.github_repo_path && gitConfig?.git_branch;
  const isBranchNamespace = !!gitConfig?.parent_namespace;

  // Handlers for git operations
  const handleSaveGitConfig = async config => {
    const result = await djClient.updateNamespaceGitConfig(namespace, config);
    if (!result?._error) {
      setGitConfig(result);
    }
    return result;
  };
  const handleRemoveGitConfig = async () => {
    const result = await djClient.deleteNamespaceGitConfig(namespace);
    if (!result?._error) {
      setGitConfig(null);
    }
  };

  const handleCreateBranch = async branchName => {
    return await djClient.createBranch(namespace, branchName);
  };

  const handleSyncToGit = async commitMessage => {
    return await djClient.syncNamespaceToGit(namespace, commitMessage);
  };

  const handleCreatePR = async (title, body, onProgress) => {
    // First sync changes to git using PR title as commit message
    if (onProgress) onProgress('syncing');
    const syncResult = await djClient.syncNamespaceToGit(namespace, title);
    if (syncResult?._error) {
      return syncResult;
    }

    // Then create the PR
    if (onProgress) onProgress('creating');
    const result = await djClient.createPullRequest(namespace, title, body);
    if (result && !result._error) {
      setExistingPR(result);
    }
    return result;
  };

  const handleDeleteBranch = async deleteGitBranch => {
    return await djClient.deleteBranch(
      gitConfig.parent_namespace,
      namespace,
      deleteGitBranch,
    );
  };

  // Button style helpers
  const buttonStyle = {
    height: '28px',
    padding: '0 10px',
    fontSize: '12px',
    border: '1px solid #e2e8f0',
    borderRadius: '4px',
    backgroundColor: '#ffffff',
    color: '#475569',
    cursor: 'pointer',
    display: 'flex',
    alignItems: 'center',
    gap: '4px',
    whiteSpace: 'nowrap',
  };

  const primaryButtonStyle = {
    ...buttonStyle,
    backgroundColor: '#3b82f6',
    borderColor: '#3b82f6',
    color: '#ffffff',
  };

  return (
    <div
      style={{
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        padding: '12px 12px 12px 20px',
        marginBottom: '16px',
        borderTop: '1px solid #e2e8f0',
        borderBottom: '1px solid #e2e8f0',
        background: '#ffffff',
      }}
    >
      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
        <a href="/" style={{ display: 'flex', alignItems: 'center' }}>
          <svg
            xmlns="http://www.w3.org/2000/svg"
            width="16"
            height="16"
            fill="currentColor"
            viewBox="0 0 16 16"
          >
            <path d="M8.186 1.113a.5.5 0 0 0-.372 0L1.846 3.5 8 5.961 14.154 3.5 8.186 1.113zM15 4.239l-6.5 2.6v7.922l6.5-2.6V4.24zM7.5 14.762V6.838L1 4.239v7.923l6.5 2.6zM7.443.184a1.5 1.5 0 0 1 1.114 0l7.129 2.852A.5.5 0 0 1 16 3.5v8.662a1 1 0 0 1-.629.928l-7.185 2.874a.5.5 0 0 1-.372 0L.63 13.09a1 1 0 0 1-.63-.928V3.5a.5.5 0 0 1 .314-.464L7.443.184z" />
          </svg>
        </a>
        <svg
          xmlns="http://www.w3.org/2000/svg"
          width="12"
          height="12"
          fill="#6c757d"
          viewBox="0 0 16 16"
        >
          <path
            fillRule="evenodd"
            d="M4.646 1.646a.5.5 0 0 1 .708 0l6 6a.5.5 0 0 1 0 .708l-6 6a.5.5 0 0 1-.708-.708L10.293 8 4.646 2.354a.5.5 0 0 1 0-.708z"
          />
        </svg>
        {namespace ? (
          namespaceParts.map((part, index, arr) => (
            <span
              key={index}
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: '8px',
              }}
            >
              <a
                href={`/namespaces/${arr.slice(0, index + 1).join('.')}`}
                style={{
                  fontWeight: '400',
                  color: '#1e293b',
                  textDecoration: 'none',
                }}
              >
                {part}
              </a>
              {index < arr.length - 1 && (
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  width="12"
                  height="12"
                  fill="#94a3b8"
                  viewBox="0 0 16 16"
                >
                  <path
                    fillRule="evenodd"
                    d="M4.646 1.646a.5.5 0 0 1 .708 0l6 6a.5.5 0 0 1 0 .708l-6 6a.5.5 0 0 1-.708-.708L10.293 8 4.646 2.354a.5.5 0 0 1 0-.708z"
                  />
                </svg>
              )}
            </span>
          ))
        ) : (
          <span style={{ fontWeight: '600', color: '#1e293b' }}>
            All Namespaces
          </span>
        )}

        {/* Branch indicator */}
        {isBranchNamespace && (
          <span
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: '4px',
              padding: '2px 8px',
              backgroundColor: '#dbeafe',
              borderRadius: '12px',
              fontSize: '11px',
              color: '#1e40af',
              marginLeft: '4px',
            }}
          >
            <svg
              xmlns="http://www.w3.org/2000/svg"
              width="12"
              height="12"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
            >
              <line x1="6" y1="3" x2="6" y2="15" />
              <circle cx="18" cy="6" r="3" />
              <circle cx="6" cy="18" r="3" />
              <path d="M18 9a9 9 0 0 1-9 9" />
            </svg>
            Branch of{' '}
            <a
              href={`/namespaces/${gitConfig.parent_namespace}`}
              style={{ color: '#1e40af', textDecoration: 'underline' }}
            >
              {gitConfig.parent_namespace}
            </a>
          </span>
        )}

        {/* Git-only (read-only) indicator */}
        {gitConfig?.git_only && (
          <span
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: '4px',
              padding: '2px 8px',
              backgroundColor: '#fef3c7',
              borderRadius: '12px',
              fontSize: '11px',
              color: '#92400e',
              marginLeft: '4px',
            }}
            title="This namespace is git-only. Changes must be made via git and deployed."
          >
            <svg
              xmlns="http://www.w3.org/2000/svg"
              width="12"
              height="12"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
            >
              <rect x="3" y="11" width="18" height="11" rx="2" ry="2" />
              <path d="M7 11V7a5 5 0 0 1 10 0v4" />
            </svg>
            Read-only
          </span>
        )}

        {/* Deployment badge + dropdown (existing functionality) */}
        {sources && sources.total_deployments > 0 && (
          <div
            style={{ position: 'relative', marginLeft: '8px' }}
            ref={dropdownRef}
          >
            <button
              onClick={() =>
                setDeploymentsDropdownOpen(!deploymentsDropdownOpen)
              }
              style={{
                height: '32px',
                padding: '0 12px',
                fontSize: '12px',
                border: 'none',
                borderRadius: '4px',
                backgroundColor: '#ffffff',
                color: '#0b3d91',
                cursor: 'pointer',
                display: 'flex',
                alignItems: 'center',
                gap: '4px',
                whiteSpace: 'nowrap',
              }}
            >
              {sources.primary_source?.type === 'git' ? (
                <>
                  <svg
                    xmlns="http://www.w3.org/2000/svg"
                    width="12"
                    height="12"
                    viewBox="0 0 24 24"
                    fill="none"
                    stroke="currentColor"
                    strokeWidth="2"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  >
                    <line x1="6" y1="3" x2="6" y2="15"></line>
                    <circle cx="18" cy="6" r="3"></circle>
                    <circle cx="6" cy="18" r="3"></circle>
                    <path d="M18 9a9 9 0 0 1-9 9"></path>
                  </svg>
                  Deployed from Git
                </>
              ) : (
                <>
                  <svg
                    xmlns="http://www.w3.org/2000/svg"
                    width="12"
                    height="12"
                    viewBox="0 0 24 24"
                    fill="none"
                    stroke="currentColor"
                    strokeWidth="2"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  >
                    <circle cx="12" cy="7" r="4" />
                    <path d="M5.5 21a6.5 6.5 0 0 1 13 0Z" />
                  </svg>
                  Local Deploy
                </>
              )}
              <span style={{ fontSize: '8px' }}>
                {deploymentsDropdownOpen ? '▲' : '▼'}
              </span>
            </button>

            {deploymentsDropdownOpen && (
              <div
                style={{
                  position: 'absolute',
                  top: '100%',
                  left: 0,
                  marginTop: '4px',
                  padding: '12px',
                  backgroundColor: 'white',
                  border: '1px solid #ddd',
                  borderRadius: '8px',
                  boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
                  zIndex: 1000,
                  minWidth: 'max-content',
                }}
              >
                {sources.primary_source?.type === 'git' ? (
                  <a
                    href={
                      sources.primary_source.repository?.startsWith('http')
                        ? sources.primary_source.repository
                        : `https://${sources.primary_source.repository}`
                    }
                    target="_blank"
                    rel="noopener noreferrer"
                    style={{
                      display: 'flex',
                      alignItems: 'center',
                      gap: '8px',
                      fontSize: '13px',
                      fontWeight: 400,
                      textDecoration: 'none',
                      marginBottom: '12px',
                    }}
                  >
                    <svg
                      width="16"
                      height="16"
                      viewBox="0 0 24 24"
                      fill="none"
                      stroke="currentColor"
                      strokeWidth="2"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                    >
                      <line x1="6" y1="3" x2="6" y2="15" />
                      <circle cx="18" cy="6" r="3" />
                      <circle cx="6" cy="18" r="3" />
                      <path d="M18 9a9 9 0 0 1-9 9" />
                    </svg>
                    {sources.primary_source.repository}
                    {sources.primary_source.branch &&
                      ` (${sources.primary_source.branch})`}
                  </a>
                ) : (
                  <div
                    style={{
                      display: 'flex',
                      alignItems: 'center',
                      gap: '8px',
                      fontSize: '13px',
                      fontWeight: 600,
                      color: '#0b3d91',
                      marginBottom: '12px',
                    }}
                  >
                    <svg
                      width="16"
                      height="16"
                      viewBox="0 0 24 24"
                      fill="none"
                      stroke="currentColor"
                      strokeWidth="2"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                    >
                      <circle cx="12" cy="7" r="4" />
                      <path d="M5.5 21a6.5 6.5 0 0 1 13 0Z" />
                    </svg>
                    {recentDeployments?.[0]?.created_by
                      ? `Local deploys by ${recentDeployments[0].created_by}`
                      : 'Local/adhoc deployments'}
                  </div>
                )}

                {/* Separator */}
                <div
                  style={{
                    height: '1px',
                    backgroundColor: '#e2e8f0',
                    marginBottom: '8px',
                  }}
                />

                {/* Recent deployments list */}
                {recentDeployments?.length > 0 ? (
                  recentDeployments.map((d, idx) => {
                    const isGit = d.source?.type === 'git';
                    const statusColor =
                      d.status === 'success'
                        ? '#22c55e'
                        : d.status === 'failed'
                        ? '#ef4444'
                        : '#94a3b8';

                    const commitUrl =
                      isGit && d.source?.repository && d.source?.commit_sha
                        ? `${
                            d.source.repository.startsWith('http')
                              ? d.source.repository
                              : `https://${d.source.repository}`
                          }/commit/${d.source.commit_sha}`
                        : null;

                    const detail = isGit
                      ? d.source?.branch || 'main'
                      : d.source?.reason || d.source?.hostname || 'adhoc';

                    const shortSha = d.source?.commit_sha?.slice(0, 7);

                    return (
                      <div
                        key={`${d.uuid}-${idx}`}
                        style={{
                          display: 'grid',
                          gridTemplateColumns: '18px 1fr auto',
                          alignItems: 'center',
                          gap: '8px',
                          padding: '6px 0',
                          borderBottom:
                            idx === recentDeployments.length - 1
                              ? 'none'
                              : '1px solid #f1f5f9',
                          fontSize: '12px',
                        }}
                      >
                        {/* Status dot */}
                        <div
                          style={{
                            width: '8px',
                            height: '8px',
                            borderRadius: '50%',
                            backgroundColor: statusColor,
                          }}
                          title={d.status}
                        />

                        {/* User + detail */}
                        <div
                          style={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: '6px',
                            minWidth: 0,
                          }}
                        >
                          <span
                            style={{
                              fontWeight: 500,
                              color: '#0f172a',
                              whiteSpace: 'nowrap',
                            }}
                          >
                            {d.created_by || 'unknown'}
                          </span>
                          <span style={{ color: '#cbd5e1' }}>—</span>
                          {isGit ? (
                            <>
                              <span
                                style={{
                                  color: '#64748b',
                                  whiteSpace: 'nowrap',
                                }}
                              >
                                {detail}
                              </span>
                              {shortSha && (
                                <>
                                  <span style={{ color: '#cbd5e1' }}>@</span>
                                  {commitUrl ? (
                                    <a
                                      href={commitUrl}
                                      target="_blank"
                                      rel="noopener noreferrer"
                                      style={{
                                        fontFamily: 'monospace',
                                        fontSize: '11px',
                                        color: '#3b82f6',
                                        textDecoration: 'none',
                                      }}
                                    >
                                      {shortSha}
                                    </a>
                                  ) : (
                                    <span
                                      style={{
                                        fontFamily: 'monospace',
                                        fontSize: '11px',
                                        color: '#64748b',
                                      }}
                                    >
                                      {shortSha}
                                    </span>
                                  )}
                                </>
                              )}
                            </>
                          ) : (
                            <span
                              style={{
                                color: '#64748b',
                                overflow: 'hidden',
                                textOverflow: 'ellipsis',
                                whiteSpace: 'nowrap',
                              }}
                            >
                              {detail}
                            </span>
                          )}
                        </div>

                        {/* Timestamp */}
                        <span
                          style={{
                            color: '#94a3b8',
                            fontSize: '11px',
                            whiteSpace: 'nowrap',
                          }}
                        >
                          {new Date(d.created_at).toLocaleDateString()}
                        </span>
                      </div>
                    );
                  })
                ) : (
                  <div style={{ color: '#94a3b8', fontSize: '12px' }}>
                    No recent deployments
                  </div>
                )}
              </div>
            )}
          </div>
        )}
      </div>

      {/* Right side: git actions + children */}
      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
        {/* Git controls for non-branch namespaces */}
        {namespace && !isBranchNamespace && !gitConfigLoading && (
          <>
            <button
              style={buttonStyle}
              onClick={() => setShowGitSettings(true)}
              title="Git Settings"
            >
              <svg
                xmlns="http://www.w3.org/2000/svg"
                width="14"
                height="14"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
              >
                <circle cx="12" cy="12" r="3" />
                <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z" />
              </svg>
              Git Settings
            </button>
            {hasGitConfig ? (
              <button
                style={primaryButtonStyle}
                onClick={() => setShowCreateBranch(true)}
              >
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  width="14"
                  height="14"
                  viewBox="0 0 24 24"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                >
                  <line x1="12" y1="5" x2="12" y2="19" />
                  <line x1="5" y1="12" x2="19" y2="12" />
                </svg>
                New Branch
              </button>
            ) : (
              <></>
            )}
          </>
        )}

        {/* Git controls for branch namespaces */}
        {isBranchNamespace && hasGitConfig && (
          <>
            <button style={buttonStyle} onClick={() => setShowSyncToGit(true)}>
              <svg
                xmlns="http://www.w3.org/2000/svg"
                width="14"
                height="14"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
              >
                <path d="M21 12a9 9 0 0 1-9 9m9-9a9 9 0 0 0-9-9m9 9H3m9 9a9 9 0 0 1-9-9m9 9c1.66 0 3-4.03 3-9s-1.34-9-3-9m0 18c-1.66 0-3-4.03-3-9s1.34-9 3-9m-9 9a9 9 0 0 1 9-9" />
              </svg>
              Sync to Git
            </button>
            {existingPR ? (
              <a
                href={existingPR.pr_url}
                target="_blank"
                rel="noopener noreferrer"
                style={{
                  ...primaryButtonStyle,
                  textDecoration: 'none',
                  backgroundColor: '#16a34a',
                  borderColor: '#16a34a',
                }}
              >
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  width="14"
                  height="14"
                  viewBox="0 0 24 24"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                >
                  <circle cx="18" cy="18" r="3" />
                  <circle cx="6" cy="6" r="3" />
                  <path d="M13 6h3a2 2 0 0 1 2 2v7" />
                  <line x1="6" y1="9" x2="6" y2="21" />
                </svg>
                View PR #{existingPR.pr_number}
              </a>
            ) : (
              <button
                style={primaryButtonStyle}
                onClick={() => setShowCreatePR(true)}
              >
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  width="14"
                  height="14"
                  viewBox="0 0 24 24"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                >
                  <circle cx="18" cy="18" r="3" />
                  <circle cx="6" cy="6" r="3" />
                  <path d="M13 6h3a2 2 0 0 1 2 2v7" />
                  <line x1="6" y1="9" x2="6" y2="21" />
                </svg>
                Create PR
              </button>
            )}
            <button
              style={{
                ...buttonStyle,
                color: '#dc2626',
                borderColor: '#fecaca',
              }}
              onClick={() => setShowDeleteBranch(true)}
              title="Delete Branch"
            >
              <svg
                xmlns="http://www.w3.org/2000/svg"
                width="14"
                height="14"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
              >
                <path d="M3 6h18" />
                <path d="M19 6v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6" />
                <path d="M8 6V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2" />
              </svg>
            </button>
          </>
        )}

        {/* Additional actions passed as children */}
        {children}
      </div>

      {/* Modals */}
      <GitSettingsModal
        isOpen={showGitSettings}
        onClose={() => setShowGitSettings(false)}
        onSave={handleSaveGitConfig}
        onRemove={handleRemoveGitConfig}
        currentConfig={gitConfig}
        namespace={namespace}
      />

      <CreateBranchModal
        isOpen={showCreateBranch}
        onClose={() => setShowCreateBranch(false)}
        onCreate={handleCreateBranch}
        namespace={namespace}
        gitBranch={gitConfig?.git_branch}
      />

      <SyncToGitModal
        isOpen={showSyncToGit}
        onClose={() => setShowSyncToGit(false)}
        onSync={handleSyncToGit}
        namespace={namespace}
        gitBranch={gitConfig?.git_branch}
        repoPath={gitConfig?.github_repo_path}
      />

      <CreatePRModal
        isOpen={showCreatePR}
        onClose={() => setShowCreatePR(false)}
        onCreate={handleCreatePR}
        namespace={namespace}
        gitBranch={gitConfig?.git_branch}
        parentBranch={parentGitConfig?.git_branch}
        repoPath={gitConfig?.github_repo_path}
      />

      <DeleteBranchModal
        isOpen={showDeleteBranch}
        onClose={() => setShowDeleteBranch(false)}
        onDelete={handleDeleteBranch}
        namespace={namespace}
        gitBranch={gitConfig?.git_branch}
        parentNamespace={gitConfig?.parent_namespace}
      />
    </div>
  );
}
