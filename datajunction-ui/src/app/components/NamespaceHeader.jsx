import { useContext, useEffect, useState, useRef } from 'react';
import DJClientContext from '../providers/djclient';

export default function NamespaceHeader({ namespace, children }) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [sources, setSources] = useState(null);
  const [recentDeployments, setRecentDeployments] = useState([]);
  const [deploymentsDropdownOpen, setDeploymentsDropdownOpen] = useState(false);
  const dropdownRef = useRef(null);

  useEffect(() => {
    const fetchSources = async () => {
      if (namespace) {
        try {
          const data = await djClient.namespaceSources(namespace);
          setSources(data);

          // Fetch recent deployments for this namespace
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
      }
    };
    fetchSources();
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

        {/* Deployment badge + dropdown */}
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
                  Git Managed
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

      {/* Right side actions passed as children */}
      {children && (
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          {children}
        </div>
      )}
    </div>
  );
}
