import React, { useContext, useEffect, useRef, useState } from 'react';
import CollapsedIcon from '../../icons/CollapsedIcon';
import ExpandedIcon from '../../icons/ExpandedIcon';
import AddItemIcon from '../../icons/AddItemIcon';
import DJClientContext from '../../providers/djclient';

const Explorer = ({
  item = [],
  current,
  gitRoots = new Set(),
  pinnedSet,
  onTogglePin,
}) => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [items, setItems] = useState([]);
  const [expand, setExpand] = useState(false);
  const [highlight, setHighlight] = useState(false);
  const [showAddButton, setShowAddButton] = useState(false);
  const [isCreatingChild, setIsCreatingChild] = useState(false);
  const [newNamespace, setNewNamespace] = useState('');
  const [error, setError] = useState('');
  const [menuOpen, setMenuOpen] = useState(false);
  const inputRef = useRef(null);
  const formRef = useRef(null);
  const menuRef = useRef(null);

  useEffect(() => {
    setItems(item);
    setHighlight(current);
    if (current !== undefined && current?.startsWith(item.path)) {
      setExpand(true);
    } else setExpand(false);
  }, [current, item]);

  useEffect(() => {
    if (isCreatingChild && inputRef.current) {
      inputRef.current.focus();
    }
  }, [isCreatingChild]);

  useEffect(() => {
    const handleClickOutside = event => {
      if (formRef.current && !formRef.current.contains(event.target)) {
        handleCancelAdd();
      }
    };

    if (isCreatingChild) {
      document.addEventListener('mousedown', handleClickOutside);
      return () => {
        document.removeEventListener('mousedown', handleClickOutside);
      };
    }
  }, [isCreatingChild]);

  useEffect(() => {
    const handleClickOutside = event => {
      if (menuRef.current && !menuRef.current.contains(event.target)) {
        setMenuOpen(false);
      }
    };
    if (menuOpen) {
      document.addEventListener('mousedown', handleClickOutside);
      return () => {
        document.removeEventListener('mousedown', handleClickOutside);
      };
    }
  }, [menuOpen]);

  const handleClickOnParent = e => {
    e.stopPropagation();
    setExpand(prev => {
      return !prev;
    });
  };

  const handleAddNamespace = async e => {
    e.preventDefault();
    if (!newNamespace.trim()) {
      setError('Namespace cannot be empty');
      return;
    }

    const fullNamespace = items.path
      ? `${items.path}.${newNamespace}`
      : newNamespace;

    const response = await djClient.addNamespace(fullNamespace);
    if (response.status === 200 || response.status === 201) {
      setIsCreatingChild(false);
      setNewNamespace('');
      setError('');
      window.location.href = `/namespaces/${fullNamespace}`;
    } else {
      setError(response.json?.message || 'Failed to create namespace');
    }
  };

  const handleCancelAdd = () => {
    setIsCreatingChild(false);
    setNewNamespace('');
    setError('');
  };

  const handleKeyDown = e => {
    if (e.key === 'Enter') {
      handleAddNamespace(e);
    } else if (e.key === 'Escape') {
      handleCancelAdd();
    }
  };

  // A namespace is git-backed when it IS a git root or sits under one. Git-backed
  // namespaces are managed via git, so we don't offer "add child" in the UI.
  const path = items.path;
  const isGitBacked =
    !!path && [...gitRoots].some(r => path === r || path.startsWith(`${r}.`));
  const isPinned = !!(pinnedSet && path && pinnedSet.has(path));

  return (
    <>
      <div className="namespace-item" style={{ position: 'relative' }}>
        <div
          className={`select-name ${
            highlight === items.path ? 'select-name-highlight' : ''
          }`}
          onClick={handleClickOnParent}
          onMouseEnter={() => setShowAddButton(true)}
          onMouseLeave={() => setShowAddButton(false)}
          style={{
            display: 'flex',
            alignItems: 'center',
            width: '100%',
            position: 'relative',
          }}
        >
          {items.children && items.children.length > 0 ? (
            <span
              style={{
                fontSize: '10px',
                color: '#94a3b8',
                width: '12px',
                minWidth: '12px',
                flexShrink: 0,
                display: 'flex',
                alignItems: 'center',
              }}
            >
              {!expand ? <CollapsedIcon /> : <ExpandedIcon />}
            </span>
          ) : (
            <span style={{ width: '12px', minWidth: '12px', flexShrink: 0 }} />
          )}
          <a
            href={`/namespaces/${items.path}`}
            title={items.namespace}
            style={{
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              whiteSpace: 'nowrap',
              minWidth: 0,
            }}
          >
            {items.namespace}
          </a>
          {/* Git root badge */}
          {gitRoots.has(items.path) && (
            <span
              title="Git-backed namespace"
              style={{
                marginLeft: '6px',
                fontSize: '9px',
                padding: '1px 4px',
                borderRadius: '3px',
                backgroundColor: '#d4edda',
                color: '#155724',
                display: 'inline-flex',
                alignItems: 'center',
                gap: '2px',
              }}
            >
              <svg
                xmlns="http://www.w3.org/2000/svg"
                width="10"
                height="10"
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
              Git
            </span>
          )}
          {(onTogglePin || !isGitBacked) && (
            <div
              ref={menuRef}
              className="dj-ns-row-actions"
              onClick={e => e.stopPropagation()}
            >
              {isPinned && onTogglePin && (
                <span
                  className="dj-ns-pin-indicator"
                  aria-hidden="true"
                  title="Pinned"
                >
                  ★
                </span>
              )}
              <button
                type="button"
                className="dj-ns-kebab"
                aria-label={`Actions for ${path}`}
                aria-haspopup="menu"
                aria-expanded={menuOpen}
                onClick={e => {
                  e.stopPropagation();
                  setMenuOpen(o => !o);
                }}
                style={{
                  opacity: showAddButton || menuOpen ? 1 : 0,
                  visibility: showAddButton || menuOpen ? 'visible' : 'hidden',
                }}
              >
                <svg
                  width="16"
                  height="16"
                  viewBox="0 0 24 24"
                  fill="currentColor"
                  aria-hidden="true"
                >
                  <circle cx="5" cy="12" r="2" />
                  <circle cx="12" cy="12" r="2" />
                  <circle cx="19" cy="12" r="2" />
                </svg>
              </button>
              {menuOpen && (
                <div role="menu" className="dj-ns-row-menu">
                  {onTogglePin && (
                    <button
                      type="button"
                      role="menuitem"
                      className="dj-ns-row-menu-item"
                      onClick={e => {
                        e.stopPropagation();
                        onTogglePin(path);
                        setMenuOpen(false);
                      }}
                    >
                      <span
                        className={`dj-ns-mi-icon${isPinned ? ' pinned' : ''}`}
                      >
                        ★
                      </span>
                      {isPinned ? 'Unpin namespace' : 'Pin namespace'}
                    </button>
                  )}
                  {!isGitBacked && (
                    <button
                      type="button"
                      role="menuitem"
                      className="dj-ns-row-menu-item"
                      onClick={e => {
                        e.stopPropagation();
                        setIsCreatingChild(true);
                        setExpand(true);
                        setMenuOpen(false);
                      }}
                    >
                      <span className="dj-ns-mi-icon">
                        <AddItemIcon />
                      </span>
                      Add child namespace
                    </button>
                  )}
                </div>
              )}
            </div>
          )}
        </div>
      </div>
      {(items.children || isCreatingChild) && (
        <div>
          {isCreatingChild && (
            <div
              style={{
                paddingLeft: '0.25rem',
                marginLeft: '0.25rem',
                borderLeft: '1px solid #e2e8f0',
                marginTop: '2px',
              }}
            >
              <form
                ref={formRef}
                onSubmit={handleAddNamespace}
                style={{
                  display: 'flex',
                  flexDirection: 'column',
                  gap: '4px',
                }}
              >
                <div
                  style={{ display: 'flex', gap: '4px', alignItems: 'center' }}
                >
                  <input
                    ref={inputRef}
                    type="text"
                    value={newNamespace}
                    onChange={e => setNewNamespace(e.target.value)}
                    onKeyDown={handleKeyDown}
                    placeholder="New namespace name"
                    style={{
                      padding: '4px 8px',
                      fontSize: '0.875rem',
                      border: '1px solid #ccc',
                      borderRadius: '4px',
                      flex: 1,
                    }}
                  />
                  <button
                    type="submit"
                    style={{
                      padding: '4px 8px',
                      fontSize: '0.75rem',
                      background: '#007bff',
                      color: 'white',
                      border: 'none',
                      borderRadius: '4px',
                      cursor: 'pointer',
                      margin: '0 1em',
                    }}
                  >
                    ✓
                  </button>
                </div>
                {error && (
                  <span style={{ color: 'red', fontSize: '0.75rem' }}>
                    {error}
                  </span>
                )}
              </form>
            </div>
          )}
          {items.children &&
            items.children.map((item, index) => (
              <div
                style={{
                  paddingLeft: '0.25rem',
                  marginLeft: '0.25rem',
                  borderLeft: '1px solid #e2e8f0',
                }}
                key={index}
              >
                <div
                  className={`${expand ? '' : 'inactive'}`}
                  key={`nested-${index}`}
                >
                  <Explorer
                    item={item}
                    current={highlight}
                    gitRoots={gitRoots}
                    pinnedSet={pinnedSet}
                    onTogglePin={onTogglePin}
                  />
                </div>
              </div>
            ))}
        </div>
      )}
    </>
  );
};

export default Explorer;
