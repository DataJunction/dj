import React, { useContext, useEffect, useRef, useState } from 'react';
import CollapsedIcon from '../../icons/CollapsedIcon';
import ExpandedIcon from '../../icons/ExpandedIcon';
import AddItemIcon from '../../icons/AddItemIcon';
import DJClientContext from '../../providers/djclient';

const Explorer = ({ item = [], current, isTopLevel = false }) => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [items, setItems] = useState([]);
  const [expand, setExpand] = useState(false);
  const [highlight, setHighlight] = useState(false);
  const [showAddButton, setShowAddButton] = useState(false);
  const [isCreatingChild, setIsCreatingChild] = useState(false);
  const [newNamespace, setNewNamespace] = useState('');
  const [error, setError] = useState('');
  const inputRef = useRef(null);
  const formRef = useRef(null);

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
    const handleClickOutside = (event) => {
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

    const fullNamespace = items.path ? `${items.path}.${newNamespace}` : newNamespace;
    
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

  return (
    <>
      <div
        className="namespace-item"
        style={{ position: 'relative' }}
      >
        <div
          className={`select-name ${
            highlight === items.path ? 'select-name-highlight' : ''
          }`}
          onClick={handleClickOnParent}
          onMouseEnter={() => setShowAddButton(true)}
          onMouseLeave={() => setShowAddButton(false)}
          style={{ display: 'inline-flex', alignItems: 'center', width: '100%', position: 'relative', 
             }}
        >
          {items.children && items.children.length > 0 ? (
            <span style={{ marginRight: '4px' }}>
              {!expand ? <CollapsedIcon /> : <ExpandedIcon />}
            </span>
          ) : (
            <span style={{ left: '-18px' }} />
          )}
          <a href={`/namespaces/${items.path}`}>{items.namespace}</a>
          <button
            className="namespace-add-button"
            onClick={(e) => {
              e.stopPropagation();
              setIsCreatingChild(true);
              setExpand(true);
            }}
            title="Add child namespace"
            style={{
              position: 'absolute',
              right: '0',
              padding: '2px 6px',
              border: 'none',
              background: 'transparent',
              cursor: 'pointer',
              opacity: showAddButton ? 0.6 : 0,
              visibility: showAddButton ? 'visible' : 'hidden',
              display: 'inline-flex',
              alignItems: 'center',
              transition: 'opacity 0.15s ease',
            }}
          >
            <AddItemIcon />
          </button>
        </div>
      </div>
      {(items.children || isCreatingChild) && (
        <div>
          {isCreatingChild && (
            <div
              style={{
                paddingLeft: '1.4rem',
                marginLeft: '1rem',
                borderLeft: '1px solid rgb(218 233 255)',
                marginTop: '5px',
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
                <div style={{ display: 'flex', gap: '4px', alignItems: 'center' }}>
                  <input
                    ref={inputRef}
                    type="text"
                    value={newNamespace}
                    onChange={(e) => setNewNamespace(e.target.value)}
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
                  paddingLeft: '1.4rem',
                  marginLeft: '1rem',
                  borderLeft: '1px solid rgb(218 233 255)',
                }}
                key={index}
              >
                <div
                  className={`${expand ? '' : 'inactive'}`}
                  key={`nested-${index}`}
                >
                  <Explorer item={item} current={highlight} isTopLevel={false} />
                </div>
              </div>
            ))}
        </div>
      )}
    </>
  );
};

export default Explorer;
