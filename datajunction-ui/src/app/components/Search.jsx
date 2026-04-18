import { useCallback, useContext, useEffect, useRef, useState } from 'react';
import DJClientContext from '../providers/djclient';

import './search.css';

const DEBOUNCE_MS = 200;
const MIN_QUERY_LENGTH = 2;

const truncate = str => {
  if (!str) return '';
  return str.length > 100 ? str.substring(0, 90) + '...' : str;
};

export default function Search() {
  const [searchValue, setSearchValue] = useState('');
  const [nodes, setNodes] = useState([]);
  const [tags, setTags] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  const abortRef = useRef(null);
  const debounceRef = useRef(null);
  const inputRef = useRef(null);

  const djClient = useContext(DJClientContext).DataJunctionAPI;

  const runSearch = useCallback(
    async query => {
      if (abortRef.current) {
        abortRef.current.abort();
      }
      const controller = new AbortController();
      abortRef.current = controller;
      setIsLoading(true);
      try {
        const results = await djClient.globalSearch(query, {
          signal: controller.signal,
        });
        if (!controller.signal.aborted) {
          setNodes(results.nodes);
          setTags(results.tags);
        }
      } catch (err) {
        if (err.name !== 'AbortError') {
          console.error('Search failed:', err);
          setNodes([]);
          setTags([]);
        }
      } finally {
        if (!controller.signal.aborted) {
          setIsLoading(false);
        }
      }
    },
    [djClient],
  );

  const handleChange = e => {
    const value = e.target.value;
    setSearchValue(value);
    if (debounceRef.current) {
      clearTimeout(debounceRef.current);
    }
    if (value.trim().length < MIN_QUERY_LENGTH) {
      if (abortRef.current) abortRef.current.abort();
      setNodes([]);
      setTags([]);
      setIsLoading(false);
      return;
    }
    debounceRef.current = setTimeout(() => runSearch(value.trim()), DEBOUNCE_MS);
  };

  useEffect(() => {
    return () => {
      if (debounceRef.current) clearTimeout(debounceRef.current);
      if (abortRef.current) abortRef.current.abort();
    };
  }, []);

  const hasResults = nodes.length > 0 || tags.length > 0;

  return (
    <div>
      <div className="nav-search-box" onClick={() => inputRef.current?.focus()}>
        <input
          ref={inputRef}
          type="text"
          placeholder={isLoading ? 'Searching...' : 'Search nodes and tags...'}
          name="search"
          value={searchValue}
          onChange={handleChange}
        />
      </div>
      {hasResults && (
        <div className="search-results">
          {nodes.map(item => (
            <a key={`node-${item.name}`} href={`/nodes/${item.name}`}>
              <div className="search-result-item">
                <span className={`node_type__${item.type} badge node_type`}>
                  {item.type}
                </span>
                {item.display_name} (<b>{item.name}</b>){' '}
                {item.description ? '- ' : ' '}
                {truncate(item.description)}
              </div>
            </a>
          ))}
          {tags.map(item => (
            <a key={`tag-${item.name}`} href={`/tags/${item.name}`}>
              <div className="search-result-item">
                <span className="node_type__tag badge node_type">tag</span>
                {item.display_name} (<b>{item.name}</b>){' '}
                {item.description ? '- ' : ' '}
                {truncate(item.description)}
              </div>
            </a>
          ))}
        </div>
      )}
    </div>
  );
}
