import { useCallback, useContext, useEffect, useRef, useState } from 'react';
import DJClientContext from '../providers/djclient';

import './search.css';

const DEBOUNCE_MS = 120;
const MIN_QUERY_LENGTH = 1;
const CACHE_MAX_ENTRIES = 50;
const CACHE_TTL_MS = 60 * 1000;

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
  // LRU cache keyed on normalized query; Map preserves insertion order so the
  // oldest entry is always the first key.
  const cacheRef = useRef(new Map());

  const djClient = useContext(DJClientContext).DataJunctionAPI;

  const readCache = useCallback(key => {
    const entry = cacheRef.current.get(key);
    if (!entry) return null;
    if (Date.now() - entry.at > CACHE_TTL_MS) {
      cacheRef.current.delete(key);
      return null;
    }
    // Touch for LRU ordering.
    cacheRef.current.delete(key);
    cacheRef.current.set(key, entry);
    return entry.value;
  }, []);

  const writeCache = useCallback((key, value) => {
    const cache = cacheRef.current;
    cache.set(key, { value, at: Date.now() });
    while (cache.size > CACHE_MAX_ENTRIES) {
      const oldest = cache.keys().next().value;
      cache.delete(oldest);
    }
  }, []);

  const runSearch = useCallback(
    async query => {
      const cached = readCache(query);
      if (cached) {
        if (abortRef.current) abortRef.current.abort();
        setNodes(cached.nodes);
        setTags(cached.tags);
        setIsLoading(false);
        return;
      }
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
          writeCache(query, results);
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
    [djClient, readCache, writeCache],
  );

  const handleChange = e => {
    const value = e.target.value;
    setSearchValue(value);
    if (debounceRef.current) {
      clearTimeout(debounceRef.current);
    }
    const trimmed = value.trim();
    if (trimmed.length < MIN_QUERY_LENGTH) {
      if (abortRef.current) abortRef.current.abort();
      setNodes([]);
      setTags([]);
      setIsLoading(false);
      return;
    }
    // Synchronous cache hit: render instantly without waiting for debounce.
    const cached = readCache(trimmed);
    if (cached) {
      if (abortRef.current) abortRef.current.abort();
      setNodes(cached.nodes);
      setTags(cached.tags);
      setIsLoading(false);
      return;
    }
    debounceRef.current = setTimeout(() => runSearch(trimmed), DEBOUNCE_MS);
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
        <div
          className="search-results"
          style={isLoading ? { opacity: 0.6 } : undefined}
        >
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
