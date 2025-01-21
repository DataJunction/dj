import { useState, useEffect, useContext } from 'react';
import DJClientContext from '../providers/djclient';
import Fuse from 'fuse.js';

import './search.css';

export default function Search() {
  const [fuse, setFuse] = useState();
  const [searchValue, setSearchValue] = useState('');
  const [searchResults, setSearchResults] = useState([]);

  const djClient = useContext(DJClientContext).DataJunctionAPI;

  const truncate = str => {
    if (str === null) {
      return '';
    }
    return str.length > 100 ? str.substring(0, 90) + '...' : str;
  };

  useEffect(() => {
    const fetchNodes = async () => {
      try {
        const [data, tags] = await Promise.all([
          djClient.nodeDetails(),
          djClient.listTags(),
        ]);
        const allEntities = data.concat(
          (tags || []).map(tag => {
            tag.type = 'tag';
            return tag;
          }),
        );
        const fuse = new Fuse(allEntities || [], {
          keys: [
            'name', // will be assigned a `weight` of 1
            { name: 'description', weight: 2 },
            { name: 'display_name', weight: 3 },
            { name: 'type', weight: 4 },
            { name: 'tag_type', weight: 5 },
          ],
        });
        setFuse(fuse);
      } catch (error) {
        console.error('Error fetching nodes or tags:', error);
      }
    };
    fetchNodes();
  }, []);
  
  const handleChange = e => {
    setSearchValue(e.target.value);
    if (fuse) {
      setSearchResults(fuse.search(e.target.value).map(result => result.item));
    }
  };

  return (
    <div>
      <form
        className="search-box"
        onSubmit={e => {
          e.preventDefault();
        }}
      >
        <input
          type="text"
          placeholder="Search"
          name="search"
          value={searchValue}
          onChange={handleChange}
        />
      </form>
      <div className="search-results">
        {searchResults.map(item => {
          const itemUrl =
            item.type !== 'tag' ? `/nodes/${item.name}` : `/tags/${item.name}`;
          return (
            <a href={itemUrl}>
              <div key={item.name} className="search-result-item">
                <span className={`node_type__${item.type} badge node_type`}>
                  {item.type}
                </span>
                {item.display_name} (<b>{item.name}</b>){' '}
                {item.description ? '- ' : ' '}
                {truncate(item.description || '')}
              </div>
            </a>
          );
        })}
      </div>
    </div>
  );
}
