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
    return str.length > 100 ? str.substring(0, 90) + '...' : str;
  };
  useEffect(() => {
    const fetchNodes = async () => {
      const data = await djClient.nodeDetails();
      const fuse = new Fuse(data, {
        keys: [
          'name', // will be assigned a `weight` of 1
          {
            name: 'description',
            weight: 2,
          },
          {
            name: 'display_name',
            weight: 3,
          },
          {
            name: 'type',
            weight: 4,
          },
        ],
      });
      setFuse(fuse);
    };
    fetchNodes();
  }, []);

  const handleChange = e => {
    setSearchValue(e.target.value);
    if (fuse) {
      setSearchResults(fuse.search(e.target.value).map(result => result.item));
    }
    console.log(searchValue);
    console.log(searchResults);
    console.log(fuse);
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
          return (
            <a href={`/nodes/${item.name}`}>
              <div id={item} className="search-result-item">
                <span class={`node_type__${item.type} badge node_type`}>
                  {item.type}
                </span>
                {item.display_name} (<b>{item.name}</b>) -{' '}
                {truncate(item.description)}
              </div>
            </a>
          );
        })}
      </div>
    </div>
  );
}
