import React, { Fragment, useEffect, useState } from 'react';

const Explorer = ({ parent = [], current }) => {
  const [newData, setNewData] = useState([]);
  const [expand, setExpand] = useState(false);
  const [highlight, setHighlight] = useState(false);

  useEffect(() => {
    setNewData(parent);
    setHighlight(current);
    if (current.startsWith(parent.path)) setExpand(true);
    else setExpand(false);
  }, [current, parent]);

  const handleClickOnParent = e => {
    e.stopPropagation();
    setExpand(prev => {
      return !prev;
    });
  };

  return (
    <>
      <div
        className={`select-name ${
          highlight === newData.path ? 'select-name-highlight' : ''
        }`}
        onClick={handleClickOnParent}
      >
        {newData.children && newData.children.length > 0 ? (
          <span>
            {!expand ? (
              <svg
                stroke="currentColor"
                fill="currentColor"
                strokeWidth="0"
                viewBox="0 0 512 512"
                height="1em"
                width="1em"
                xmlns="http://www.w3.org/2000/svg"
              >
                <path d="M48 256c0 114.9 93.1 208 208 208s208-93.1 208-208S370.9 48 256 48 48 141.1 48 256zm244.5 0l-81.9-81.1c-7.5-7.5-7.5-19.8 0-27.3s19.8-7.5 27.3 0l95.4 95.7c7.3 7.3 7.5 19.1.6 26.6l-94 94.3c-3.8 3.8-8.7 5.7-13.7 5.7-4.9 0-9.9-1.9-13.6-5.6-7.5-7.5-7.6-19.7 0-27.3l79.9-81z"></path>
              </svg>
            ) : (
              <svg
                stroke="currentColor"
                fill="currentColor"
                strokeWidth="0"
                viewBox="0 0 512 512"
                height="1em"
                width="1em"
                xmlns="http://www.w3.org/2000/svg"
              >
                <path d="M48 256c0 114.9 93.1 208 208 208s208-93.1 208-208S370.9 48 256 48 48 141.1 48 256zm289.1-43.4c7.5-7.5 19.8-7.5 27.3 0 3.8 3.8 5.6 8.7 5.6 13.6s-1.9 9.9-5.7 13.7l-94.3 94c-7.6 6.9-19.3 6.7-26.6-.6l-95.7-95.4c-7.5-7.5-7.6-19.7 0-27.3 7.5-7.5 19.7-7.6 27.3 0l81.1 81.9 81-79.9z"></path>
              </svg>
            )}{' '}
          </span>
        ) : null}
        <a href={`/namespaces/${newData.path}`}>{newData.namespace}</a>{' '}
      </div>
      {newData.children
        ? newData.children.map((item, index) => (
            <div
              style={{
                paddingLeft: '1.4rem',
                marginLeft: '1rem',
                borderLeft: '1px solid rgb(218 233 255)',
              }}
            >
              <div className={`${expand ? '' : 'inactive'}`}>
                <Explorer parent={item} current={highlight} />
              </div>
            </div>
          ))
        : null}
    </>
  );
};

export default Explorer;
