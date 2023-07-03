import React, { Fragment, useEffect, useState } from 'react';

const Explorer = ({
  parent = [],
  onParentClick,
  onNameClick,
  expandOnHover = false,
  defaultExpand = true,
}) => {
  const [newData, setNewData] = useState([]);
  const [expand, setExpand] = useState(false);

  useEffect(() => {
    setNewData(parent);
    setExpand(defaultExpand);
  }, [parent, defaultExpand]);

  const handleClickOnParent = (e, data) => {
    e.stopPropagation();
    if (onParentClick && onParentClick instanceof Function) {
      onParentClick(data);
    }
    setExpand(prev => !prev);
  };

  const handleClickOnName = (e, data) => {
    e.stopPropagation();
    if (onNameClick && onNameClick instanceof Function) {
      onNameClick(data);
    }
  };

  return (
    <>
      {newData.map((item, index) => (
        <Fragment key={index}>
          {item.children && item.children.length ? (
            <Fragment key={index}>
              <div
                className="select-name"
                onMouseOver={() => (expandOnHover ? setExpand(true) : {})}
                onClick={e => handleClickOnParent(e, item)}
              >
                <span>
                  {!expand ? (
                    <svg
                      stroke="currentColor"
                      fill="currentColor"
                      stroke-width="0"
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
                      stroke-width="0"
                      viewBox="0 0 512 512"
                      height="1em"
                      width="1em"
                      xmlns="http://www.w3.org/2000/svg"
                    >
                      <path d="M48 256c0 114.9 93.1 208 208 208s208-93.1 208-208S370.9 48 256 48 48 141.1 48 256zm289.1-43.4c7.5-7.5 19.8-7.5 27.3 0 3.8 3.8 5.6 8.7 5.6 13.6s-1.9 9.9-5.7 13.7l-94.3 94c-7.6 6.9-19.3 6.7-26.6-.6l-95.7-95.4c-7.5-7.5-7.6-19.7 0-27.3 7.5-7.5 19.7-7.6 27.3 0l81.1 81.9 81-79.9z"></path>
                    </svg>
                  )}{' '}
                </span>
                <a href={`/namespaces/${item.path}`}>{item.namespace}</a>{' '}
              </div>

              {expand ? (
                <div
                  style={{
                    paddingLeft: '2.4rem',
                  }}
                >
                  <Explorer
                    parent={item.children}
                    onParentClick={onParentClick}
                    onNameClick={onNameClick}
                    expandOnHover={expandOnHover}
                  />
                </div>
              ) : null}
            </Fragment>
          ) : (
            <div
              className="select-name"
              onClick={e => handleClickOnName(e, item)}
            >
              <a href={`/namespaces/${item.path}`}>{item.namespace}</a>{' '}
            </div>
          )}
        </Fragment>
      ))}
    </>
  );
};

export default Explorer;
