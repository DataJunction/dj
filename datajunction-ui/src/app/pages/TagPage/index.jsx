/**
 * For a given tag, displays nodes tagged with it
 */
import NamespaceHeader from '../../components/NamespaceHeader';
import React, { useContext, useEffect, useState } from 'react';
import 'styles/node-creation.scss';
import DJClientContext from '../../providers/djclient';
import { useParams } from 'react-router-dom';

export function TagPage() {
  const [nodes, setNodes] = useState([]);
  const [tag, setTag] = useState([]);

  const { name } = useParams();

  const djClient = useContext(DJClientContext).DataJunctionAPI;
  useEffect(() => {
    const fetchData = async () => {
      const data = await djClient.listNodesForTag(name);
      const tagData = await djClient.getTag(name);
      setNodes(data);
      setTag(tagData);
    };
    fetchData().catch(console.error);
  }, [djClient, name]);

  return (
    <div className="mid">
      <NamespaceHeader namespace="" />
      <div className="card">
        <div className="card-header">
          <h3
            className="card-title align-items-start flex-column"
            style={{ display: 'inline-block' }}
          >
            Tag
          </h3>
          <div>
            <div style={{ marginBottom: '1.5rem' }}>
              <h6 className="mb-0 w-100">Display Name</h6>
              <p className="mb-0 opacity-75">{tag.display_name}</p>
            </div>
            <div style={{ marginBottom: '1.5rem' }}>
              <h6 className="mb-0 w-100">Name</h6>
              <p className="mb-0 opacity-75">{name}</p>
            </div>
            <div style={{ marginBottom: '1.5rem' }}>
              <h6 className="mb-0 w-100">Tag Type</h6>
              <p className="mb-0 opacity-75">{tag.tag_type}</p>
            </div>
            <div style={{ marginBottom: '1.5rem' }}>
              <h6 className="mb-0 w-100">Description</h6>
              <p className="mb-0 opacity-75">{tag.description}</p>
            </div>
            <div style={{ marginBottom: '1.5rem' }}>
              <h6 className="mb-0 w-100">Nodes</h6>
              <div className={`list-group-item`}>
                {nodes?.map(node => (
                  <div
                    className="button-3 cube-element"
                    key={node.name}
                    role="cell"
                    aria-label="CubeElement"
                    aria-hidden="false"
                  >
                    <a href={`/nodes/${node.name}`}>{node.display_name}</a>
                    <span className={`badge node_type__${node.type}`}>
                      {node.type}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
