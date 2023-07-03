import * as React from 'react';
import { useContext, useEffect, useState } from 'react';
import NamespaceHeader from '../../components/NamespaceHeader';
import { DataJunctionAPI } from '../../services/DJService';
import DJClientContext from '../../providers/djclient';
import Explorer from './Explorer';

export function ListNamespacesPage() {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [namespaces, setNamespaces] = useState({});

  useEffect(() => {
    const fetchData = async () => {
      const namespaces = await djClient.namespaces();
      var hierarchy = { namespace: 'r', children: [], path: '' };
      namespaces.forEach(namespace => {
        const parts = namespace.namespace.split('.');
        let current = hierarchy;
        parts.forEach(part => {
          const found = current.children.find(
            child => part === child.namespace,
          );
          if (found !== undefined) current = found;
          else
            current.children.push({
              namespace: part,
              children: [],
              path: current.path === '' ? part : current.path + '.' + part,
            });
        });
      });
      setNamespaces(hierarchy);
    };
    fetchData().catch(console.error);
  }, [djClient, djClient.namespaces]);

  return (
    <>
      <div className="mid">
        <NamespaceHeader namespace="" />
        <div className="card">
          <div className="card-header">
            <h2>Namespaces</h2>
            <Explorer parent={namespaces.children} />
          </div>
        </div>
      </div>
    </>
  );
}

ListNamespacesPage.defaultProps = {
  djClient: DataJunctionAPI,
};
