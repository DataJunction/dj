import * as React from 'react';
import { useEffect, useState } from 'react';
import { DataJunctionAPI } from '../../services/DJService';
import NamespaceHeader from '../../components/NamespaceHeader';

export function ListNamespacesPage() {
  const [state, setState] = useState({
    namespaces: [],
  });

  useEffect(() => {
    const fetchData = async () => {
      const namespaces = await DataJunctionAPI.namespaces();
      setState({
        namespaces: namespaces,
      });
    };
    fetchData().catch(console.error);
  }, []);

  const namespacesList = state.namespaces.map(node => (
    <tr>
      <td>
        <a href={'/namespaces/' + node.namespace}>{node.namespace}</a>
      </td>
      <td>5</td>
    </tr>
  ));

  // @ts-ignore
  return (
    <>
      <div className="mid">
        <NamespaceHeader namespace="" />
        <div className="card">
          <div className="card-header">
            <h2>Namespaces</h2>
            <div className="table-responsive">
              <table className="card-table table">
                <thead>
                  <th>Namespace</th>
                  <th>Node Count</th>
                </thead>
                {namespacesList}
              </table>
            </div>
          </div>
        </div>
      </div>
    </>
  );
}
