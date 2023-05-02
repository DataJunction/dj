import * as React from 'react';
import { useEffect, useState } from 'react';
import NamespaceHeader from '../../components/NamespaceHeader';
const datajunction = require('datajunction');
const dj = new datajunction.DJClient('http://localhost:8000');

export function ListNamespacesPage() {
  const [state, setState] = useState({
    namespaces: [],
  });

  useEffect(() => {
    const fetchData = async () => {
      const namespaces = await dj.namespaces.get();
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
