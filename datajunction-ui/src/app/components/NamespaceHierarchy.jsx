import AddNamespacePopover from '../pages/NamespacePage/AddNamespacePopover';
import DJClientContext from '../providers/djclient';
import Explorer from '../pages/NamespacePage/Explorer';
import { useContext, useEffect, useState } from 'react';

export default function NamespaceHierarchy({ nodes }) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [namespaces, setNamespaces] = useState([]);
  const [namespaceHierarchy, setNamespaceHierarchy] = useState([]);

  const createNamespaceHierarchy = namespaceList => {
    const hierarchy = [];

    for (const item of namespaceList) {
      const namespaces = item.namespace.split('.');
      let currentLevel = hierarchy;

      let path = '';
      for (const ns of namespaces) {
        path += ns;

        let existingNamespace = currentLevel.find(el => el.namespace === ns);
        if (!existingNamespace) {
          existingNamespace = {
            namespace: ns,
            children: [],
            path: path,
          };
          currentLevel.push(existingNamespace);
        }

        currentLevel = existingNamespace.children;
        path += '.';
      }
    }
    return hierarchy;
  };
  
  useEffect(() => {
    const fetchData = async () => {
      const namespacesFromNodes = Array.from(new Set(nodes.map(node => {
          return {namespace: node.split('.').slice(0, -1).join('.')};
      })));
      const namespaces = nodes ? namespacesFromNodes : await djClient.namespaces();
      setNamespaces(namespaces);

      const hierarchy = createNamespaceHierarchy(namespaces);
      setNamespaceHierarchy(hierarchy);

    };
    fetchData().catch(console.error);
  }, [djClient, djClient.namespaces]);

  return (
    <>
    <span
      style={{
        textTransform: 'uppercase',
        fontSize: '0.8125rem',
        fontWeight: '600',
        color: '#95aac9',
        padding: '1rem 1rem 1rem 0',
      }}
    >
      Namespaces <AddNamespacePopover />
    </span>
    {namespaceHierarchy
      ? namespaceHierarchy.map(child => (
          <Explorer
            item={child}
            // current={state.namespace}
            defaultExpand={false}
          />
        ))
      : null}
    </>
  );
}