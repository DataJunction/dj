import { useContext } from 'react';
import { MarkerType } from 'reactflow';

import '../../../styles/dag.css';
import 'reactflow/dist/style.css';
import DJClientContext from '../../providers/djclient';
import LayoutFlow from '../../components/djgraph/LayoutFlow';

const NodeLineage = djNode => {
  const djClient = useContext(DJClientContext).DataJunctionAPI;

  const createNode = node => {
    const primary_key = node.columns
      .filter(col =>
        col.attributes.some(attr => attr.attribute_type.name === 'primary_key'),
      )
      .map(col => col.name);
    const dimensionLinkForeignKeys = node.dimension_links
      .flatMap(link => Object.keys(link.foreign_keys).map(key => key.split('.').slice(-1)));
    const column_names = node.columns
      .map(col => {
        return {
          name: col.name,
          type: col.type,
          dimension: col.dimension !== null ? col.dimension.name : null,
          order: primary_key.includes(col.name)
            ? -1
            : dimensionLinkForeignKeys.includes(col.name)
            ? 0
            : 1,
        };
      })
      .sort((a, b) => a.order - b.order);
    return {
      id: String(node.name),
      type: 'DJNode',
      data: {
        label:
          node.table !== null
            ? String(node.schema_ + '.' + node.table)
            : 'default.' + node.name,
        table: node.table,
        name: String(node.name),
        display_name: String(node.display_name),
        type: node.type,
        primary_key: primary_key,
        column_names: column_names,
        is_current: node.name === djNode.djNode.name,
      },
    };
  };

  const dimensionEdges = node => {
    return node.dimension_links
      .flatMap(link => {
        return Object.keys(link.foreign_keys).map(fk => {
            return {
              id: link.dimension.name + '->' + node.name + '=' + link.foreign_keys[fk] + '->' + fk,
              source: link.dimension.name,
              sourceHandle: link.foreign_keys[fk],
              target: node.name,
              targetHandle: fk,
              draggable: true,
              markerStart: {
                type: MarkerType.Arrow,
                width: 20,
                height: 20,
                color: '#b0b9c2',
              },
              style: {
                strokeWidth: 3,
                stroke: '#b0b9c2',
              },
            };
          }
        )
      });
  };

  const parentEdges = node => {
    return node.parents
      .filter(parent => parent.name)
      .map(parent => {
        return {
          id: node.name + '-' + parent.name,
          source: parent.name,
          sourceHandle: parent.name,
          target: node.name,
          targetHandle: node.name,
          animated: true,
          markerEnd: {
            type: MarkerType.Arrow,
          },
          style: {
            strokeWidth: 3,
            stroke: '#b0b9c2',
          },
        };
      });
  };

  const dagFetch = async (getLayoutedElements, setNodes, setEdges) => {
    let related_nodes = await djClient.node_dag(djNode.djNode.name);
    var djNodes = [djNode.djNode];
    for (const iterable of [related_nodes]) {
      for (const item of iterable) {
        if (item.type !== 'cube') {
          djNodes.push(item);
        }
      }
    }
    let edges = [];
    djNodes.forEach(node => {
      edges = edges.concat(parentEdges(node));
      edges = edges.concat(dimensionEdges(node));
    });
    const nodes = djNodes.map(node => createNode(node));

    // use dagre to determine the position of the parents (the DJ nodes)
    // the positions of the columns are relative to each DJ node
    getLayoutedElements(nodes, edges);
    setNodes(nodes);
    setEdges(edges);
  };
  return LayoutFlow(djNode, dagFetch);
};
export default NodeLineage;
