import React from 'react';
import {MarkerType} from "reactflow";

const DJ_URL = process.env.REACT_APP_DJ_URL;

export const DataJunctionAPI = {
    dag: async function (namespace = "default") {
      const edges = [];
      const data = await (
        await fetch(
          DJ_URL + "/nodes/"
        )
      ).json();
      data.forEach(
        (obj) => {
          obj.parents.forEach((parent) => {
            if (parent.name) {
              edges.push({
                id: obj.name + "-" + parent.name,
                target: obj.name,
                source: parent.name,
                markerEnd: {
                  type: MarkerType.Arrow,
                },
              });
            }
          });

          obj.columns.forEach((col) => {
            if (col.dimension) {
              edges.push({
                id: obj.name + "-" + col.dimension.name,
                target: obj.name,
                source: col.dimension.name,
                animated: true,
                draggable: true,
              });
            }
          });
        }
      );

      const nodes = data.map((node, index) => {
        return {
            id: String(node.name),
            type: "DJNode",
            data: {label: node.display_name, type: node.type},
        }
      });
      return {edges: edges, nodes: nodes};
    }
};
