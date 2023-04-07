import React, { useState, useEffect } from 'react';
import ReactFlow, { isNode } from 'react-flow-renderer';
import axios from 'axios';

const NodeContainer = () => {
  // const [nodes, setNodes] = useState([]);
    const [state, setState] = useState([]);

    useEffect(() => {
        const dataFetch = async () => {
          const data = await (
            await fetch(
              "http://localhost:8000/nodes/"
            )
          ).json();

          // set state when the data received
          await setState(data.map(node => {
                      return {
                          id: node.node_id,
                          type: node.type,
                          data: {label: node.display_name},
                          position: {x: 0, y: node.node_id * 10},
                      };
                  })
          );
          console.log(state);
        };
        dataFetch();
  }, []);
    return {data: state};
  //
  // //
  // // useEffect(() => {
  // //   axios.get('http://localhost:8000/nodes/')
  // //     .then(response => {
  // //         const allNodes = response.data;
  // //         setNodes({nodes: allNodes});
  // //         console.log(nodes);
  // //
  // //         return nodes;
  // //     })
  // //     .catch(error => console.log(error));
  // // }, []);
  //   // useEffect(() => {
  //   //     getDAG();
  //   // }, []);
  //
  // const getDAG = () => {
  //     return nodes.map(node => {
  //             return {
  //                 id: node.node_id,
  //                 type: node.type,
  //                 data: {label: node.display_name},
  //                 position: {x: 0, y: node.node_id * 10},
  //             };
  //         }
  //     );
  // };
};

export default NodeContainer;
