import { Component } from 'react';

export default class NodeColumnTab extends Component {
  columnList = node => {
    return node.columns.map(col => (
      <tr>
        <td className="text-start">{col.name}</td>
        <td>
          <span className="node_type__transform badge node_type">
            {col.type}
          </span>
        </td>
        <td>{col.dimension ? col.dimension.name : ''}</td>
        <td>
          {col.attributes.find(
            attr => attr.attribute_type.name === 'dimension',
          ) ? (
            <span className="node_type__dimension badge node_type">
              dimensional
            </span>
          ) : (
            ''
          )}
        </td>
      </tr>
    ));
  };

  render() {
    return (
      <div className="table-responsive">
        <table className="card-inner-table table">
          <thead className="fs-7 fw-bold text-gray-400 border-bottom-0">
            <th className="text-start">Column</th>
            <th>Type</th>
            <th>Dimension</th>
            <th>Attributes</th>
          </thead>
          {this.columnList(this.props.node)}
        </table>
      </div>
    );
  }
}
