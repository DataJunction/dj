import { Component } from 'react';
import ValidIcon from '../../icons/ValidIcon';
import InvalidIcon from '../../icons/InvalidIcon';

export default class NodeStatus extends Component {
  render() {
    const { node } = this.props;
    return (
      <>
        {node?.status === 'valid' ? (
          <span
            className="status__valid status"
            style={{ alignContent: 'center' }}
          >
            <ValidIcon />
          </span>
        ) : (
          <span
            className="status__invalid status"
            style={{ alignContent: 'center' }}
          >
            <InvalidIcon />
          </span>
        )}
      </>
    );
  }
}
