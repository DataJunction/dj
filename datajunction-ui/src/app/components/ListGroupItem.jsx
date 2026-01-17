import { Component } from 'react';
import Markdown from 'react-markdown';

export default class ListGroupItem extends Component {
  render() {
    const { label, value } = this.props;
    return (
      <div className="list-group-item d-flex">
        <div className="d-flex gap-2 w-100 justify-content-between py-3">
          <div>
            <h6 className="mb-0 w-100">{label}</h6>
            <div
              className="mb-0 opacity-75"
              role="dialog"
              aria-hidden="false"
              aria-label={label}
            >
              <Markdown>{value}</Markdown>
            </div>
          </div>
        </div>
      </div>
    );
  }
}
