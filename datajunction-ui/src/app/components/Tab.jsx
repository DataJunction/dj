import { Component } from 'react';

export default class Tab extends Component {
  render() {
    const { id, onClick, selectedTab } = this.props;
    return (
      <div className={selectedTab === id ? 'col active' : 'col'}>
        <div className="header-tabs nav-overflow nav nav-tabs">
          <div className="nav-item">
            <button
              id={id}
              className="nav-link"
              tabIndex="0"
              onClick={onClick}
              aria-label={this.props.name}
              aria-hidden="false"
            >
              {this.props.name}
            </button>
          </div>
        </div>
      </div>
    );
  }
}
