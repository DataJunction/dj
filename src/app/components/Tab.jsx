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
              role="button"
              className="nav-link"
              tabIndex="0"
              onClick={onClick}
            >
              {this.props.name}
              {/*<span className="rounded-pill badge bg-secondary-soft">823</span>*/}
            </button>
          </div>
        </div>
      </div>
    );
  }
}
