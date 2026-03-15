gimport { Component } from 'react';

export default class Tab extends Component {
  render() {
    const { id, onClick, selectedTab } = this.props;
    const isActive = selectedTab === id;
    return (
      <button
        id={id}
        className={isActive ? 'dj-tab dj-tab--active' : 'dj-tab'}
        tabIndex="0"
        onClick={onClick}
        aria-label={this.props.name}
        aria-hidden="false"
      >
        <span
          style={{ display: 'inline-flex', alignItems: 'center', gap: '4px' }}
        >
          {this.props.icon}
          {this.props.name}
        </span>
      </button>
    );
  }
}
