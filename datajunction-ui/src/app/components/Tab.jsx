import { Component } from 'react';

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
        {this.props.name}
      </button>
    );
  }
}
