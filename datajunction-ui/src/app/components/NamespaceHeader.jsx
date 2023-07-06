import { Component } from 'react';
import HorizontalHierarchyIcon from '../icons/HorizontalHierarchyIcon';

export default class NamespaceHeader extends Component {
  render() {
    const { namespace } = this.props;
    const namespaceParts = namespace.split('.');
    const namespaceList = namespaceParts.map((piece, index) => {
      return (
        <li className="breadcrumb-item" key={index}>
          <a
            className="link-body-emphasis"
            href={'/namespaces/' + namespaceParts.slice(0, index + 1).join('.')}
          >
            {piece}
          </a>
        </li>
      );
    });
    return (
      <ol className="breadcrumb breadcrumb-chevron p-3 bg-body-tertiary rounded-3">
        <li className="breadcrumb-item">
          <a href="/">
            <HorizontalHierarchyIcon />
          </a>
        </li>
        {namespaceList}
      </ol>
    );
  }
}
