import {Component} from "react";

export default class NamespaceHeader extends Component {
    render() {
      const { namespace } = this.props;
      const namespaceParts = namespace.split(".");
      const namespaceList = namespaceParts.map((piece, index) =>
        <li className="breadcrumb-item">
          <a className="link-body-emphasis"
             href={"/namespaces/" + namespaceParts.slice(0, index + 1).join(".")}>{piece}</a>
        </li>
      );
      return (
      <ol className="breadcrumb breadcrumb-chevron p-3 bg-body-tertiary rounded-3">
        <li className="breadcrumb-item">
            <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor"
                 className="bi bi-house-door-fill" viewBox="0 0 16 16">
              <path
                d="M6.5 14.5v-3.505c0-.245.25-.495.5-.495h2c.25 0 .5.25.5.5v3.5a.5.5 0 0 0 .5.5h4a.5.5 0 0 0 .5-.5v-7a.5.5 0 0 0-.146-.354L13 5.793V2.5a.5.5 0 0 0-.5-.5h-1a.5.5 0 0 0-.5.5v1.293L8.354 1.146a.5.5 0 0 0-.708 0l-6 6A.5.5 0 0 0 1.5 7.5v7a.5.5 0 0 0 .5.5h4a.5.5 0 0 0 .5-.5Z"/>
            </svg>
        </li>
        { namespaceList }
      </ol>
      );
    }
}