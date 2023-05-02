import {Component} from "react";
import {Light as SyntaxHighlighter} from "react-syntax-highlighter";
import {foundation} from "react-syntax-highlighter/src/styles/hljs";
import sql from 'react-syntax-highlighter/dist/esm/languages/hljs/sql';
import { format } from 'sql-formatter';

import NodeStatus from "./NodeStatus";

SyntaxHighlighter.registerLanguage('sql', sql);
foundation.hljs["padding"] = "2rem";

export default class NodeInfoTab extends Component {

  nodeTags = this.props.node?.tags.map(
    (tag) => <div>{ tag }</div>
  )

  render() {
    return(
      <div className="list-group align-items-center justify-content-between flex-md-row gap-2">
        <div className="list-group-item d-flex">
          <div className="d-flex gap-2 w-100 justify-content-between py-3">
            <div>
              <h6 className="mb-0 w-100">Description</h6>
              <p className="mb-0 opacity-75">{ this.props.node?.description }</p>
            </div>
          </div>
        </div>


        <div className="list-group-item d-flex">
          <div className="d-flex gap-2 w-100 justify-content-between py-3">
            <div>
              <h6 className="mb-0 w-100">Version</h6>

              <p className="mb-0 opacity-75">
                <span className="rounded-pill badge bg-secondary-soft" style={{marginLeft: "0.5rem", fontSize: "100%"}}>
                  { this.props.node?.version }
                </span>
              </p>
            </div>
            <div>
              <h6 className="mb-0 w-100">Status</h6>
              <p className="mb-0 opacity-75">
                <NodeStatus node={ this.props.node } />
              </p>
            </div>
            <div>
              <h6 className="mb-0 w-100">Mode</h6>
              <p className="mb-0 opacity-75">
                <span className="status">{this.props.node?.mode}</span>
              </p>
            </div>
            <div>
              <h6 className="mb-0 w-100">Tags</h6>
              <p className="mb-0 opacity-75">{ this.nodeTags }</p>
            </div>
          </div>
        </div>

        <div className="list-group-item d-flex">
          <div className="d-flex gap-2 w-100 justify-content-between py-3">
            <div style={{width: "100%"}}>
              <h6 className="mb-0 w-100">Query</h6>
              <SyntaxHighlighter language="sql" style={foundation}>
              { format(
                this.props.node?.query,
                {
                  language: 'spark',
                  tabWidth: 2,
                  keywordCase: 'upper',
                }) }
              </SyntaxHighlighter>
            </div>
          </div>
        </div>
        <div className="list-group-item d-flex">{ this.props.node?.primary_key }</div>
      </div>
    );
  }
}
