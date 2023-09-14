import { Light as SyntaxHighlighter } from 'react-syntax-highlighter';
import { useState } from 'react';
import { nightOwl } from 'react-syntax-highlighter/src/styles/hljs';
import PythonIcon from '../../icons/PythonIcon';

export default function ClientCodePopover({ code }) {
  const [codeAnchor, setCodeAnchor] = useState(false);

  return (
    <>
      <button
        className="code-button"
        aria-label="code-button"
        tabIndex="0"
        height="45px"
        onClick={() => setCodeAnchor(!codeAnchor)}
      >
        <PythonIcon />
      </button>
      <div
        id={`node-create-code`}
        role="dialog"
        aria-label="client-code"
        style={{ display: codeAnchor === false ? 'none' : 'block' }}
      >
        <SyntaxHighlighter language="python" style={nightOwl}>
          {code}
        </SyntaxHighlighter>
      </div>
    </>
  );
}
