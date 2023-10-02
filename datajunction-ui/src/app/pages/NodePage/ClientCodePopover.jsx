import { Light as SyntaxHighlighter } from 'react-syntax-highlighter';
import { useEffect, useRef, useState } from 'react';
import { nightOwl } from 'react-syntax-highlighter/src/styles/hljs';
import PythonIcon from '../../icons/PythonIcon';

export default function ClientCodePopover({ code }) {
  const [codeAnchor, setCodeAnchor] = useState(false);
  const ref = useRef(null);

  useEffect(() => {
    const handleClickOutside = event => {
      if (ref.current && !ref.current.contains(event.target)) {
        setCodeAnchor(false);
      }
    };
    document.addEventListener('click', handleClickOutside, true);
    return () => {
      document.removeEventListener('click', handleClickOutside, true);
    };
  }, [setCodeAnchor]);

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
        ref={ref}
      >
        <SyntaxHighlighter language="python" style={nightOwl}>
          {code}
        </SyntaxHighlighter>
      </div>
    </>
  );
}
