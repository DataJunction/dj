import DJClientContext from '../../providers/djclient';
import { Light as SyntaxHighlighter } from 'react-syntax-highlighter';
import { useEffect, useRef, useState, useContext } from 'react';
import { nightOwl } from 'react-syntax-highlighter/dist/esm/styles/hljs';
import PythonIcon from '../../icons/PythonIcon';
import LoadingIcon from 'app/icons/LoadingIcon';

export default function ClientCodePopover({ nodeName, buttonStyle }) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [showModal, setShowModal] = useState(false);
  const modalRef = useRef(null);
  const [code, setCode] = useState(null);

  useEffect(() => {
    async function fetchCode() {
      try {
        const code = await djClient.clientCode(nodeName);
        setCode(code);
      } catch (err) {
        console.log(err);
      }
    }
    fetchCode();
  }, [nodeName, djClient]);

  useEffect(() => {
    const handleClickOutside = event => {
      if (modalRef.current && !modalRef.current.contains(event.target)) {
        setShowModal(false);
      }
    };

    if (showModal) {
      document.addEventListener('mousedown', handleClickOutside);
    } else {
      document.removeEventListener('mousedown', handleClickOutside);
    }

    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
    };
  }, [showModal]);

  return (
    <>
      <button onClick={() => setShowModal(true)} style={buttonStyle}>
        <PythonIcon /> Python
      </button>

      {showModal && (
        <div
          className="modal-backdrop fade in"
          style={{
            position: 'fixed',
            top: 0,
            left: 0,
            width: '100vw',
            height: '100vh',
            backgroundColor: 'rgba(0, 0, 0, 0.5)',
            zIndex: 9999,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
          }}
        >
          <div
            className="centerPopover"
            ref={modalRef}
            style={{
              position: 'relative',
              maxWidth: '80%',
              width: '600px',
              maxHeight: '80vh',
              overflowY: 'auto',
              padding: '1.5rem',
              background: '#fff',
              borderRadius: '10px',
              boxShadow: '0 3px 10px rgba(0, 0, 0, 0.3)',
            }}
          >
            <button
              onClick={() => setShowModal(false)}
              style={{
                position: 'absolute',
                top: '1rem',
                right: '1rem',
                background: 'none',
                border: 'none',
                fontSize: '1.5rem',
                cursor: 'pointer',
                color: '#999',
              }}
              aria-label="Close modal"
            >
              ×
            </button>
            <h2>Python Client Code</h2>
            {code ? (
              <SyntaxHighlighter language="python" style={nightOwl}>
                {code}
              </SyntaxHighlighter>
            ) : (
              <>
                <LoadingIcon />
              </>
            )}
          </div>
        </div>
      )}
    </>
  );
}
