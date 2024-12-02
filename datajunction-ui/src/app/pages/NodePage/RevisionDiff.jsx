import { useContext, useEffect, useState } from 'react';
import * as React from 'react';
import { diffLines, formatLines } from 'unidiff';
import { parseDiff, Diff, Hunk } from 'react-diff-view';

import { useParams } from 'react-router-dom';
import DJClientContext from '../../providers/djclient';
import NamespaceHeader from '../../components/NamespaceHeader';
import { labelize } from '../../../utils/form';
import DiffIcon from '../../icons/DiffIcon';
import { Light as SyntaxHighlighter } from 'react-syntax-highlighter';
import { foundation } from 'react-syntax-highlighter/src/styles/hljs';
import sql from 'react-syntax-highlighter/dist/cjs/languages/hljs/sql';

SyntaxHighlighter.registerLanguage('sql', sql);
foundation.hljs['padding'] = '2rem';

export default function RevisionDiff() {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [revisions, setRevisions] = useState([]);

  const { name, revision } = useParams();

  useEffect(() => {
    const fetchData = async () => {
      const revisions = await djClient.revisions(name);
      setRevisions(revisions);
    };
    fetchData().catch(console.error);
  }, [djClient, name]);

  const thisRevision = revisions
    .map((rev, idx) => [idx, rev])
    .filter((rev, idx) => {
      return rev[1].version === revision;
    });
  const prevRevision = revisions.filter(
    (rev, idx) => idx + 1 === thisRevision[0][0],
  );

  const EMPTY_HUNKS = [];

  const revisionDiff = (older, newer) => {
    const diffObj = {};
    const fields = [
      'display_name',
      'version',
      'query',
      'mode',
      'status',
      'description',
      'columns',
      // 'catalog',
      'schema',
      'table',
      'updated_at',
    ];
    if (older) {
      for (const key of fields) {
        if (older[key] && (key !== 'columns' || older.type === 'cube')) {
          diffObj[key] = {};
          diffObj[key].diffText = formatLines(
            diffLines(
              older
                ? key === 'columns'
                  ? older[key].map(col => col.name).join('\n')
                  : older[key].toString()
                : '',
              newer
                ? key === 'columns'
                  ? newer[key].map(col => col.name).join('\n')
                  : newer[key].toString()
                : '',
            ),
            {
              context: 5000,
            },
          );
          const [diff] = parseDiff(diffObj[key].diffText, {
            nearbySequences: 'zip',
          });
          diffObj[key].diff = diff;
        }
      }
    }
    return diffObj;
  };

  const diffObjects = revisionDiff(
    prevRevision[0],
    thisRevision[0] ? thisRevision[0][1] : thisRevision[0],
  );

  return (
    <div className="node__header">
      <NamespaceHeader
        namespace={(prevRevision[0] ? prevRevision[0].name : '')
          .split('.')
          .slice(0, -1)
          .join('.')}
      />
      <div className="card">
        <div className="card-header">
          <h3
            className="card-title align-items-start flex-column"
            style={{ display: 'inline-block' }}
          >
            <span
              className="card-label fw-bold text-gray-800"
              role="dialog"
              aria-hidden="false"
              aria-label="DisplayName"
            >
              <a
                href={'/nodes/' + prevRevision[0]?.name}
                className=""
                role="dialog"
                aria-hidden="false"
                aria-label="NodeName"
              >
                {prevRevision[0]?.name}
              </a>{' '}
              <span
                className={
                  'node_type__' + prevRevision[0]?.type + ' badge node_type'
                }
                role="dialog"
                aria-hidden="false"
                aria-label="NodeType"
              >
                {prevRevision[0]?.type}
              </span>
            </span>
          </h3>
          <div>
            <span
              className="rounded-pill badge version"
              style={{ marginLeft: '0.5rem', fontSize: '16px' }}
            >
              {prevRevision[0]?.version}
            </span>
            <DiffIcon />
            <span
              className="rounded-pill badge version"
              style={{ marginLeft: '0.3rem', fontSize: '16px' }}
            >
              {thisRevision[0] ? thisRevision[0][1].version : ''}
            </span>{' '}
          </div>
          {Object.keys(diffObjects).map(field => {
            return (
              <div className="diff" aria-label={'DiffView'} role={'gridcell'}>
                <h4>
                  {labelize(field)}{' '}
                  <small className="no-change-banner">
                    {diffObjects[field]?.diff.hunks.length > 0
                      ? ''
                      : 'no change'}
                  </small>
                </h4>
                {diffObjects[field]?.diff.hunks.length > 0 ? (
                  <Diff
                    viewType="split"
                    diffType=""
                    hunks={diffObjects[field]?.diff.hunks || EMPTY_HUNKS}
                    tokens={diffObjects[field]?.tokens}
                  >
                    {hunks =>
                      hunks.map(hunk => <Hunk key={hunk.content} hunk={hunk} />)
                    }
                  </Diff>
                ) : (
                  <div className="no-change">
                    {prevRevision[0] ? (
                      field === 'query' ? (
                        <>
                          <SyntaxHighlighter
                            language="sql"
                            style={foundation}
                            wrapLongLines={true}
                          >
                            {prevRevision[0].query}
                          </SyntaxHighlighter>
                        </>
                      ) : field === 'columns' ? (
                        <div>{prevRevision[0][field].map(col => <>{col.name}<br /></>)}</div>
                      ) : (
                        prevRevision[0][field].toString()
                      )
                    ) : (
                      ''
                    )}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
