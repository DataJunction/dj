import { Light as SyntaxHighlighter } from 'react-syntax-highlighter';
import { solarizedDark } from 'react-syntax-highlighter/src/styles/hljs';
import React from 'react';

export default function QueryInfo({
  id,
  state,
  engine_name,
  engine_version,
  errors,
  links,
  output_table,
  scheduled,
  started,
  finished,
  numRows,
  isList = false,
}) {
  return isList === false ? (
    <div className="table-responsive">
      <table className="card-inner-table table">
        <thead className="fs-7 fw-bold text-gray-400 border-bottom-0">
          <tr>
            <th>Query ID</th>
            <th>Engine</th>
            <th>State</th>
            <th>Scheduled</th>
            <th>Started</th>
            <th>Errors</th>
            <th>Links</th>
            <th>Output Table</th>
            <th>Number of Rows</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>
              <span className="rounded-pill badge bg-secondary-soft">{id}</span>
            </td>
            <td>
              <span className="rounded-pill badge bg-secondary-soft">
                {engine_name}
                {' - '}
                {engine_version}
              </span>
            </td>
            <td>{state}</td>
            <td>{scheduled}</td>
            <td>{started}</td>
            <td>
              {errors?.length ? (
                errors.map((e, idx) => (
                  <p key={`error-${idx}`}>
                    <span className="rounded-pill badge bg-secondary-error">
                      {e}
                    </span>
                  </p>
                ))
              ) : (
                <></>
              )}
            </td>
            <td>
              {links?.length ? (
                links.map((link, idx) => (
                  <p key={idx}>
                    <a href={link} target="_blank" rel="noreferrer">
                      {link}
                    </a>
                  </p>
                ))
              ) : (
                <></>
              )}
            </td>
            <td>{output_table}</td>
            <td>{numRows}</td>
          </tr>
        </tbody>
      </table>
    </div>
  ) : (
    <div className="rightbottom">
      <ul style={{ padding: '20px' }}>
        <li className={'query-info'}>
          <label>Query ID</label>{' '}
          <span className="tag_value rounded-pill badge">
            {links?.length ? (
              <a
                href={links[links.length - 1]}
                target={'_blank'}
                rel="noreferrer"
              >
                {id}
              </a>
            ) : (
              id
            )}
          </span>
        </li>
        <li className={'query-info'}>
          <label>State</label>
          <span className="tag_value rounded-pill badge">{state}</span>
        </li>
        <li className={'query-info'}>
          <label>Engine</label>{' '}
          <span className="tag_value rounded-pill badge">
            {engine_name}
            {' - '}
            {engine_version}
          </span>
        </li>
        <li className={'query-info'}>
          <label>Scheduled</label> {scheduled}
        </li>
        <li className={'query-info'}>
          <label>Started</label> {started}
        </li>
        <li className={'query-info'}>
          <label>Finished</label> {finished}
        </li>
        <li className={'query-info'}>
          <label>Logs</label>{' '}
          {errors?.length ? (
            errors.map((error, idx) => (
              <div
                key={`error-${idx}`}
                style={{
                  height: '800px',
                  width: '80%',
                  overflow: 'scroll',
                  borderRadius: '0',
                  border: '1px solid #ccc',
                }}
                className="queryrunner-query"
              >
                <SyntaxHighlighter
                  language="javascript"
                  style={solarizedDark}
                  wrapLines={true}
                >
                  {error}
                </SyntaxHighlighter>
              </div>
            ))
          ) : (
            <></>
          )}
        </li>
        <li className={'query-info'}>
          <label>Links:</label>{' '}
          {links?.length ? (
            links.map((link, idx) => (
              <p key={idx}>
                <a href={link} target="_blank" rel="noreferrer">
                  {link}
                </a>
              </p>
            ))
          ) : (
            <></>
          )}
        </li>
        <li className={'query-info'}>
          <label>Output Table:</label> {output_table}
        </li>
        <li className={'query-info'}>
          <label>Rows:</label> {numRows}
        </li>
      </ul>
    </div>
  );
}
