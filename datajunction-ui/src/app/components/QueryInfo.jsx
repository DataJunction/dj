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
  numRows,
}) {
  const stateIcon =
    state === 'FINISHED' ? (
      <span className="status__valid status" style={{ alignContent: 'center' }}>
        <svg
          xmlns="http://www.w3.org/2000/svg"
          width="25"
          height="25"
          fill="currentColor"
          className="bi bi-check-circle-fill"
          viewBox="0 0 16 16"
        >
          <path d="M16 8A8 8 0 1 1 0 8a8 8 0 0 1 16 0zm-3.97-3.03a.75.75 0 0 0-1.08.022L7.477 9.417 5.384 7.323a.75.75 0 0 0-1.06 1.06L6.97 11.03a.75.75 0 0 0 1.079-.02l3.992-4.99a.75.75 0 0 0-.01-1.05z" />
        </svg>
      </span>
    ) : (
      <span
        className="status__invalid status"
        style={{ alignContent: 'center' }}
      >
        <svg
          xmlns="http://www.w3.org/2000/svg"
          width="16"
          height="16"
          fill="currentColor"
          className="bi bi-x-circle-fill"
          viewBox="0 0 16 16"
        >
          <path d="M16 8A8 8 0 1 1 0 8a8 8 0 0 1 16 0zM5.354 4.646a.5.5 0 1 0-.708.708L7.293 8l-2.647 2.646a.5.5 0 0 0 .708.708L8 8.707l2.646 2.647a.5.5 0 0 0 .708-.708L8.707 8l2.647-2.646a.5.5 0 0 0-.708-.708L8 7.293 5.354 4.646z" />
        </svg>
      </span>
    );

  return (
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
            <td>{stateIcon}</td>
            <td>{scheduled}</td>
            <td>{started}</td>
            <td>
              {errors.length ? (
                errors.map(e => (
                  <p>
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
                links.map(link => (
                  <p>
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
  );
}
