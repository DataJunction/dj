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
  );
}
