import TableIcon from '../../icons/TableIcon';

export default function AvailabilityStateBlock({ availability }) {
  return (
    <table
      className="card-inner-table table"
      aria-label="Availability"
      aria-hidden="false"
      style={{ marginBottom: '20px' }}
    >
      <thead className="fs-7 fw-bold text-gray-400 border-bottom-0">
        <tr>
          <th className="text-start">Output Dataset</th>
          <th>Valid Through</th>
          <th>Partitions</th>
          <th>Links</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>
            <div className={`table__full`} key={availability.table}>
              <div className="table__header">
                <TableIcon />{' '}
                <span className={`entity-info`}>
                  {availability.catalog + '.' + availability.schema_}
                </span>
              </div>
              <div className={`table__body upstream_tables`}>
                <a href={availability.url}>{availability.table}</a>
              </div>
            </div>
          </td>
          <td>{new Date(availability.valid_through_ts).toISOString()}</td>
          <td>
            <span
              className={`badge partition_value`}
              style={{ fontSize: '100%' }}
            >
              <span className={`badge partition_value_highlight`}>
                {availability.min_temporal_partition?.join(', ') || 'N/A'}
              </span>
              to
              <span className={`badge partition_value_highlight`}>
                {availability.max_temporal_partition?.join(', ') || 'N/A'}
              </span>
            </span>
          </td>
          <td>
            {availability.links &&
            Object.keys(availability.links).length > 0 ? (
              Object.entries(availability.links).map(([key, value]) => (
                <div key={key}>
                  <a href={value} target="_blank" rel="noreferrer">
                    {key}
                  </a>
                </div>
              ))
            ) : (
              <></>
            )}
          </td>
        </tr>
      </tbody>
    </table>
  );
}
