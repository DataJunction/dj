import * as React from 'react';
import { Field } from 'formik';

export default function PartitionValueForm({ col, columnIdentifier }) {
  if (col.partition.type_ === 'temporal') {
    return (
      <>
        <div className="partition__full" style={{ width: '50%' }}>
          <div className="partition__header">{col.display_name}</div>
          <div className="partition__body">
            <span style={{ padding: '0.5rem' }}>From</span>{' '}
            <Field
              type="text"
              name={`partitionValues.['${columnIdentifier}'].from`}
              id={`${columnIdentifier}.from`}
              placeholder="20230101"
              default="20230101"
              style={{ width: '7rem', paddingRight: '1rem' }}
            />{' '}
            <span style={{ padding: '0.5rem' }}>To</span>
            <Field
              type="text"
              name={`partitionValues.['${columnIdentifier}'].to`}
              id={`${columnIdentifier}.to`}
              placeholder="20230102"
              default="20230102"
              style={{ width: '7rem' }}
            />
          </div>
        </div>
      </>
    );
  } else {
    return (
      <>
        <div className="partition__full" style={{ width: '50%' }}>
          <div className="partition__header">{col.display_name}</div>
          <div className="partition__body">
            <Field
              type="text"
              name={`partitionValues.['${columnIdentifier}']`}
              id={columnIdentifier}
              placeholder=""
              default=""
              style={{ width: '7rem', paddingRight: '1rem' }}
            />
          </div>
        </div>
      </>
    );
  }
}
