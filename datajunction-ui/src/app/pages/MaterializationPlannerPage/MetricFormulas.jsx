/**
 * MetricFormulas - Shows how metrics are calculated from their components
 */
export function MetricFormulas({ metricFormulas }) {
  if (!metricFormulas?.length) {
    return null;
  }

  return (
    <div className="metric-formulas">
      <h4>Metric Formulas</h4>
      <p className="formulas-description">
        How final metrics are calculated from pre-aggregated components:
      </p>
      <div className="formulas-list">
        {metricFormulas.map((formula, i) => (
          <div key={i} className={`formula-card ${formula.is_derived ? 'formula-derived' : ''}`}>
            <div className="formula-header">
              <span className="formula-name">{formula.short_name}</span>
              {formula.is_derived && (
                <span className="formula-badge derived">Derived</span>
              )}
            </div>
            <div className="formula-expression">
              <code>{formula.combiner}</code>
            </div>
            <div className="formula-components">
              <span className="components-label">Uses:</span>
              {formula.components?.map((comp, j) => (
                <span key={j} className="component-tag">{comp}</span>
              ))}
            </div>
            {formula.parent_name && (
              <div className="formula-source">
                From: <span>{formula.parent_name}</span>
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}

export default MetricFormulas;

