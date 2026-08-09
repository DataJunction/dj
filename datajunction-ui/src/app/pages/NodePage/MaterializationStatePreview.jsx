/**
 * Dev-only preview for the materialization state panel.
 *
 * The repo's fetch mocks are vitest-only (`src/mocks/fetchMock.ts` shims
 * `__vitestFetchMock` from `setupTests.ts`), so there is no dev-server mock layer to
 * render this against. This route feeds the panel straight from fixtures instead,
 * bypassing the API entirely -- which also means it renders the `execution` fields
 * that do not exist yet, since they need `MaterializationInfo` extended and a query
 * service implementation.
 *
 * Throwaway. The panel it renders is not.
 */
import { useState } from 'react';
import MaterializationStatePanel from './MaterializationStatePanel';
import {
  materializationStateStale,
  materializationStateHealthy,
  materializationStateEngineUnknown,
  materializationStateCoverageUnknown,
  materializationStateYearOfDays,
  materializationStateHourly,
} from '../../../mocks/materializationState';

const SCENARIOS = [
  {
    key: 'stale',
    label: 'Stale (last run failed)',
    state: materializationStateStale,
    note: 'The real screenshot case: two materializations, one failing, coverage two days short.',
  },
  {
    key: 'healthy',
    label: 'Healthy',
    state: materializationStateHealthy,
    note: 'Coverage meets target and the last run succeeded.',
  },
  {
    key: 'engine-unknown',
    label: 'Query service unreachable',
    state: materializationStateEngineUnknown,
    note: 'Decides whether this view is worth building before the shim work lands: the semantic half must still render in full.',
  },
  {
    key: 'coverage-unknown',
    label: 'No watermarks reported',
    state: materializationStateCoverageUnknown,
    note: 'Coverage cannot be judged. Same blind spot that makes the freshness gate pass unwatermarked pre-aggs.',
  },
  {
    key: 'year',
    label: 'A year of daily partitions',
    state: materializationStateYearOfDays,
    note: "Layout C's coverage strip past the point where one square per partition works: 371 days bucket to 53 weekly squares, and the two behind days colour their whole week.",
  },
  {
    key: 'hourly',
    label: 'Hourly partitions',
    state: materializationStateHourly,
    note: 'A month of hourly partitions: 721 buckets to 31 daily squares, and the one behind hour still shows.',
  },
];

export default function MaterializationStatePreview() {
  const [selected, setSelected] = useState(SCENARIOS[0].key);
  const scenario = SCENARIOS.find(s => s.key === selected);

  return (
    <div className="card" style={{ margin: '20px', padding: '20px' }}>
      <h4>Materialization state — preview</h4>
      <p className="text-gray-400">Fixture-driven. Not wired to the API.</p>

      <div style={{ display: 'flex', gap: '8px', margin: '12px 0' }}>
        {SCENARIOS.map(s => (
          <button
            key={s.key}
            onClick={() => setSelected(s.key)}
            aria-pressed={selected === s.key}
            className={selected === s.key ? 'nav-link active' : 'nav-link'}
          >
            {s.label}
          </button>
        ))}
      </div>

      <p className="text-gray-400">{scenario.note}</p>

      <MaterializationStatePanel state={scenario.state} />
    </div>
  );
}
