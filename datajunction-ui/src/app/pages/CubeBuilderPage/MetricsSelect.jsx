/**
 * A select component for picking metrics.
 * Uses async search to efficiently handle large numbers of metrics.
 * Results are grouped by namespace for easier navigation.
 */
import AsyncSelect from 'react-select/async';
// `components` is exported from the main `react-select` entry, not from
// `react-select/async`. CRA's webpack was lenient about this; Vite's
// stricter ESM resolution rejects the named import on the subpath.
import { components } from 'react-select';
import React, { useContext, useEffect, useState, useCallback } from 'react';
import DJClientContext from '../../providers/djclient';

// Debounce helper
const debounce = (fn, ms) => {
  let timer;
  return (...args) => {
    clearTimeout(timer);
    return new Promise(resolve => {
      timer = setTimeout(() => resolve(fn(...args)), ms);
    });
  };
};

/**
 * Extract namespace from a fully qualified metric name.
 * e.g., "finance.total_revenue" -> "finance"
 */
const getNamespace = metricName => {
  if (!metricName) return 'default';
  const parts = metricName.split('.');
  return parts.slice(0, -1).join('.') || 'default';
};

/**
 * Git branch icon SVG component
 */
const GitBranchIcon = () => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    width="10"
    height="10"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2"
    strokeLinecap="round"
    strokeLinejoin="round"
  >
    <line x1="6" y1="3" x2="6" y2="15" />
    <circle cx="18" cy="6" r="3" />
    <circle cx="6" cy="18" r="3" />
    <path d="M18 9a9 9 0 0 1-9 9" />
  </svg>
);

/**
 * Custom Group heading component matching Explorer styling.
 */
const GroupHeading = props => {
  const { data } = props;
  const { namespace, gitInfo, count } = data;

  return (
    <div
      style={{
        display: 'flex',
        alignItems: 'center',
        padding: '8px 12px',
        backgroundColor: '#fafafa',
        borderBottom: '1px solid #eee',
      }}
    >
      <span style={{ fontWeight: 500, color: '#333' }}>{namespace}</span>
      {gitInfo?.branch && (
        <span
          title={`Branch: ${gitInfo.branch}`}
          style={{
            marginLeft: '8px',
            fontSize: '11px',
            padding: '2px 6px',
            borderRadius: '3px',
            backgroundColor: gitInfo.isDefaultBranch ? '#d4edda' : '#fff3cd',
            color: gitInfo.isDefaultBranch ? '#155724' : '#856404',
            display: 'inline-flex',
            alignItems: 'center',
            gap: '4px',
          }}
        >
          <GitBranchIcon />
          {gitInfo.branch}
        </span>
      )}
      <span style={{ marginLeft: '8px', color: '#999', fontSize: '12px' }}>
        ({count})
      </span>
    </div>
  );
};

/**
 * Group flat metrics array into react-select grouped format.
 * Includes branch info for styled group headers.
 */
const groupMetricsByNamespace = metrics => {
  const grouped = {};
  const gitInfoByNamespace = {};

  metrics.forEach(metric => {
    const namespace = getNamespace(metric.value);
    if (!grouped[namespace]) {
      grouped[namespace] = [];
      gitInfoByNamespace[namespace] = metric.gitInfo;
    }
    grouped[namespace].push(metric);
  });

  // Sort namespaces alphabetically and build grouped options
  return Object.keys(grouped)
    .sort()
    .map(namespace => {
      const gitInfo = gitInfoByNamespace[namespace];
      const count = grouped[namespace].length;

      return {
        label: namespace,
        namespace,
        gitInfo,
        count,
        options: grouped[namespace],
      };
    });
};

/**
 * Custom option component that shows display name and full node name.
 */
const formatOptionLabel = (option, { context }) => {
  if (context === 'menu') {
    const displayName = option.label;
    const nodeName = option.value;
    const isDifferent = displayName !== nodeName;

    return (
      <div>
        <div>{displayName}</div>
        {isDifferent && (
          <div style={{ fontSize: '12px', color: '#999', marginTop: '2px' }}>
            {nodeName}
          </div>
        )}
      </div>
    );
  }
  // For selected chips: show display name with tooltip and optional branch badge
  const displayName = option.label || option.value;
  const gitInfo = option.gitInfo;
  const showBranch = gitInfo?.branch && !gitInfo?.isDefaultBranch;

  return (
    <span
      title={option.value}
      style={{ display: 'inline-flex', alignItems: 'center', gap: '4px' }}
    >
      <span>{displayName}</span>
      {showBranch && (
        <span
          style={{
            fontSize: '9px',
            padding: '1px 4px',
            borderRadius: '2px',
            backgroundColor: 'rgba(255, 255, 255, 0.6)',
            color: '#a2283e',
            border: '1px solid rgba(162, 40, 62, 0.2)',
            fontWeight: 500,
            letterSpacing: '0.2px',
          }}
        >
          ⎇ {gitInfo.branch}
        </span>
      )}
    </span>
  );
};

export const MetricsSelect = React.memo(function MetricsSelect({
  cube,
  onChange,
}) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;

  // Currently selected metrics (for controlled component)
  const [selectedMetrics, setSelectedMetrics] = useState([]);

  // Load existing cube metrics when editing
  useEffect(() => {
    if (cube?.current?.cubeMetrics) {
      const cubeMetrics = cube.current.cubeMetrics.map(metric => ({
        value: metric.name,
        label: metric.displayName || metric.name,
      }));
      setSelectedMetrics(cubeMetrics);
      onChange(cubeMetrics.map(m => m.value));

      // Fetch gitInfo for existing metrics so we can display branch badges
      const names = cubeMetrics.map(m => m.value);
      djClient.getMetricsInfo(names).then(enriched => {
        if (enriched.length > 0) {
          const infoByName = Object.fromEntries(
            enriched.map(e => [e.value, e]),
          );
          setSelectedMetrics(prev =>
            prev.map(m => (infoByName[m.value] ? infoByName[m.value] : m)),
          );
        }
      });
    }
  }, [cube, onChange, djClient]);

  // Async load options - searches metrics via GraphQL, grouped by namespace
  const loadOptions = useCallback(
    debounce(async inputValue => {
      if (!inputValue || inputValue.length < 2) {
        return [];
      }
      try {
        const results = await djClient.searchMetrics(inputValue, 50);
        return groupMetricsByNamespace(results);
      } catch (error) {
        console.error('Error searching metrics:', error);
        return [];
      }
    }, 300),
    [djClient],
  );

  const handleChange = selected => {
    setSelectedMetrics(selected || []);
    onChange((selected || []).map(option => option.value));
  };

  // Custom styles to color-code metric tags (matching Query Planner exactly)
  const metricStyles = {
    multiValue: base => ({
      ...base,
      backgroundColor: '#fad7dd',
      border: '1px solid rgba(162, 40, 62, 0.3)',
      borderRadius: '3px',
      margin: '2px',
    }),
    multiValueLabel: base => ({
      ...base,
      color: '#a2283e',
      fontSize: '10px',
      fontWeight: 500,
      padding: '2px 4px 2px 6px',
    }),
    multiValueRemove: base => ({
      ...base,
      color: '#a2283e',
      padding: '0 4px',
      ':hover': {
        backgroundColor: '#f5c4cd',
        color: '#a2283e',
      },
    }),
  };

  return (
    <AsyncSelect
      value={selectedMetrics}
      loadOptions={loadOptions}
      onChange={handleChange}
      name="metrics"
      placeholder="Type to search metrics..."
      noOptionsMessage={({ inputValue }) =>
        inputValue.length < 2
          ? 'Type at least 2 characters to search'
          : 'No metrics found'
      }
      loadingMessage={() => 'Searching...'}
      formatOptionLabel={formatOptionLabel}
      components={{ GroupHeading }}
      styles={metricStyles}
      isMulti
      isClearable
      closeMenuOnSelect={false}
      cacheOptions
      defaultOptions={false}
    />
  );
});
