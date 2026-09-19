import React from 'react';

/**
 * A compact pill segmented control: a rounded pill split into option halves with
 * a hairline divider and a light-blue active tint. Shared so the same styling is
 * used by the namespace filters and the Git Configuration modal.
 *
 * Clicking the active option calls `onChange('')` (deselect). Callers that must
 * always keep a selection should ignore the empty value, e.g.
 * `onChange={v => v && setValue(v)}`.
 */
export default function SplitFilter({ label, options, value, onChange }) {
  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        gap: '2px',
        flex: '0 0 auto',
      }}
    >
      <label
        style={{
          fontSize: '10px',
          fontWeight: '600',
          color: '#666',
          textTransform: 'uppercase',
          letterSpacing: '0.5px',
        }}
      >
        {label}
      </label>
      <div
        style={{
          display: 'inline-flex',
          alignItems: 'center',
          height: '32px',
          border: '1px solid #cbd5e1',
          borderRadius: '999px',
          backgroundColor: '#ffffff',
          overflow: 'hidden',
          whiteSpace: 'nowrap',
        }}
      >
        {options.map((option, index) => {
          const active = value === option.value;
          return (
            <button
              key={option.value}
              type="button"
              onClick={() => onChange(active ? '' : option.value)}
              style={{
                height: '30px',
                margin: 0,
                padding: '0 13px',
                border: 'none',
                borderLeft: index === 0 ? 'none' : '1px solid #e2e8f0',
                borderRadius: 0,
                backgroundColor: active ? '#e3f2fd' : 'transparent',
                color: active ? '#1976d2' : '#475569',
                fontFamily: 'inherit',
                fontSize: '12px',
                fontWeight: active ? '600' : '500',
                textTransform: 'none',
                cursor: 'pointer',
                whiteSpace: 'nowrap',
              }}
            >
              {option.label}
            </button>
          );
        })}
      </div>
    </div>
  );
}
