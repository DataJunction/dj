// Border props stay longhand: swapping the `border` shorthand for `borderColor`
// on rerender (as the hover handlers do) triggers a React warning.

export const secondaryButtonStyle = {
  height: '28px',
  padding: '0 10px',
  fontSize: '12px',
  fontWeight: '500',
  borderWidth: '1px',
  borderStyle: 'solid',
  borderColor: '#e2e8f0',
  borderRadius: '4px',
  backgroundColor: '#ffffff',
  color: '#475569',
  cursor: 'pointer',
  display: 'inline-flex',
  alignItems: 'center',
  gap: '6px',
  whiteSpace: 'nowrap',
  transition: 'all 0.15s ease',
};

export const primaryButtonStyle = {
  ...secondaryButtonStyle,
  backgroundColor: '#3b82f6',
  borderColor: '#3b82f6',
  color: '#ffffff',
};

export const dangerButtonStyle = {
  ...secondaryButtonStyle,
  color: '#dc2626',
  borderColor: '#fecaca',
};

export const onSecondaryHover = e => {
  e.currentTarget.style.color = '#1e293b';
  e.currentTarget.style.borderColor = '#cbd5e1';
  e.currentTarget.style.backgroundColor = '#f8fafc';
};

export const onSecondaryOut = e => {
  e.currentTarget.style.color = '#475569';
  e.currentTarget.style.borderColor = '#e2e8f0';
  e.currentTarget.style.backgroundColor = '#ffffff';
};

export const onDangerHover = e => {
  e.currentTarget.style.backgroundColor = '#fef2f2';
  e.currentTarget.style.borderColor = '#fca5a5';
};

export const onDangerOut = e => {
  e.currentTarget.style.backgroundColor = '#ffffff';
  e.currentTarget.style.borderColor = '#fecaca';
};
