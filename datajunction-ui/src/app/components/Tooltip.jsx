import { useId, useState } from 'react';

export default function Tooltip({
  content,
  placement = 'bottom',
  children,
  maxWidth = 240,
}) {
  const [visible, setVisible] = useState(false);
  const tooltipId = useId();

  if (!content) return children;

  const show = () => setVisible(true);
  const hide = () => setVisible(false);

  const isTop = placement === 'top';

  return (
    <span
      style={{ position: 'relative', display: 'inline-flex' }}
      onMouseEnter={show}
      onMouseLeave={hide}
      onFocus={show}
      onBlur={hide}
    >
      <span aria-describedby={visible ? tooltipId : undefined}>{children}</span>
      {visible && (
        <span
          role="tooltip"
          id={tooltipId}
          style={{
            position: 'absolute',
            [isTop ? 'bottom' : 'top']: 'calc(100% + 6px)',
            left: '50%',
            transform: 'translateX(-50%)',
            backgroundColor: '#1e293b',
            color: '#ffffff',
            fontSize: '12px',
            lineHeight: 1.4,
            fontWeight: 400,
            padding: '6px 10px',
            borderRadius: '6px',
            boxShadow: '0 4px 12px rgba(0, 0, 0, 0.18)',
            maxWidth: `${maxWidth}px`,
            width: 'max-content',
            textAlign: 'center',
            whiteSpace: 'normal',
            zIndex: 2000,
            pointerEvents: 'none',
          }}
        >
          {content}
          {/* Caret sits on the edge nearest the trigger. */}
          <span
            style={{
              position: 'absolute',
              [isTop ? 'bottom' : 'top']: '-4px',
              left: '50%',
              transform: 'translateX(-50%) rotate(45deg)',
              width: '8px',
              height: '8px',
              backgroundColor: '#1e293b',
            }}
          />
        </span>
      )}
    </span>
  );
}
