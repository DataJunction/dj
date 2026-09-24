import { useContext, useEffect, useRef, useState } from 'react';
import DJClientContext from '../../providers/djclient';
import * as React from 'react';

/**
 * The governance checks that apply to this node, evaluated as it stands now.
 *
 * A check whose `when` clause excluded this node is reported as skipped rather
 * than dropped, so the counts add up to what the manifest declares.
 */
export default function NodeChecks({ node }) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;
  const [results, setResults] = useState(undefined);
  const [open, setOpen] = useState(false);
  // Which tiers have had their met checks revealed. The unmet ones always
  // show; the met ones are what a reader asks for only when curious.
  const [expanded, setExpanded] = useState({});
  const ref = useRef(null);
  const toggle = name =>
    setExpanded(state => ({ ...state, [name]: !state[name] }));

  useEffect(() => {
    const fetchData = async () => {
      setResults(await djClient.nodeChecks(node.name));
    };
    fetchData().catch(console.error);
  }, [djClient, node]);

  if (results === undefined) {
    return <></>;
  }
  // Null when no manifest covering this node declares any checks.
  if (results === null) {
    return <span className="status">Not governed</span>;
  }

  const checks = results.checks || [];
  const skipped = checks.filter(check => check.verdict === 'skipped');

  // A tier a human named reads as a bar someone set, where a bare count of
  // failures reads like a second opinion on whether the node works.
  const tiers = (results.rulesets || []).filter(
    ruleset => ruleset.verdict !== 'not_applicable',
  );
  // Nothing applied here, so there is nothing to meet or miss.
  if (checks.length === skipped.length) {
    return <span className="status">Not governed</span>;
  }

  const byName = Object.fromEntries(checks.map(check => [check.check, check]));

  // `checks` is the expanded membership, so one tier building on another shows
  // up as a superset. Sorting by size puts a tier after everything it contains.
  const ordered = [...tiers].sort(
    (a, b) => (a.checks || []).length - (b.checks || []).length,
  );
  // A ladder only if each tier really does contain the one before it. Rulesets
  // need not nest -- `pii` and `finance` would be siblings -- and calling a set
  // of siblings a ladder would invent a progression nobody declared.
  // One tier is trivially a chain, and naming it beats tallying it.
  const isLadder = ordered.every((tier, index) => {
    if (index === 0) return true;
    const inner = new Set(ordered[index - 1].checks || []);
    return [...inner].every(name => (tier.checks || []).includes(name));
  });

  // In a ladder each tier is credited only with what it adds, so nothing is
  // listed twice. Siblings each stand alone and keep their whole membership.
  const claimed = new Set();
  const rows = ordered.map((tier, depth) => {
    const own = (tier.checks || []).filter(
      name => !isLadder || !claimed.has(name),
    );
    own.forEach(name => claimed.add(name));
    const members = own.map(name => byName[name]).filter(Boolean);
    return {
      ...tier,
      builtOn: isLadder && depth > 0 ? ordered[depth - 1].ruleset : null,
      unmet: members.filter(check => check.verdict === 'failed'),
      met: members.filter(check => check.verdict === 'passed'),
      total: members.length,
    };
  });

  // A check belongs to a ruleset only if someone put it in one. The rest are
  // still evaluated and still fail, so they get a group of their own rather
  // than disappearing between the tiers.
  const loose = checks.filter(
    check => !claimed.has(check.check) && check.verdict !== 'skipped',
  );
  if (loose.length > 0) {
    rows.push({
      ruleset: null,
      verdict: loose.every(check => check.verdict === 'passed')
        ? 'passed'
        : 'failed',
      builtOn: null,
      unmet: loose.filter(check => check.verdict === 'failed'),
      met: loose.filter(check => check.verdict === 'passed'),
      total: loose.length,
    });
  }

  const passedCount = ordered.filter(t => t.verdict === 'passed').length;
  // Only a ladder has a level to name; siblings just have a tally.
  const reached = isLadder
    ? [...ordered].reverse().find(tier => tier.verdict === 'passed')
    : null;
  const next = isLadder ? rows.find(tier => tier.verdict !== 'passed') : null;

  return (
    // Anchors the card, which is positioned out of flow so that opening it
    // cannot reflow the fields beside it.
    <span style={{ position: 'relative', display: 'inline-block' }}>
      <button
        className="badge"
        aria-label="GovernanceChecks"
        style={{
          backgroundColor: 'transparent',
          color: reached ? '#1f6f43' : '#8a6d00',
          border: `1px solid ${reached ? '#00b36840' : '#8a6d0040'}`,
          fontSize: '14px',
          outline: '0',
          cursor: 'pointer',
        }}
        onClick={() => setOpen(!open)}
      >
        {isLadder ? (
          <>
            <span style={{ textTransform: 'capitalize' }}>
              {reached ? reached.ruleset : 'No tier met'}
            </span>
            {next && (
              <span style={{ color: '#8a6d00' }}>
                {' \u00b7 '}
                {next.unmet.length} from{' '}
                <span style={{ textTransform: 'capitalize' }}>
                  {next.ruleset}
                </span>
              </span>
            )}
          </>
        ) : (
          // Siblings have no order, so a tally is all that can be said.
          <span>
            {passedCount}/{ordered.length} met
          </span>
        )}{' '}
        {open ? '\u25b4' : '\u25be'}
      </button>
      <div
        ref={ref}
        style={{
          display: open === false ? 'none' : 'block',
          position: 'absolute',
          top: '100%',
          left: 0,
          marginTop: '0.4rem',
          padding: '0.6rem 0.9rem',
          width: 'max-content',
          maxWidth: '560px',
          background: '#ffffff',
          border: '1px solid #e2e2e2',
          borderRadius: '8px',
          fontSize: 'small',
          lineHeight: 1.75,
          zIndex: 100000,
        }}
      >
        {rows.map(tier => (
          <div key={tier.ruleset || 'other'} style={{ marginBottom: '0.4rem' }}>
            <div
              role="button"
              tabIndex={0}
              aria-label={`tier ${tier.ruleset || 'other'}`}
              onClick={() => toggle(tier.ruleset || 'other')}
              onKeyDown={event =>
                event.key === 'Enter' && toggle(tier.ruleset || 'other')
              }
              style={{
                display: 'flex',
                gap: '0.5rem',
                cursor: 'pointer',
              }}
            >
              <span
                style={{
                  color:
                    tier.verdict === 'passed'
                      ? '#00b368'
                      : tier.unmet.some(check => check.gate !== 'warn')
                      ? '#b34b00'
                      : '#8a6d00',
                  fontWeight: 600,
                  width: '0.9rem',
                }}
              >
                {tier.verdict === 'passed' ? '\u2713' : '\u2717'}
              </span>
              <span style={{ fontWeight: 600, textTransform: 'capitalize' }}>
                {tier.ruleset || 'Other checks'}
              </span>
              <span style={{ marginLeft: 'auto', opacity: 0.75 }}>
                {/*
                  The tier it builds on is a label, the count is the datum.
                  Same weight for both leaves the row reading as a sentence.
                */}
                {tier.builtOn && (
                  <span style={{ textTransform: 'capitalize', opacity: 0.65 }}>
                    {tier.builtOn}{' '}
                  </span>
                )}
                <span style={{ fontVariantNumeric: 'tabular-nums' }}>
                  {tier.builtOn ? '+' : ''}
                  {tier.met.length}/{tier.total}
                </span>
                {tier.verdict === 'passed' && (
                  <span style={{ fontSize: '0.8em', marginLeft: '0.4rem' }}>
                    {expanded[tier.ruleset || 'other'] ? '\u25b4' : '\u25be'}
                  </span>
                )}
              </span>
            </div>
            {/*
              A tier can be part met and part unmet, so each line carries its
              own mark. A failing tier shows all of itself -- hiding part of
              what it asked for leaves its score not reconciling with the
              lines under it.
            */}
            {[
              ...tier.unmet.map(check => ({ check, ok: false })),
              ...(expanded[tier.ruleset || 'other'] || tier.verdict !== 'passed'
                ? tier.met.map(check => ({ check, ok: true }))
                : []),
            ].map(({ check, ok }) => (
              <div
                key={check.check}
                title={check.check}
                style={{
                  display: 'flex',
                  gap: '0.5rem',
                  marginLeft: '0.9rem',
                  opacity: ok ? 0.55 : 0.9,
                }}
              >
                <span
                  style={{
                    color: ok
                      ? '#00b368'
                      : check.gate === 'warn'
                      ? '#8a6d00'
                      : '#b34b00',
                    width: '0.7rem',
                  }}
                >
                  {ok ? '✓' : '✗'}
                </span>
                <span>{check.description || check.check}</span>
              </div>
            ))}
          </div>
        ))}
        {skipped.length > 0 && (
          <div style={{ opacity: 0.55, marginTop: '0.5rem' }}>
            {skipped.length} did not apply
          </div>
        )}
      </div>
    </span>
  );
}
