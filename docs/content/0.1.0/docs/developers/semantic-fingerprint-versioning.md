---
weight: 85
title: "Semantic fingerprint compatibility"
description: "Compatibility rules and migration procedures for semantic fingerprint consumers"
draft: false
---

Semantic fingerprints are versioned identifiers for a node definition and its
semantic ancestors. Consumers may store them in Git, use changes to select
reviewers, or compare a deployment preview with committed server state. Stable
fingerprints therefore require a compatibility contract that extends beyond the
final hash function.

For a given version, a fingerprint must remain stable when the complete DJ
metadata graph remains stable. The version covers all behavior that contributes
to the result:

- selected node fields and their defaults;
- field normalization and canonical JSON encoding;
- structural SQL serialization;
- semantic parent discovery and resolution;
- cycle handling and parent hash composition;
- propagation of the `unknown` sentinel; and
- the digest algorithm and its domain separators.

After consumers adopt a version, any change to this behavior requires a new
fingerprint version. A bug fix that changes existing digests also requires a new
version.

## What can change a fingerprint

The stability condition is:

```text
same fingerprint version + same complete DJ metadata graph = same fingerprint
```

A repository can retain identical files while one of its fingerprints changes.
The complete graph can change through:

- a Git-backed change in another namespace that supplies an ancestor;
- an API or UI update outside the repository deployment;
- source registration or schema reflection;
- resolution of a previously missing parent; or
- adoption of a new fingerprint version.

Node owners, tags, display names, node descriptions, and other fields outside a
version's semantic field projection do not change its digest. Metadata attached
to a projected field, such as a source column, may contribute to the digest.

## External tables

DJ fingerprints metadata rather than the live contents of a warehouse. When an
external table has a DJ source node, its catalog, schema, table, and reflected
columns contribute to the source fingerprint. A physical schema change affects
the source and its descendants after DJ refreshes or redeploys that source
metadata.

A physical change that has not reached DJ metadata is invisible to the
fingerprint. Data changes and properties absent from the source specification
are also invisible. A query parent with no corresponding DJ node makes the
fingerprint `unknown`, and that value propagates to its descendants.

Source fingerprints represent the full reflected column set. Fingerprints do
not currently track column-level lineage, so a reflected change to an unused
source column can change every descendant fingerprint.

## Normal server upgrade

Consumers should request an explicit fingerprint version. They should not use
the server's latest-version default as a rollout control.

Before a normal server upgrade:

1. Evaluate a representative committed metadata snapshot with the current and
   candidate server.
2. Request the fingerprint version pinned by each consumer.
3. Compare every returned value, including `unknown`.
4. Block the release if a pinned version changes unexpectedly.
5. Deploy the server without changing consumer baselines when the comparison is
   clean.

A normal server upgrade requires no fingerprint metadata update.

## Introducing a fingerprint version

New fingerprint behavior should be introduced through a staged migration:

1. Add the new version while continuing to serve every version used by a
   consumer.
2. Deploy the server with both the old and new versions available.
3. Keep consumers pinned to the old version.
4. Evaluate both versions over the production metadata graph and review the
   changed-node and owner counts.
5. Generate a dedicated metadata migration pull request containing the new
   version pin and its complete fingerprint baseline.
6. Mark that pull request as an algorithm migration so review automation does
   not notify every owner whose hash changed only because of the migration.
7. Switch the consumer to the new version when the migration pull request
   merges.
8. Retain the old server version until all consumers have migrated and the
   rollback window has closed.

The server deployment makes the new algorithm available. The migration pull
request chooses when a consumer adopts it. Fingerprints are computed from DJ
metadata, so this process does not require rewriting node metadata in the DJ
database.

## Review automation safeguards

Automation that turns fingerprint changes into review requests should enforce
the following controls:

- Record the fingerprint version in every generated baseline.
- Reject a server response whose version differs from the requested version.
- Require an explicit migration mode for a version change.
- Stop before opening or updating a pull request when an unexpected change
  exceeds a configured node or owner threshold.
- Report direct deployment changes separately from inherited Merkle changes.
- Treat transitions to or from `unknown` as a separate warning category.
- Cap automatic reviewer fanout and require manual approval above the cap.
- Suppress per-node owner notifications for an approved algorithm migration.

These controls keep a server defect or accidental same-version change from
creating a repository-wide review event.

## Rollback

The previous fingerprint version must remain available during the migration
window. If the new version has a defect:

1. Revert the consumer's version pin and generated baseline.
2. Resume requests for the previous version.
3. Fix the algorithm under another new version if the corrected output differs
   from a version already adopted by consumers.

No DJ node deployment is needed when rolling back only the fingerprint version.

## Release checklist

For a normal server release:

- [ ] Consumers request explicit versions.
- [ ] Full-graph golden fixtures pass.
- [ ] The candidate server matches the current server for pinned versions.
- [ ] No generated metadata migration is required.

For an algorithm migration:

- [ ] The new behavior uses a new version number.
- [ ] The server serves both old and new versions.
- [ ] Production graph differences and owner fanout have been reviewed.
- [ ] The migration pull request records the new version and complete baseline.
- [ ] Migration mode prevents repository-wide owner notifications.
- [ ] The previous version remains available for rollback.
