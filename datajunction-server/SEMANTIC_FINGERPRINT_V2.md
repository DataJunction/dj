# Offline semantic fingerprint component core

Version 2 is an inactive candidate. Deployment responses, validation, and the
default fingerprint engine continue to use version 1.

This layer provides isolated `SemanticGraphSnapshot` inputs, component addresses
and recipes, labelled dependency hashing, and deterministic evaluation of
component graphs, including cycles. It accepts explicit component definitions;
SQL dependency extraction and the node fingerprint entry point follow in the
next layer.

A component carries a versioned digest or `"unknown"`, a precision classification,
and reasons. `compare_components()` returns `indeterminate` when either digest
is unavailable. Missing dependencies propagate unknown, and conservative
dependencies propagate conservative precision.

Snapshots copy definitions and resolved column schemas. Evaluation makes no
database calls and does not reconstruct compiler or ORM objects.
