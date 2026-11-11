# Governance

The repository is maintained under the Catcherog account. It is the community-facing source, documentation, and research-preparation workspace for AM.

## Responsibilities

| Responsibility | Scope |
| --- | --- |
| AM development owner | Runtime behavior, source changes, model integrations, deployment and operational acceptance |
| Community publication maintainer | Documentation, de-identification, examples, attribution, release descriptions and community triage |
| Research contributors | Study design, annotations, reproducibility material and analysis |
| Independent reviewers | Assess the declared materials and scope; report disagreements and limits |

The maintainer may currently perform more than one role. That does not make self-review independent review. Individual authorship and affiliation will be agreed before a paper is submitted; no academic author list is assigned by this preview.

## Changes and releases

Documentation and synthetic examples can be changed through reviewed PRs. Material that changes runtime behavior must be routed to the AM development owner. Publication work must not patch a running installation.

Runtime publication requires a fixed source identity, a disclosure and license review, explicit dependency information, safe examples, and a reproducible verification procedure. The R8 preview verifies core offline checks, not a production deployment. See [the release checklist](docs/release-checklist.md).

A release note describes what was actually published and verified. Engineering checks, external platform delivery, business correctness, and independent user acceptance are reported separately. Failed attempts remain in the relevant private evidence history; sanitized aggregate reports must not silently remove them.

## Community decisions

Discuss substantial scope changes in issues. Record the motivation, decision, and consequences in the relevant document. Status can be proposed, accepted, deferred, or superseded; popularity alone does not establish correctness or research validity.

## Research authorship

Credit substantive human contributions to framing, design, implementation, data, evaluation, analysis, and writing. A GitHub contribution does not automatically imply paper authorship. Disclose AI assistance according to the target venue's current requirements and obtain each author's approval before submission.
