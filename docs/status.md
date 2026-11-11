# Current publication status

As of 2026-11-11 this repository contains an R8 source preview, community documentation and research preparation. The AM development project is iterating independently. No running installation is changed by publication work.

## Evidence boundaries

| Evidence | What it can establish | What it cannot establish alone |
| --- | --- | --- |
| A fixed source revision | Which implementation was examined | What is currently deployed |
| A unit or integration test | Behavior under the tested conditions | Correctness across real business tasks |
| A real model response | That a model returned output for an input | That the output is semantically correct |
| A platform receipt | A platform acknowledged a specific operation | User-visible presentation or business acceptance |
| An independent business review | A reviewer's judgment within the recorded scope | Untested projects or future versions |

Development reports may help identify research questions. Their test counts must not be represented as public benchmark scores. This repository currently contains no independently validated empirical results.

## Available now

The de-identified source contains 464 selected files. In a fresh Python 3.12.13 environment, the fixed baseline and the public copy each passed the same 1807 core tests. Two local loopback demos passed with zero real model calls and zero Feishu sends. See [source verification](source-release.md) for scope and a known legacy-demo failure. Companion modules are included for source availability; this does not imply every companion feature was exercised by the core suite.

The twelve separate discussion examples remain proposed annotation cases. They are not observations of real employees or customers, and they are not a validated research test set.

## Required before a live deployment release

The versioned source and offline installation/check instructions are now published. Machine-specific launch overlays, real resources, CLI login state and production configuration are excluded. A portable live-deployment procedure, verified permission setup, independent installation and business acceptance remain required. The source-release checklist assigns those inputs to their owners.

## Known research gaps

- Independent annotation and adjudication of task expectations.
- A frozen evaluation set separated by project and time from development material.
- Comparable baselines and controlled component ablations.
- Full-flow latency, failure, and manual-correction measurements.
- A study of actual team use if collaboration or productivity claims are pursued.

These gaps are research prerequisites, not newly verified runtime defects.

## Automated publication checks

The structure and link checker has passed locally. GitHub Actions is not enabled for this preview because the publishing credential lacks workflow-upload permission. The workflow template remains with the maintainer for later enablement; there is no cloud CI result to claim.
