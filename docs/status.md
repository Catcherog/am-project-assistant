# Current publication status

As of 2026-11-11 this repository is a documentation and research-preparation preview. The AM development project is iterating independently. No running installation is changed by publication work.

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

The documentation and examples can be read and their structure checked locally. The twelve synthetic examples are proposed annotation cases. They are not observations of real employees or customers, and they are not a validated test set.

## Required before a runtime release

A complete source package, dependency and license inventory, safe configuration example, clean-environment installation evidence, and an accurate capability matrix are still required. The source-release checklist assigns these inputs to their owners. Availability of an internal implementation is not proof that an external contributor can install it.

## Known research gaps

- Independent annotation and adjudication of task expectations.
- A frozen evaluation set separated by project and time from development material.
- Comparable baselines and controlled component ablations.
- Full-flow latency, failure, and manual-correction measurements.
- A study of actual team use if collaboration or productivity claims are pursued.

These gaps are research prerequisites, not newly verified runtime defects.

## Automated publication checks

The structure and link checker has passed locally. GitHub Actions is not enabled for this preview because the publishing credential lacks workflow-upload permission. The workflow template remains with the maintainer for later enablement; there is no cloud CI result to claim.
