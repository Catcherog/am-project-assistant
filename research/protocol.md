# Study protocol draft

Working topic: **Evidence changes and reliable project-management assistance**.

This protocol is a proposal. It has not been preregistered, independently reviewed, or executed as a comparative study. The public example set is illustrative. Runtime variants and any product changes must be supplied by the AM development project, not implemented through publication work.

## Research questions

1. How often do agents confuse plans, estimates, completion, acceptance, or approval when preparing project answers and actions?
2. Can rechecking declared evidence dependencies and approval bindings reduce unsupported claims and incorrect actions while preserving legitimate task completion?
3. What latency and human-correction costs accompany these checks across projects and repeated runs?

The hypothesized improvement is a research question, not a result. A system that blocks every action must not appear effective merely because it avoids incorrect writes.

## Scope and units

The unit of sampling is an underlying business scenario. A scenario may contain multiple turns, source revisions, channels, and repeated runs. These are nested observations, not additional independent projects or tasks.

Study two layers separately: read-only answers and actions with external effects. Completion of a local test, a model response, an HTTP call, and an external business action are distinct outcomes. Any prototype must document the final check-to-execution boundary and residual concurrency risks.

## Materials and sampling

Start with a pilot inventory of existing authorized cases, preserving failed attempts and near-miss cases. Cover the families illustrated in [the examples](../examples/README.md), and add normal successful tasks as controls. Do not select cases solely because the current system solves them.

Keep synthetic, permissioned reconstructions, and prospective real observations in separate strata. Record provenance, version, task family, project cluster, evidence timestamps, requested action, and any exclusions. Do not publish raw business records.

Use pilot variance, task clustering, the smallest useful effect, annotation cost, and available project diversity to choose the formal sample size. A convenient round number or a large count of repeated prompts is not a sample-size justification.

## Annotation

Two reviewers should independently evaluate a subset sufficient to estimate agreement, with at least one reviewer independent of the implementation where feasible. Reviewers need domain knowledge and a fixed rubric; record their relationship to the project privately. Resolve disagreements through an adjudication record rather than silently replacing labels.

Annotate supported claims, permitted actions, mandatory qualifications, forbidden overstatements, and acceptable clarification behavior. Allow multiple correct formulations. Evaluate complete outputs; keyword occurrence alone is insufficient. AI may assist triage, but AI-generated labels and self-scores must remain identified as such.

## Splits and leakage controls

Separate development and test material by project and time where possible. Keep variants of the same underlying scenario in the same split. Freeze the test set, rubric, model versions, system configuration, and analysis plan before inspecting final results. Track whether any test material was previously used in prompts, repair discussions, training, or model-based optimization.

Repeated adaptive evaluation can overfit a held-out set. Reserve a final untouched set or recruit an independent final evaluation once the development procedure is fixed. Current illustrative cases cannot be relabeled as untouched holdout material.

## Comparisons

Use the same evidence, model, tool permissions, and budgets across matched comparisons:

- A well-specified direct-prompt/tool baseline.
- A conventional workflow with explicit rules and human review.
- The selected AM variant with declared evidence and approval checks.

Include a relevant published or open implementation when its task scope permits a fair comparison. Record adaptation effort. Ablate one mechanism at a time only when the AM owner can supply a meaningful, controlled variant. Do not weaken a baseline to create an artificial gain.

## Measurements

Primary measures: independently adjudicated task correctness; unsupported state claims; incorrect external actions; unnecessary refusal or re-review; and manual correction effort.

For actions, inspect resulting state and an operation identity where available. Preserve `unknown` as its own outcome until reconciled. Report denominators, exclusions, failures, and unresolved attempts. Successful-transport counts cannot substitute for task correctness.

Secondary measures: full-flow latency, model calls and cost, review burden, and reliability over repeated runs. Report latency for successes and the failure/timeout distribution separately, with a clearly defined start and endpoint. Do not estimate a stable tail percentile from a handful of observations.

## Analysis plan

Use paired comparisons for matched scenarios. Report effect sizes and uncertainty, not only significance. Account for repeated runs and project clustering using a suitable cluster bootstrap or hierarchical model; justify the choice before final analysis. Stratify read-only and write tasks and examine both failure prevention and unnecessary blocking.

Preserve a complete run ledger with model/version, configuration digest, scenario/split identity, timestamps, outcome, and adjudication status. Publish de-identified aggregate results and replayable synthetic cases where rights permit. A result table will remain absent until the corresponding experiments and reviews exist.

## Threats to validity

Single-team behavior, author-designed cases, synthetic dialogue, imperfect annotations, provider drift, overlapping project facts, and adaptive testing can all bias results. Document the system's dependence on platform permissions and source quality. Distinguish an operational case study from a causal claim about organization-wide productivity.

## Publication route

A software-engineering study may fit Information and Software Technology or a future ICSE practice track if the contribution is clearly software-engineering relevant. An industrial-process study may fit Computers in Industry; a team-collaboration study requires the kinds of human and organizational evidence expected by CSCW. Advanced Engineering Informatics requires a substantive engineering-knowledge contribution and strong validation. JOSS concerns mature research software and has its own public-development requirements.

Select one route after the pilot clarifies the contribution. Do not submit this protocol or the illustrative cases as a completed empirical paper. Check the target venue's current data, authorship, AI-disclosure, and submission policies before submission.
