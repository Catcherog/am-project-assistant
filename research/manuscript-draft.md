# Evidence Changes and Reliable Project-Management Assistance

**Manuscript starter — research design only.** No comparative experiments, independently annotated dataset, or real-team outcome study are reported here. Authors and affiliations have not been assigned. This draft requires substantive human review and experimental evidence before submission.

## Draft abstract

Project-management assistants must interpret information whose business meaning depends on scope, time, and authority. A planned date does not establish completion, a completed activity does not establish acceptance, and an approval may apply only to an earlier proposal. Moreover, source records and permissions can change while an assistant prepares an answer or action. We outline a study of these failures in collaboration-platform workflows. The proposed evaluation separates evidence-supported claims, authorized actions, and externally verified outcomes; it also measures unnecessary blocking and human correction effort. Matched comparisons and controlled ablations are planned across project-level splits and repeated runs. The current artifact provides a research protocol and synthetic discussion cases. Empirical results and claims of effectiveness remain to be established.

## 1 Introduction

Project information is often distributed between conversations, progress records, deliverables, and review decisions. An assistant can produce a plausible summary while changing the meaning of that information. For example, it may describe a scheduled verification as completed, extend acceptance of one batch to the whole project, or reuse an approval after the proposal has changed. These errors matter because the output may become the basis for another person's decision or an external update.

Existing research addresses conversation-driven project artifacts, workplace agents, interactive task evaluation, and runtime enforcement. [DevNous](https://arxiv.org/abs/2508.08761) is particularly relevant to project artifact generation, while [AgentSpec](https://arxiv.org/abs/2503.18666) addresses explicit runtime constraints. These foundations motivate a narrower empirical question: how do evidence changes and approval scope affect correctness and utility in project-management workflows?

This study will examine answers and actions under controlled changes to relevant evidence, irrelevant records, project identity, and authority. It will assess whether additional verification prevents unsupported outcomes without causing excessive refusals or repeated human review. The intended contribution is a reproducible evaluation and a substantiated account of the relevant trade-offs. The existence of an internal assistant or a large engineering test suite alone does not establish that contribution.

## 2 Problem formulation

A task is defined by a user request, a target project, an authorized audience, available evidence, and a permitted action scope. Evidence has content, provenance, temporal context, and a version. A proposed answer contains claims; a proposed action also has a target and parameters. An approval refers to a particular proposal and scope. The evaluation asks whether the claims are supported and whether the action remains permitted at the relevant execution boundary.

The formulation must not collapse plans, estimates, completion, acceptance, and approval into a single universal progression. It must also preserve unknown external outcomes. A timeout can leave the effects of a request unresolved; declaring failure and retrying may be incorrect even when the transport reports an error.

## 3 Study design

The proposed design uses scenario families that isolate business-state interpretation, changed dependencies, approval reuse, access revocation, ambiguity, duplicate events, and uncertain execution. Positive cases test whether supported answers and current authorized actions still complete. Data provenance and independent annotation status will be recorded separately from expected outcomes.

Comparisons will hold the model, source evidence, tool permissions, and budgets constant where feasible. Project and time splits will reduce leakage between development and final evaluation. Repeated runs will estimate behavioral reliability without inflating the number of independent scenarios. The detailed sampling, annotation, and analysis choices are specified in the [protocol](protocol.md).

## 4 Results to be reported after execution

The final paper will report task correctness, unsupported state claims, incorrect actions, unnecessary blocking, correction effort, and full-flow cost and latency with denominators and uncertainty. Read-only tasks and external-effect tasks will be presented separately. All planned comparisons need actual results, including failures and unresolved outcomes. No values are inserted in this draft.

## 5 Discussion questions

Which errors are prevented by source freshness checks, and which require domain interpretation? When does relevant-change detection preserve useful work? How does human review burden shift as checks become stricter? Which assumptions require platform support, and which findings generalize beyond one team's conventions? Answers must be grounded in the executed study rather than in implementation intent.

## 6 Limitations and research integrity

The current examples are AI-assisted synthetic discussion material. They have not been independently annotated and cannot establish real-world prevalence or improvement. Internal operational records may motivate case selection but cannot be published without a release review. Any future field study needs an appropriate participant and data-handling process. The final author list, affiliations, contribution statements, and venue-specific AI disclosure require human confirmation.

## Preparation note

This starter was prepared with OpenAI Codex assistance for planning and writing. No model-generated score is presented as a human judgment. Future authors must review the reasoning and citations, replace proposed methods with the actual executed procedure, and approve the final manuscript before submission.
