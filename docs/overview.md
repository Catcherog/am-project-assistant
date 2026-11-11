# Project workflows and evidence

AM's intended audience includes project managers who maintain progress records, teams that discuss delivery and acceptance, and developers who integrate AI assistance into collaboration platforms. The current public material documents these workflows; it is not a runnable implementation.

## Workflow

A typical project question passes through identity and scope checks, retrieval of relevant records, answer or proposal preparation, required review, execution, and result readback. These are distinct responsibilities. A model's proposed action is not evidence that an external system performed it.

Implementation choices, model names, and platform integration details are owned by the development project. The community documentation keeps the behavior and evidence requirements understandable without prescribing a replacement architecture.

## Terms that must remain distinct

| Term | Meaning | Insufficient evidence |
| --- | --- | --- |
| Plan | An intended future activity or date | The planned date passing |
| Estimate | A forecast subject to stated assumptions | A confident model answer |
| Completion | Evidence that a defined activity finished | A plan or uploaded attachment alone |
| Acceptance | A relevant reviewer accepted a defined deliverable and scope | Completion of engineering checks alone |
| Approval | An authorized person permitted a particular proposal or action | An old approval for changed content or a different target |
| Unknown result | Available evidence cannot determine whether an action completed | A timeout or local process exit |

These concepts are not a universal linear state machine. For example, approval can occur before execution, and completion can exist without acceptance.

## Evidence dependencies

An answer or proposal should identify the project, source, version, relevant content, time, and authorized audience on which it depends. Changes to relevant evidence require reassessment. Unrelated edits should not automatically invalidate every read-only answer, but deciding relevance must include subject resolution and access checks.

A check performed before an external write does not by itself eliminate a race with a later edit. A deployed implementation must document its final execution boundary, platform concurrency support, and residual risks. This preview does not claim atomicity across independent systems.

## Completion evidence

An HTTP response, a successful tool transport, an application-level result, an external readback, and business acceptance answer different questions. Reports should preserve these layers and explicitly handle missing acknowledgments. An unknown outcome should be reconciled before deciding whether another attempt is appropriate.

See the [synthetic cases](../examples/README.md) for concrete discussion examples and the [study protocol](../research/protocol.md) for proposed measurements.
