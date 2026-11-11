# Related work and novelty questions

This is an initial literature map, checked on 2026-11-11. It is not a systematic review or a completed novelty claim. Paper results are not directly comparable to AM without matching tasks, data, models, budgets, and evaluation procedures.

| Work | Relevant contribution | Required comparison |
| --- | --- | --- |
| [DevNous](https://www.sciencedirect.com/science/article/pii/S0950584926000674), also available as [a preprint](https://arxiv.org/abs/2508.08761) | Conversation-driven project artifacts and a public, annotated conversational benchmark | Compare task formalization and progress synthesis; explain exactly how changing evidence and approval scope extend the research question |
| [AgentSpec](https://arxiv.org/abs/2503.18666) | Runtime rules and enforcement for agents | A new rule wrapper is not sufficient novelty; compare enforcement semantics and utility under legitimate tasks |
| [TheAgentCompany](https://arxiv.org/abs/2412.14161) | Evaluation of workplace tasks involving tools and collaboration | Explain which project-governance failure families are covered or missing after examining the actual tasks |
| [WorkArena++](https://arxiv.org/abs/2407.05291) | Enterprise workflows requiring planning and reasoning | Separate platform-operation difficulty from evidence and business-state correctness |
| [tau-bench](https://arxiv.org/abs/2406.12045) | User–agent–tool interaction, final-state checks, and reliability across trials | Reuse the principle of state-based evaluation while pinning the maintained task version for any direct comparison |
| [A Taxonomy of Runtime Faults in Model Context Protocol Servers](https://arxiv.org/abs/2606.05339) | Empirical classification of runtime faults | Check whether an observed failure is already covered; a single upstream bug fix is not automatically a research contribution |
| [Darwin Gödel Machine](https://arxiv.org/abs/2505.22954) | Empirical evaluation of self-improving agent code | Defer autonomous-improvement claims until independent held-out benefits and failure behavior are measured |

The main candidate contribution is an empirically tested account of how evidence changes, project identity, approval binding, and unknown outcomes jointly affect task correctness. Version checks, hashing, optimistic concurrency, and human approval are established techniques. The study must identify and substantiate its additional modeling or empirical contribution.

The [maintained Sierra repository](https://github.com/sierra-research/tau2-bench) should be examined before selecting tasks; the [original repository](https://github.com/sierra-research/tau-bench) warns that its task versions are outdated. Record the exact selected commit and retain the original version label in reports.

Full-text related-work screening, reference chaining, and a task-by-task coverage comparison remain to be completed before submission. References here are factual background, not evidence of AM's effectiveness.
