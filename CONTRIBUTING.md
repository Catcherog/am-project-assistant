# Contributing

Thank you for helping make evidence-aware project assistance understandable and reproducible. Documentation, translation, synthetic-case, research-protocol, and source-review contributions are welcome. Product-code changes must be reviewed by the AM development owner; the publication maintainer does not independently alter product behavior or deploy a change.

## Before opening a change

Search existing issues and PRs. For a new research direction or a large restructuring, describe the concrete problem and intended reader in an issue first. Small corrections can go directly to a PR. Chinese and English are both welcome.

Use a separate branch, keep changes focused, and explain what a reader or evaluator can do after the change. Include the command and actual result of relevant checks:

```bash
python3 tools/check_publication.py
```

For source changes, follow the [offline checks](docs/quickstart.md), disclose the exact test scope, and retain any existing failures. Read [source-update rules](docs/source-updates.md) before importing a development revision.

## Adding an example

Use the structure in [the example guide](examples/README.md). Clearly label synthetic content. Describe the question, available evidence, expected permitted behavior, and the basis for that expectation. Include a nearby positive case where useful; a policy that refuses every task is not a useful baseline.

An expected answer is a proposed annotation. Do not describe it as independent ground truth until an independent reviewer has checked it. Distinct channels, retries, or paraphrases of the same underlying task do not automatically create independent research samples.

## Privacy and provenance

Submit only material you can publish under this repository's license. Replace identities, tenant resources, paths, and business values consistently. Do not upload raw chats, screenshots of production workspaces, authentication files, or private evidence maps. Follow [the publication rules](docs/publishing.md). Report suspected sensitive disclosures privately using [SECURITY.md](SECURITY.md).

## AI assistance and research claims

State whether AI helped write prose, code, cases, or labels, and explain how the result was checked. Do not claim a human review that did not occur. Contributors remain responsible for the material they submit. Model-generated self-scores are not independent acceptance evidence.

Keep proposed experiments, completed runs, reviewed findings, and accepted publications separate. A passing documentation check is not an agent benchmark result.

## Review

The community maintainer reviews published material and routes runtime findings to the AM development owner. Follow [Governance](GOVERNANCE.md) and the [Code of Conduct](CODE_OF_CONDUCT.md). The project does not promise a review deadline or acceptance of every contribution.
