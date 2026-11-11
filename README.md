# AM Project Assistant

[简体中文](README.zh-CN.md) · [Project overview](docs/overview.md) · [Current status](docs/status.md) · [Contribute](CONTRIBUTING.md)

AM is an evolving project-management assistant for Feishu/Lark workflows. Its focus is keeping project answers and proposed actions tied to their evidence: a plan is not a completion record, a finished task is not automatically accepted, and an earlier approval may no longer apply to a changed proposal.

**This repository is the community documentation preview.** It currently publishes original documentation, synthetic examples, and a research protocol. The runnable AM service and deployment package have not been released here. There is no installation command or public performance claim for this preview.

## Start here

- **Project managers:** read the [workflow and terminology](docs/overview.md), then inspect the [synthetic examples](examples/README.md).
- **Contributors:** improve documentation, review example expectations, or propose reproducible evaluation cases using the [contribution guide](CONTRIBUTING.md).
- **Researchers:** read the [study protocol](research/protocol.md), [related work](research/related-work.md), and [manuscript draft](research/manuscript-draft.md).
- **Developers waiting for the runtime:** follow the [source-release checklist](docs/release-checklist.md). Runtime implementation remains with the AM development project.

## What is available

| Material | Status |
| --- | --- |
| English and Chinese introduction | Available |
| Workflow concepts, evidence terminology, community policies | Available |
| Twelve fully synthetic examples with proposed expectations | Available for discussion; not a validated benchmark |
| Research questions, evaluation protocol, manuscript starter | Draft; no experimental results |
| Runnable bot, installer, model integration, platform adapter | Not included in this preview |
| Independent deployment and business acceptance | Not established for a public release |

The distinction between an implementation report, an executed check, a platform receipt, and business acceptance is documented in [Current status](docs/status.md).

## Review the examples locally

No Feishu account, model, or API key is needed to inspect the published examples. With Python 3.9 or later:

```bash
python3 tools/check_publication.py
```

This checks documentation links and example structure. It does **not** run AM or measure agent performance. Example answers are discussion drafts and must be independently reviewed before research use.

## Help shape the open project

Useful first contributions include a clearer example, a translated explanation, an independently reasoned correction to a proposed answer, or a reproducible case showing why evidence or approval must be rechecked. Start with an [issue](https://github.com/Catcherog/am-project-assistant/issues) or a small documentation PR.

Please use fabricated data in public reports. See [privacy and publication rules](docs/publishing.md) and the [security policy](SECURITY.md).

## License and attribution

The material currently published here is covered by the [MIT license](LICENSE). It consists of newly authored documentation, synthetic data, and publication-check tooling. This license does not grant rights to unpublished AM code, internal records, model weights, or third-party software. Future source imports require a separate provenance and dependency review.

Documentation and fixtures were prepared with AI assistance. They are research preparation material, not independently validated findings. See [Governance](GOVERNANCE.md) for responsibilities and review expectations.
