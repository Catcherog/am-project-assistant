# AM Project Assistant

[简体中文](README.zh-CN.md) · [Project overview](docs/overview.md) · [Current status](docs/status.md) · [Contribute](CONTRIBUTING.md)

AM is an evolving project-management assistant for Feishu/Lark workflows. Its focus is keeping project answers and proposed actions tied to their evidence: a plan is not a completion record, a finished task is not automatically accepted, and an earlier approval may no longer apply to a changed proposal.

**This repository now includes an R8 source preview.** The [runtime tree](runtime/) contains a de-identified snapshot of the versioned gateway, first-party companion modules, tests, and synthetic configuration examples. The original development history, machine-specific launch overlay, credentials, live configuration, business records, and model weights are excluded. Start with the verified [offline quickstart](docs/quickstart.md); live Feishu deployment still requires an operator-owned integration and configuration review.

## Start here

- **Project managers:** read the [workflow and terminology](docs/overview.md), then inspect the [synthetic examples](examples/README.md).
- **Contributors:** improve documentation, review example expectations, or propose reproducible evaluation cases using the [contribution guide](CONTRIBUTING.md).
- **Researchers:** read the [study protocol](research/protocol.md), [related work](research/related-work.md), and [manuscript draft](research/manuscript-draft.md).
- **Developers:** run the [offline quickstart](docs/quickstart.md), inspect the [source release and known limitations](docs/source-release.md), and follow the [future source-update process](docs/source-updates.md). Product implementation remains with the AM development project.

## What is available

| Material | Status |
| --- | --- |
| English and Chinese introduction | Available |
| Workflow concepts, evidence terminology, community policies | Available |
| Twelve fully synthetic examples with proposed expectations | Available for discussion; not a validated benchmark |
| Research questions, evaluation protocol, manuscript starter | Draft; no experimental results |
| R8 gateway and companion source snapshot | Available; 464 selected files, including 449 Python files |
| Core regression and local loopback demonstration | 1807 checks pass; both synthetic chat modes pass |
| Live deployment configuration, CLI logins, keys, tokens, model weights | Excluded |
| Independent production deployment and business acceptance | Not established by this source preview |

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

The published first-party material is covered by the [MIT license](LICENSE). See [third-party notices](THIRD_PARTY_NOTICES.md) for referenced external work and dependencies. This license does not grant rights to unpublished internal records, model weights, credentials, or third-party software. Future source imports require a fresh provenance and disclosure review.

Documentation and fixtures were prepared with AI assistance. They are research preparation material, not independently validated findings. See [Governance](GOVERNANCE.md) for responsibilities and review expectations.
