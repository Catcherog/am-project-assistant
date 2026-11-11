# R8 source preview

This source release comes from the fixed development revision `f1c16435927df45638928328bcf097582c3c60bc`. Files were read from Git objects into a private staging area, reviewed, de-identified, and then prepared as a new public commit. The development repository's history was not imported.

## Included source

The [runtime tree](../runtime/) contains 464 selected files, including 449 Python files. It includes the gateway, its integration modules, synthetic examples and evaluation specifications, the core test suite, and first-party companion source needed to understand the existing architecture. Legacy directory names are retained because several modules and tests resolve sibling packages by relative paths.

The [source manifest](../SOURCE_MANIFEST.json) records hashes of the public files. The original-to-public identity map and original private records remain private. Sixty-six selected files have authorized de-identification substitutions; the other selected files retain their source content. These are publication transformations, not new product features.

## Excluded material

Live configuration, CLI login state, keys and tokens, actual tenant/app/user/group/resource identifiers, databases, logs, model weights, raw business records, original audit documents and reports, and old Git history are excluded. The machine-local launch overlay is also excluded. Therefore the public source snapshot is not an image of a configured production installation.

The source retains generic CLI integration code, credential-field names, and clearly synthetic test values. Those are interface definitions and test fixtures, not the operator's credentials. Organization-specific names, protected-person examples, service labels, product examples, real resource defaults and private paths have been replaced consistently. Case and fullwidth variants used by regression fixtures were transformed together.

## Verification

| Check | Recorded result | Scope |
| --- | --- | --- |
| Fixed baseline core suite | 1807 passed, 0 failed; 2 dependency warnings | Fresh Python 3.12.13 environment |
| De-identified public core suite | 1807 passed, 0 failed; 2 dependency warnings | Same dependencies and test scope |
| Local p2p loopback demo | PASS | Synthetic; no real model calls or Feishu sends |
| Local group loopback demo | PASS | Synthetic; no real model calls or Feishu sends |
| Source parsing and fixture structure | Passed | Exported Python and selected JSON/JSONL |
| Secret and identifier review | No unresolved findings in the selected publication set | See qualification below |

The 1807-case suite is not the development project's broader mixed-runtime acceptance run. No real platform operation, phone test, independent business review, or performance-improvement claim is inferred from it.

The secret scan used Gitleaks 8.30.1 and a separate review of resource IDs, personal paths, names, string literals, URLs, selected configuration examples and the exact file allowlist. Three scanner matches were reviewed as non-secret expressions: two SQL column assignments and a synthetic evaluation method call. No scanner can establish that all possible sensitive information is absent; future updates need a fresh review.

## Known issue retained for the development owner

`runtime/am-grok-realtime/tools/run_r2_demo.py` raises `KeyError: 'artifacts'` when it reads a final payload in the tested environment. The failure reproduces on both the original fixed baseline and the public copy. Publication work did not fix or suppress it. Use the verified `run_loopback_demo.py` commands in the [quickstart](quickstart.md); the older R2 script is retained for source transparency and should be repaired by the AM development project.

## Deployment limitations

The generic source has not been installed as a live public Feishu bot. Replacing placeholders and supplying a key is not sufficient evidence that identity, permission, source binding, recovery and write behavior are correct. A portable live-deployment procedure and independent installation evidence remain work for the AM development owner and release process.
