# Source-release handoff checklist

The AM development owner supplies runtime inputs. The community maintainer prepares public documentation and de-identified examples. The following are release conditions, not claims of completion.

## Inputs from the development project

- [ ] Select a fixed, releasable commit and identify the exact included modules.
- [ ] Provide complete runtime dependencies, supported Python/platform versions, and required external services.
- [ ] Provide a configuration schema and a safe example that contains no real identifiers or credentials.
- [ ] Identify code, data, model, or bundled assets requiring third-party attribution or separate licenses.
- [ ] Demonstrate installation and startup from a clean checkout, without relying on private parent directories.
- [ ] Specify supported read and write operations, required permissions, and default behavior.
- [ ] Provide narrow reproductions for startup, a representative query, approval binding, and unknown-result handling.
- [ ] Distinguish deterministic checks, model-backed evaluation, platform tests, and independent business acceptance.

## Community publication work

- [ ] Complete the selected-file and history disclosure review.
- [ ] Map each public capability statement to evidence for the selected version.
- [ ] Produce installation and troubleshooting documentation from the verified procedure.
- [ ] Replace private examples with approved public or fully synthetic material.
- [ ] Verify packaging, relative links, notices, and issue-reporting routes.
- [ ] Ask an external participant to follow the documented procedure and record gaps.
- [ ] Publish a release with an exact source identity, limitations, and verification scope.

## Recording findings without changing the runtime

A handoff item should contain: observed behavior; expected behavior and source; affected version and entry point; reproduction conditions; actual evidence; potential impact; and a proposed acceptance check. Mark it as confirmed, suspected, or an evidence gap. Do not turn an untested concern into a reported production failure.

## Additional material from another computer

Request only the missing source or evidence: a file manifest and commit identity, dependency/configuration examples, clean-install notes, or a specific evaluation record. Do not request a full home directory, authentication store, or unfiltered chat archive.
