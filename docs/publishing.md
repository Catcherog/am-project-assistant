# Privacy and publication rules

Prepare public material in a clean staging directory using an explicit file allowlist. A copy of a development folder or its Git history is not a de-identified release.

## Material to exclude

- Credentials, authentication caches, local configuration, databases, logs, and model weights.
- Raw chats, customer or employee identifiers, tenant-specific resource links, internal project codes, or private filesystem paths.
- Screenshots, attachments, or office documents whose metadata or hidden content has not been examined.
- Private provenance maps, access-policy bindings, and records of actual approvals.
- Existing Git history until it has received its own disclosure review.

## Reconstruct examples safely

Use fictional identities and projects. Preserve the logical relationships needed to explain the problem: which evidence supports a claim, which proposal was approved, what changed, and what remained unknown. Replacing only a name may leave a uniquely identifying combination of dates, values, and events.

Label each example as synthetic, reconstructed with permission, or a real research observation. The examples in this preview are fully synthetic. Their proposed expectations are not independent human annotations.

## Review process

1. Record source ownership and intended public purpose privately.
2. Select only necessary materials and create a public version.
3. Check obvious identifier patterns, secrets, personal information, and contextual re-identification risks.
4. Review wording for unsupported capability, novelty, performance, and acceptance claims.
5. Inspect the exact staged diff and retained file list before publication.
6. Read back the remote files and compare their contents or hashes after publication.

The repository checker covers structure, local links, and several obvious sensitive patterns. It is not a complete secret detector or a privacy certification. A zero-finding result must be supplemented by content review.

## Code and license handling

This preview publishes the selected, de-identified first-party source and newly authored material under MIT. Unpublished internal material and third-party software are not relicensed by this repository. Before every source import, inspect provenance, notices, bundled dependencies, generated code, and any model or dataset terms.

For an authorized source export, identity and resource literals may be consistently replaced in an isolated public copy and its corresponding fixtures. Preserve the private transformation map, compare against the fixed baseline, and re-run affected checks. Product logic, features, and live configuration remain under the AM development owner's control. Record behavioral defects for that project instead of silently fixing them during publication.
