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

This preview publishes newly authored material under MIT. Unpublished runtime source and third-party material are not relicensed by this repository. Before source import, inspect file provenance, notices, bundled dependencies, generated code, and any model or dataset terms.

If removing an embedded business identifier requires changing runtime code, record the issue for the AM development owner. The publication maintainer does not change product behavior to make an export appear ready.
