# Updating the public source

Development continues in the AM project. Public updates are prepared from fixed revisions and submitted through a separate release branch and PR.

1. The development owner identifies a stable source revision, changed modules, dependencies and relevant verification evidence.
2. The publication maintainer reads the selected Git objects into private staging and records their provenance. Do not merge an unreviewed private branch or import its history into the public repository.
3. Apply the maintained private de-identification map to new and changed material. Review new names, identifiers, configuration values, CLI references, fixtures, paths and binary artifacts. Do not publish the map itself.
4. Preserve runtime logic and route product defects to the development owner. If a new transformation changes an interface, document it and obtain the necessary development review before release.
5. Run the applicable regression checks, compare outcomes with the same baseline environment, and retain failures. Recheck packaging and the documented offline commands when affected.
6. Review the exact public diff and allowlist, then push a sanitized release branch. Public PRs must never contain an intermediate unsanitized commit, even if a later commit removes the value.
7. Merge after the publication and development responsibilities are satisfied. Read back the resulting remote files and publish precise release notes.

For community functional PRs, the development owner decides whether and how to integrate the change into the working AM project. A public merge must not automatically deploy a production service.

Never resolve a failed check by omitting a test result, relabeling a synthetic evaluation as real, or changing a private value without recording the transformation. A reported gain must remain tied to the actual source and evaluation versions.
