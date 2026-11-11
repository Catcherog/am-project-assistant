# Synthetic discussion cases

The [case file](project-cases.jsonl) contains twelve fully synthetic scenarios. Names, identifiers, dates, and business facts are invented. No case is a production transcript. These examples illustrate requirements and annotation questions; they are not a validated benchmark and contain no agent scores.

Each row has an ID, a scenario family, a user request, available evidence, a proposed expected behavior, and annotation status. The expectations were prepared with AI assistance and await independent review. Evaluators must not include `expected_behavior` in a model's input.

| ID | Topic |
| --- | --- |
| C01 | Planned activity is not completed activity |
| C02 | Completion without acceptance |
| C03 | Acceptance applies only to the specified batch |
| C04 | Approval bound to an earlier proposal |
| C05 | A supported, qualified progress answer |
| C06 | An unrelated edit need not invalidate a read-only answer |
| C07 | Revoked access before delivery |
| C08 | Ambiguous project identity |
| C09 | Duplicate delivery of the same request |
| C10 | Unknown outcome after a write timeout |
| C11 | HTTP success carrying an application error |
| C12 | A current approved action followed by readback |

## Reviewing a case

Check whether the evidence is sufficient, whether a different action is equally valid, and whether the proposed expectation requires information that is not supplied. Record disagreement before changing the expectation. Add a positive control when a family could otherwise be solved by always refusing.

Use separate versioned fields for independent annotations and adjudication in a future dataset. Channel variants and repeated attempts remain linked to their underlying scenario. Do not treat all twelve illustrative cases as twelve observed real-world incidents.

Run the repository structure check with `python3 tools/check_publication.py`. It verifies the published case format, not the semantic correctness of these proposed expectations.
