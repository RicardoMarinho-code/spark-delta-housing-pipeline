# Privacy by design (LGPD)

The pipeline processes personal data of people in vulnerable situations, including sensitive data (race/color, disability, indigenous or quilombola origin). The measures below live in the code, not only in this document.

## Legal basis and purpose

Processing by the public administration to carry out a public housing policy (LGPD art. 7, III, and art. 11, II, "b" for sensitive data; art. 23). The purpose limits what is kept: targeting, monitoring and planning of housing policy. **The actual legal basis of any real use must be confirmed by the agency's data protection officer (DPO)**; this repository only demonstrates the technical measures.

## Technical measures

| Principle / article | Measure | Where |
|---|---|---|
| Necessity and minimization (art. 6, III) | Columns not needed for the purpose never leave bronze: names, address, civil documents, the interviewer's free text, power bill account, community codes | [layouts.py](../src/housing_lakehouse/layouts.py) |
| Pseudonymization (art. 13, §4) | CPF, NIS and family/person codes become HMAC-SHA256 with a Key Vault key and domain separation. The same CPF yields the same pseudonym in every dataset, which enables the join without exposing the document | [pseudonymization.py](../src/housing_lakehouse/governance/pseudonymization.py) |
| Generalization | ZIP code → first 5 digits; birth date → age | [policy.py](../src/housing_lakehouse/governance/policy.py) |
| Sensitive data (art. 11) | Only the needed flag is kept (e.g. indigenous family yes/no), labeled `sensitive` in the catalog and in the Unity Catalog tags | [catalog.py](../src/housing_lakehouse/governance/catalog.py) |
| Security (art. 46) | Layers in separate containers; storage without shared keys (Entra ID only); pseudonymization key in Key Vault with access auditing; quarantine without raw lines | [main.tf](../infra/terraform/main.tf) |
| Deletion (art. 15 and 16) | Bronze deleted after 3 months (`housing retention` + VACUUM); landing after 90 days through a storage policy | [bronze.py](../src/housing_lakehouse/layers/bronze.py) |
| Publishing aggregates | Counts from 1 to 4 suppressed, together with the rates derived from them | [disclosure.py](../src/housing_lakehouse/governance/disclosure.py) |
| Accountability (art. 37) | Every run audited; catalog generated from the code and checked in CI | [observability.py](../src/housing_lakehouse/observability.py), [ci.yml](../.github/workflows/ci.yml) |

The full per-column catalog is in [lgpd_catalog.md](lgpd_catalog.md).

## Tests that protect the policy

- No column of category `identifier` may have an action other than drop or pseudonymize ([test_catalog.py](../tests/test_catalog.py)).
- The CPF uses the same pseudonymization domain in CadÚnico and in the beneficiary files; otherwise the join would break silently.
- The end-to-end test scans **every text column** of silver, quarantine and gold for names and CPFs taken from bronze ([test_pipeline.py](../tests/test_pipeline.py)).
- CI fails if `docs/lgpd_catalog.md` does not match the layouts.

## Controlled re-identification

The pseudonym is one-way: only someone holding both the original data **and** the key can recompute a CPF's pseudonym and find the record. That path exists so an authorized team can check `review_flags`, and every read of the key is logged in Key Vault.

## Known limitations

- Disclosure control is primary only: published totals may allow recomputing a suppressed cell by differencing. Complementary suppression is the next step.
- Rotating the key changes every pseudonym. Rotation requires reprocessing silver and is a planned procedure, which is why Terraform ignores changes to the secret value.
- MCMV income bands and CadÚnico dictionary codes are configuration and must be checked against the current regulations.
