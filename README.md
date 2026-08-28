# S3 Backup Retention for Swamp

`@mgreten/s3-backup-retention` applies conservative retention to a destination
bucket that mirrors a source bucket. It supports any S3-compatible HTTPS
endpoint and never exposes a source-delete operation.

## Safety protocol

1. `verifyLifecycle` proves enabled provider expiration rules retain objects for
   at least `providerMinimumRetentionDays`.
2. `previewRetention` performs bounded, fully paginated listings, advances an
   append-only two-observation absence ledger, and create-once stores a
   deterministic plan under the destination `planPrefix`. Before any read or
   write it verifies that each operational credential receives `AccessDenied`
   when listing the other credential's bucket. It never deletes.
3. `applyRetention` requires `confirmation` to equal `APPLY <plan digest>`.
   Missing, default, stale, or altered confirmation fails before deletion. It
   verifies the stored plan and digest, rechecks isolation and lifecycle, and
   HEAD-checks each source and destination key before deleting destination keys
   only. Swamp also exposes policy and live-authority preflight checks for this
   method.
4. Repeating preview with the same inputs verifies the existing immutable plan.
   Repeating apply verifies the immutable receipt and returns it without more
   deletes.

Plan and ledger writes use `If-None-Match: *`. Listings stop if `maximumObjects`
is exceeded. A candidate requires two observations separated by at least
`minimumObservationGapHours`, continuous absence no longer than
`maximumObservationGapHours`, and total absence of at least
`absenceDurationDays`. Future ledger timestamps beyond `clockSkewSeconds` fail
closed.

Each plan binds the exact ledger generation, object key, and SHA-256 digest that
established continuous absence. Apply requires that immutable object to remain
present and unchanged. The conservative policy is that **any newer ledger
invalidates an older plan**, even if it would make the same deletion decision.
Apply also rejects plans older than bounded `maximumPlanAgeHours`.

Before each destination delete, the model create-once stores a candidate intent.
It then create-once stores the candidate outcome before the final receipt. A
receipt-write retry reuses those outcomes and does not repeat completed deletes.
If execution stops after the provider accepted a delete but before its outcome
was stored, retry records that candidate as uncertain rather than falsely
claiming a new deletion. Receipts report deleted, already-absent,
source-present, and uncertain counts and bind every outcome to the plan by
candidate index and destination-key digest.

Run retention and source restoration through one serialized workflow or other
exclusive coordination domain. A source object restored after its final HEAD
check but before the destination delete is outside the model's lock boundary. On
versioned S3 buckets, `DeleteObject` may create a delete marker rather than
removing historical object versions; receipt counts describe successful current
object delete requests, not deletion of all stored versions.

## Credentials and roles

Configure five distinct identity IDs: source writer, source reader, destination
writer, destination pruner, and lifecycle auditor. The source writer is identity
evidence only; this model never accepts its secret or writes/deletes source
objects. Secrets are optional global arguments and are required only by the
method that needs them, so use separate model instances:

- preview: source-reader + destination-pruner credentials
- apply: source-reader + destination-pruner + lifecycle-auditor credentials
- lifecycle audit: lifecycle-auditor credentials only

`verifyCredentialIsolation` provides a typed read-only proof. Only an AWS
`AccessDenied` error counts as denial: a 404, connectivity error, or any other
failure fails closed. The lifecycle-auditor client is constructed only for and
used only by lifecycle configuration verification.

The destination writer identity is reserved for a separate copy process and is
checked for non-reuse; this retention model never needs its secret.

Methods emit structured entry and completion logs without credentials or object
keys. Exact candidates remain available in the immutable plan; operational logs
use only plan IDs and aggregate counts.

Example configuration fields: `endpoint`, `region`, `sourceBucket`,
`destinationBucket`, `sourcePrefix`, `destinationPrefix`, `ledgerPrefix`,
`planPrefix`, `maximumObjects`, retention timing values, the five identity IDs,
and the role-specific access-key/secret pairs. All secrets are marked sensitive.

An apply invocation deliberately repeats the plan digest:

```text
planId=<preview planId>
digest=<preview digest>
confirmation=APPLY <preview digest>
```

## Development

Tests inject `S3Sender` fakes and make no network calls.

```sh
~/.swamp/deno/deno fmt --check
~/.swamp/deno/deno check extensions/models/*.ts
~/.swamp/deno/deno lint
~/.swamp/deno/deno test --allow-env=SMITHY_NEW_RETRIES_2026 extensions/models/s3_backup_retention_test.ts
swamp extension fmt manifest.yaml --check
swamp extension quality manifest.yaml --json
swamp extension push manifest.yaml --dry-run
```
