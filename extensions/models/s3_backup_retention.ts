// deno-lint-ignore-file no-explicit-any -- injected senders return command-specific SDK shapes.
import { z } from "npm:zod@4.0.17";
import {
  DeleteObjectCommand,
  GetBucketLifecycleConfigurationCommand,
  GetObjectCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  S3Client,
} from "npm:@aws-sdk/client-s3@3.864.0";

const DAY = 86_400_000;
const HOUR = 3_600_000;

const httpsUrl = z.url().refine(
  (value) => new URL(value).protocol === "https:",
  "endpoint must use HTTPS",
);
const secret = z.string().min(1).meta({ sensitive: true }).optional();

/** Global endpoint, policy, identity, and optional role credential settings. */
export const GlobalArgumentsSchema = z.strictObject({
  endpoint: httpsUrl,
  region: z.string().min(1).default("us-east-1"),
  sourceBucket: z.string().min(1),
  destinationBucket: z.string().min(1),
  sourcePrefix: z.string().default(""),
  destinationPrefix: z.string().default("objects/"),
  ledgerPrefix: z.string().default("retention/absence-ledger/"),
  planPrefix: z.string().default("retention/plans/"),
  maximumObjects: z.number().int().min(1).max(1_000_000).default(100_000),
  absenceDurationDays: z.number().int().min(1).default(31),
  minimumObservationGapHours: z.number().int().min(1).default(24),
  maximumObservationGapHours: z.number().int().min(1).default(36),
  maximumPlanAgeHours: z.number().int().min(1).max(8760).default(48),
  clockSkewSeconds: z.number().int().min(0).max(3600).default(300),
  providerMinimumRetentionDays: z.number().int().min(1).default(33),
  sourceWriterIdentityId: z.string().min(1),
  sourceReaderIdentityId: z.string().min(1),
  destinationWriterIdentityId: z.string().min(1),
  destinationPrunerIdentityId: z.string().min(1),
  lifecycleAuditorIdentityId: z.string().min(1),
  sourceReaderAccessKeyId: secret,
  sourceReaderSecretAccessKey: secret,
  destinationPrunerAccessKeyId: secret,
  destinationPrunerSecretAccessKey: secret,
  lifecycleAuditorAccessKeyId: secret,
  lifecycleAuditorSecretAccessKey: secret,
});

/** Parsed model configuration used by pure retention operations and adapters. */
export type GlobalArguments = z.infer<typeof GlobalArgumentsSchema>;

const CandidateSchema = z.strictObject({
  sourceKey: z.string(),
  destinationKey: z.string(),
});
const PlanSchema = z.strictObject({
  schemaVersion: z.literal(1),
  planId: z.string(),
  digest: z.string().regex(/^[a-f0-9]{64}$/),
  observedAt: z.iso.datetime(),
  sourceBucket: z.string(),
  destinationBucket: z.string(),
  scopeDigest: z.string().regex(/^[a-f0-9]{64}$/),
  ledgerGeneration: z.number().int().nonnegative(),
  ledgerKey: z.string(),
  ledgerDigest: z.string().regex(/^[a-f0-9]{64}$/),
  candidates: z.array(CandidateSchema),
});
const DeletionOutcomeSchema = z.strictObject({
  candidateIndex: z.number().int().nonnegative(),
  destinationKeyDigest: z.string().regex(/^[a-f0-9]{64}$/),
  status: z.enum([
    "source-present",
    "already-absent",
    "delete-request-succeeded",
    "uncertain-after-interrupted-attempt",
  ]),
});
const ReceiptSchema = z.strictObject({
  planId: z.string(),
  digest: z.string(),
  deleted: z.number().int().nonnegative(),
  alreadyAbsent: z.number().int().nonnegative(),
  sourcePresent: z.number().int().nonnegative(),
  uncertain: z.number().int().nonnegative(),
  outcomes: z.array(DeletionOutcomeSchema),
  replayed: z.boolean(),
  appliedAt: z.iso.datetime(),
});
const LifecycleSchema = z.strictObject({
  destinationBucket: z.string(),
  minimumRetentionDays: z.number().int().positive(),
  enabledExpirationRulesChecked: z.number().int().nonnegative(),
  verifiedAt: z.iso.datetime(),
});
const IsolationSchema = z.strictObject({
  sourceReaderDeniedDestination: z.literal(true),
  destinationPrunerDeniedSource: z.literal(true),
  verifiedAt: z.iso.datetime(),
});

interface Observation {
  firstAbsentAt: string;
  lastAbsentAt: string;
  count: number;
}
interface Ledger {
  schemaVersion: 1;
  generation: number;
  observedAt: string;
  observations: Record<string, Observation>;
}
/** Minimal injectable AWS SDK sender boundary; tests supply an in-memory fake. */
export interface S3Sender {
  send(command: unknown): Promise<any>;
  destroy?(): void;
}
/** Least-privilege source, destination, and optional lifecycle senders. */
export interface RetentionClients {
  source: S3Sender;
  destination: S3Sender;
  lifecycle?: S3Sender;
}

function validateConfiguration(args: GlobalArguments): void {
  if (args.sourceBucket === args.destinationBucket) {
    throw new Error("sourceBucket and destinationBucket must differ");
  }
  const ids = [
    args.sourceWriterIdentityId,
    args.sourceReaderIdentityId,
    args.destinationWriterIdentityId,
    args.destinationPrunerIdentityId,
    args.lifecycleAuditorIdentityId,
  ];
  if (new Set(ids).size !== ids.length) {
    throw new Error("all credential-role identity IDs must be distinct");
  }
  if (args.minimumObservationGapHours > args.maximumObservationGapHours) {
    throw new Error("minimum observation gap cannot exceed maximum gap");
  }
  const prefixes = [args.destinationPrefix, args.ledgerPrefix, args.planPrefix];
  if (
    new Set(prefixes).size !== prefixes.length ||
    prefixes.some((a, i) => prefixes.some((b, j) => i !== j && a.startsWith(b)))
  ) {
    throw new Error("destination, ledger, and plan prefixes must not overlap");
  }
}

function requirePair(
  args: GlobalArguments,
  id: keyof GlobalArguments,
  key: keyof GlobalArguments,
  label: string,
): [string, string] {
  const access = args[id];
  const value = args[key];
  if (typeof access !== "string" || typeof value !== "string") {
    throw new Error(`${label} credentials are required for this method`);
  }
  return [access, value];
}

function sender(
  args: GlobalArguments,
  credentials: [string, string],
): S3Sender {
  return new S3Client({
    endpoint: args.endpoint,
    region: args.region,
    forcePathStyle: true,
    credentials: {
      accessKeyId: credentials[0],
      secretAccessKey: credentials[1],
    },
    requestChecksumCalculation: "WHEN_REQUIRED",
    responseChecksumValidation: "WHEN_REQUIRED",
  });
}

function clients(args: GlobalArguments, lifecycle = false): RetentionClients {
  validateConfiguration(args);
  const result: RetentionClients = {
    source: sender(
      args,
      requirePair(
        args,
        "sourceReaderAccessKeyId",
        "sourceReaderSecretAccessKey",
        "source-reader",
      ),
    ),
    destination: sender(
      args,
      requirePair(
        args,
        "destinationPrunerAccessKeyId",
        "destinationPrunerSecretAccessKey",
        "destination-pruner",
      ),
    ),
  };
  if (lifecycle) {
    result.lifecycle = sender(
      args,
      requirePair(
        args,
        "lifecycleAuditorAccessKeyId",
        "lifecycleAuditorSecretAccessKey",
        "lifecycle-auditor",
      ),
    );
  }
  return result;
}

function destroyClients(s3: RetentionClients): void {
  s3.source.destroy?.();
  s3.destination.destroy?.();
  s3.lifecycle?.destroy?.();
}

function isNotFound(error: unknown): boolean {
  const e = error as { name?: string; $metadata?: { httpStatusCode?: number } };
  return ["NotFound", "NoSuchKey"].includes(e?.name ?? "") ||
    e?.$metadata?.httpStatusCode === 404;
}
function isPrecondition(error: unknown): boolean {
  const e = error as { name?: string; $metadata?: { httpStatusCode?: number } };
  return e?.name === "PreconditionFailed" ||
    e?.$metadata?.httpStatusCode === 412;
}
function isAccessDenied(error: unknown): boolean {
  return (error as { name?: string })?.name === "AccessDenied";
}
async function text(body: any): Promise<string> {
  if (typeof body === "string") return body;
  if (typeof body?.transformToString === "function") {
    return await body.transformToString();
  }
  throw new Error("S3 object body is not readable text");
}

async function list(
  sender: S3Sender,
  bucket: string,
  prefix: string,
  limit: number,
): Promise<string[]> {
  const keys: string[] = [];
  let token: string | undefined;
  do {
    const response = await sender.send(
      new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: prefix || undefined,
        ContinuationToken: token,
      }),
    );
    for (const item of response.Contents ?? []) {
      if (typeof item.Key !== "string") {
        throw new Error("S3 listing contained no key");
      }
      keys.push(item.Key);
      if (keys.length > limit) {
        throw new Error(`listing exceeded maximumObjects (${limit})`);
      }
    }
    token = response.IsTruncated ? response.NextContinuationToken : undefined;
    if (response.IsTruncated && !token) {
      throw new Error("truncated listing omitted continuation token");
    }
  } while (token);
  return keys;
}

function stable(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stable).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${
      Object.entries(value)
        .sort(([a], [b]) => a.localeCompare(b)).map(([k, v]) =>
          `${JSON.stringify(k)}:${stable(v)}`
        ).join(",")
    }}`;
  }
  return JSON.stringify(value);
}
async function sha256(value: unknown): Promise<string> {
  const bytes = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(stable(value)),
  );
  return [...new Uint8Array(bytes)].map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

function scopePayload(args: GlobalArguments) {
  return {
    sourceBucket: args.sourceBucket,
    destinationBucket: args.destinationBucket,
    sourcePrefix: args.sourcePrefix,
    destinationPrefix: args.destinationPrefix,
    ledgerPrefix: args.ledgerPrefix,
    planPrefix: args.planPrefix,
    absenceDurationDays: args.absenceDurationDays,
    minimumObservationGapHours: args.minimumObservationGapHours,
    maximumObservationGapHours: args.maximumObservationGapHours,
  };
}

/** Prove that each operational credential is denied access to the other bucket. */
export async function verifyCredentialIsolation(
  s3: RetentionClients,
  args: GlobalArguments,
  now = new Date(),
): Promise<z.infer<typeof IsolationSchema>> {
  validateConfiguration(args);
  const denied = async (client: S3Sender, bucket: string, label: string) => {
    try {
      await client.send(
        new ListObjectsV2Command({ Bucket: bucket, MaxKeys: 1 }),
      );
    } catch (error) {
      if (isAccessDenied(error)) return;
      throw new Error(`${label} isolation check did not return AccessDenied`);
    }
    throw new Error(`${label} can list the forbidden bucket`);
  };
  await denied(s3.source, args.destinationBucket, "source-reader");
  await denied(s3.destination, args.sourceBucket, "destination-pruner");
  return {
    sourceReaderDeniedDestination: true,
    destinationPrunerDeniedSource: true,
    verifiedAt: now.toISOString(),
  };
}
function parseDate(value: string, label: string): Date {
  const date = new Date(value);
  if (!z.iso.datetime().safeParse(value).success) {
    throw new Error(`invalid ${label}`);
  }
  return date;
}

/** Verify destination lifecycle expiration cannot undercut retention evidence. */
export async function verifyLifecycle(
  s3: S3Sender,
  args: GlobalArguments,
  now = new Date(),
): Promise<z.infer<typeof LifecycleSchema>> {
  validateConfiguration(args);
  const response = await s3.send(
    new GetBucketLifecycleConfigurationCommand({
      Bucket: args.destinationBucket,
    }),
  ).catch((error) => {
    if (isNotFound(error)) return { Rules: [] };
    throw error;
  });
  let checked = 0;
  for (const rule of response.Rules ?? []) {
    if (rule.Status !== "Enabled" || !rule.Expiration) continue;
    checked++;
    if (
      rule.Expiration.Date || !Number.isInteger(rule.Expiration.Days) ||
      rule.Expiration.Days < args.providerMinimumRetentionDays
    ) {
      throw new Error(
        `lifecycle rule ${
          rule.ID ?? "(unnamed)"
        } must retain objects for at least ${args.providerMinimumRetentionDays} days`,
      );
    }
  }
  return {
    destinationBucket: args.destinationBucket,
    minimumRetentionDays: args.providerMinimumRetentionDays,
    enabledExpirationRulesChecked: checked,
    verifiedAt: now.toISOString(),
  };
}

async function loadLatestLedger(
  s3: S3Sender,
  args: GlobalArguments,
  now: Date,
): Promise<Ledger> {
  const keys = await list(
    s3,
    args.destinationBucket,
    args.ledgerPrefix,
    args.maximumObjects,
  );
  const entries = keys.map((key) => {
    const suffix = key.slice(args.ledgerPrefix.length);
    const match = suffix.match(/^(\d{20})\.json$/);
    if (!match) throw new Error(`malformed ledger key: ${key}`);
    return { generation: Number(match[1]), key };
  }).sort((a, b) => a.generation - b.generation);
  entries.forEach((entry, index) => {
    if (entry.generation !== index) {
      throw new Error("ledger generations are incomplete");
    }
  });
  if (!entries.length) {
    return {
      schemaVersion: 1,
      generation: -1,
      observedAt: now.toISOString(),
      observations: Object.create(null),
    };
  }
  const latest = entries.at(-1)!;
  const response = await s3.send(
    new GetObjectCommand({ Bucket: args.destinationBucket, Key: latest.key }),
  );
  const ledger = JSON.parse(await text(response.Body)) as Ledger;
  if (
    ledger.schemaVersion !== 1 || ledger.generation !== latest.generation ||
    !ledger.observations || typeof ledger.observations !== "object"
  ) {
    throw new Error("invalid retention ledger");
  }
  const observed = parseDate(ledger.observedAt, "ledger observedAt");
  if (observed.valueOf() > now.valueOf() + args.clockSkewSeconds * 1000) {
    throw new Error("retention ledger is from the future");
  }
  return ledger;
}

async function putCreateOnce(
  s3: S3Sender,
  bucket: string,
  key: string,
  body: string,
): Promise<void> {
  try {
    await s3.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: body,
        ContentType: "application/json",
        IfNoneMatch: "*",
      }),
    );
  } catch (error) {
    if (!isPrecondition(error)) throw error;
    const existing = await s3.send(
      new GetObjectCommand({ Bucket: bucket, Key: key }),
    );
    if (await text(existing.Body) !== body) {
      throw new Error(`immutable object conflict at ${key}`);
    }
  }
}

async function loadJsonIfPresent(
  s3: S3Sender,
  bucket: string,
  key: string,
): Promise<unknown | undefined> {
  try {
    const response = await s3.send(
      new GetObjectCommand({ Bucket: bucket, Key: key }),
    );
    return JSON.parse(await text(response.Body));
  } catch (error) {
    if (isNotFound(error)) return undefined;
    throw error;
  }
}

const DeletionIntentSchema = z.strictObject({
  schemaVersion: z.literal(1),
  planId: z.string(),
  digest: z.string().regex(/^[a-f0-9]{64}$/),
  candidateIndex: z.number().int().nonnegative(),
  destinationKeyDigest: z.string().regex(/^[a-f0-9]{64}$/),
});

async function storeDeletionOutcome(
  s3: S3Sender,
  args: GlobalArguments,
  planId: string,
  outcome: z.infer<typeof DeletionOutcomeSchema>,
): Promise<void> {
  await putCreateOnce(
    s3,
    args.destinationBucket,
    `${args.planPrefix}${planId}.candidate-${
      String(outcome.candidateIndex).padStart(10, "0")
    }.result.json`,
    stable(outcome),
  );
}

async function loadPlanReplays(
  s3: S3Sender,
  args: GlobalArguments,
  observedAt: string,
  scopeDigest: string,
): Promise<z.infer<typeof PlanSchema>[]> {
  const keys = (await list(
    s3,
    args.destinationBucket,
    args.planPrefix,
    args.maximumObjects,
  )).filter((key) =>
    key.endsWith(".json") && !key.endsWith(".applied.json") &&
    !key.includes(".candidate-")
  );
  const matches: z.infer<typeof PlanSchema>[] = [];
  for (const key of keys) {
    const response = await s3.send(
      new GetObjectCommand({ Bucket: args.destinationBucket, Key: key }),
    );
    const plan = PlanSchema.parse(JSON.parse(await text(response.Body)));
    if (plan.observedAt !== observedAt) continue;
    if (plan.scopeDigest !== scopeDigest) {
      throw new Error("preview replay scope conflict");
    }
    matches.push(plan);
  }
  if (matches.length > 1) throw new Error("conflicting preview replay plans");
  return matches;
}

async function verifyBoundLedger(
  s3: S3Sender,
  args: GlobalArguments,
  plan: z.infer<typeof PlanSchema>,
  now: Date,
  requireLatest: boolean,
): Promise<void> {
  const keys = await list(
    s3,
    args.destinationBucket,
    args.ledgerPrefix,
    args.maximumObjects,
  );
  if (!keys.includes(plan.ledgerKey)) {
    throw new Error("bound ledger is missing");
  }
  const generations = keys.map((key) => {
    const match = key.slice(args.ledgerPrefix.length).match(/^(\d{20})\.json$/);
    if (!match) throw new Error(`malformed ledger key: ${key}`);
    return Number(match[1]);
  });
  if (requireLatest && Math.max(...generations) !== plan.ledgerGeneration) {
    throw new Error("a newer ledger invalidates this plan");
  }
  const body = await text(
    (await s3.send(
      new GetObjectCommand({
        Bucket: args.destinationBucket,
        Key: plan.ledgerKey,
      }),
    )).Body,
  );
  if (await sha256(JSON.parse(body)) !== plan.ledgerDigest) {
    throw new Error("bound ledger digest mismatch");
  }
  const ledger = JSON.parse(body) as Ledger;
  if (ledger.generation !== plan.ledgerGeneration) {
    throw new Error("bound ledger generation mismatch");
  }
  if (parseDate(ledger.observedAt, "ledger observedAt") > now) {
    throw new Error("bound ledger is from the future");
  }
}

/** Build and immutably store a deterministic deletion plan; never deletes. */
export async function previewRetention(
  s3: RetentionClients,
  args: GlobalArguments,
  observedAt: string,
): Promise<z.infer<typeof PlanSchema>> {
  validateConfiguration(args);
  const now = parseDate(observedAt, "observedAt");
  await verifyCredentialIsolation(s3, args, now);
  const scopeDigest = await sha256(scopePayload(args));
  const replays = await loadPlanReplays(
    s3.destination,
    args,
    observedAt,
    scopeDigest,
  );
  if (replays.length) {
    const existing = replays[0];
    const { digest: _digest, planId: _planId, ...payload } = existing;
    if (await sha256(payload) !== existing.digest) {
      throw new Error("preview replay plan digest mismatch");
    }
    await verifyBoundLedger(s3.destination, args, existing, now, false);
    return existing;
  }
  const sourceKeys = new Set(
    (await list(
      s3.source,
      args.sourceBucket,
      args.sourcePrefix,
      args.maximumObjects,
    )).map((key) => key.slice(args.sourcePrefix.length)),
  );
  const destinationKeys = (await list(
    s3.destination,
    args.destinationBucket,
    args.destinationPrefix,
    args.maximumObjects,
  )).map((key) => key.slice(args.destinationPrefix.length));
  const previous = await loadLatestLedger(s3.destination, args, now);
  const observations: Record<string, Observation> = Object.create(null);
  const candidates: z.infer<typeof CandidateSchema>[] = [];
  for (const key of destinationKeys.sort()) {
    if (sourceKeys.has(key)) continue;
    const prior = previous.observations[key];
    const priorLast = prior
      ? parseDate(prior.lastAbsentAt, "lastAbsentAt")
      : null;
    const gap = priorLast ? now.valueOf() - priorLast.valueOf() : 0;
    if (gap < 0) throw new Error("clock moved behind absence observation");
    const continuous = prior && gap <= args.maximumObservationGapHours * HOUR;
    const observation: Observation = {
      firstAbsentAt: continuous ? prior.firstAbsentAt : observedAt,
      lastAbsentAt: observedAt,
      count: continuous ? prior.count + 1 : 1,
    };
    observations[key] = observation;
    const age = now.valueOf() -
      parseDate(observation.firstAbsentAt, "firstAbsentAt").valueOf();
    if (
      observation.count >= 2 && gap >= args.minimumObservationGapHours * HOUR &&
      age >= args.absenceDurationDays * DAY
    ) {
      candidates.push({
        sourceKey: `${args.sourcePrefix}${key}`,
        destinationKey: `${args.destinationPrefix}${key}`,
      });
    }
  }
  const generation = previous.generation + 1;
  const ledger: Ledger = {
    schemaVersion: 1,
    generation,
    observedAt,
    observations,
  };
  const ledgerKey = `${args.ledgerPrefix}${
    String(generation).padStart(20, "0")
  }.json`;
  const ledgerDigest = await sha256(ledger);
  await putCreateOnce(
    s3.destination,
    args.destinationBucket,
    ledgerKey,
    stable(ledger),
  );
  const payload = {
    schemaVersion: 1 as const,
    observedAt,
    sourceBucket: args.sourceBucket,
    destinationBucket: args.destinationBucket,
    scopeDigest,
    ledgerGeneration: generation,
    ledgerKey,
    ledgerDigest,
    candidates,
  };
  const digest = await sha256(payload);
  const planId = `${observedAt.replaceAll(/[^0-9]/g, "")}-${
    digest.slice(0, 16)
  }`;
  const plan = { ...payload, planId, digest };
  await putCreateOnce(
    s3.destination,
    args.destinationBucket,
    `${args.planPrefix}${planId}.json`,
    stable(plan),
  );
  return plan;
}

/** Apply exactly one immutable digest-confirmed plan, deleting destination only. */
export async function applyRetention(
  s3: RetentionClients,
  args: GlobalArguments,
  planId: string,
  digest: string,
  confirmation = "",
  now = new Date(),
): Promise<z.infer<typeof ReceiptSchema>> {
  validateConfiguration(args);
  if (!/^[a-f0-9]{64}$/.test(digest) || confirmation !== `APPLY ${digest}`) {
    throw new Error(
      "exact digest-bound confirmation is required; nothing deleted",
    );
  }
  if (!s3.lifecycle) throw new Error("lifecycle-auditor client is required");
  await verifyCredentialIsolation(s3, args, now);
  await verifyLifecycle(s3.lifecycle, args, now);
  const planResponse = await s3.destination.send(
    new GetObjectCommand({
      Bucket: args.destinationBucket,
      Key: `${args.planPrefix}${planId}.json`,
    }),
  );
  const plan = PlanSchema.parse(JSON.parse(await text(planResponse.Body)));
  if (plan.planId !== planId || plan.digest !== digest) {
    throw new Error("plan identity or digest mismatch");
  }
  const payload = {
    schemaVersion: plan.schemaVersion,
    observedAt: plan.observedAt,
    sourceBucket: plan.sourceBucket,
    destinationBucket: plan.destinationBucket,
    scopeDigest: plan.scopeDigest,
    ledgerGeneration: plan.ledgerGeneration,
    ledgerKey: plan.ledgerKey,
    ledgerDigest: plan.ledgerDigest,
    candidates: plan.candidates,
  };
  if (
    await sha256(payload) !== digest ||
    plan.sourceBucket !== args.sourceBucket ||
    plan.destinationBucket !== args.destinationBucket
  ) throw new Error("stored plan digest or scope mismatch");
  if (plan.scopeDigest !== await sha256(scopePayload(args))) {
    throw new Error("stored plan configuration scope mismatch");
  }
  const age = now.valueOf() -
    parseDate(plan.observedAt, "plan observedAt").valueOf();
  if (age < 0 || age > args.maximumPlanAgeHours * HOUR) {
    throw new Error("plan is stale or from the future");
  }
  await verifyBoundLedger(s3.destination, args, plan, now, true);
  const receiptKey = `${args.planPrefix}${planId}.applied.json`;
  try {
    const existing = ReceiptSchema.parse(
      JSON.parse(
        await text(
          (await s3.destination.send(
            new GetObjectCommand({
              Bucket: args.destinationBucket,
              Key: receiptKey,
            }),
          )).Body,
        ),
      ),
    );
    if (existing.planId !== planId || existing.digest !== digest) {
      throw new Error("apply receipt identity or digest mismatch");
    }
    return { ...existing, replayed: true };
  } catch (error) {
    if (!isNotFound(error)) throw error;
  }
  const outcomes: z.infer<typeof DeletionOutcomeSchema>[] = [];
  for (const [candidateIndex, candidate] of plan.candidates.entries()) {
    const destinationKeyDigest = await sha256(candidate.destinationKey);
    const resultKey = `${args.planPrefix}${planId}.candidate-${
      String(candidateIndex).padStart(10, "0")
    }.result.json`;
    const existingOutcome = await loadJsonIfPresent(
      s3.destination,
      args.destinationBucket,
      resultKey,
    );
    if (existingOutcome !== undefined) {
      const parsed = DeletionOutcomeSchema.parse(existingOutcome);
      if (
        parsed.candidateIndex !== candidateIndex ||
        parsed.destinationKeyDigest !== destinationKeyDigest
      ) {
        throw new Error(`candidate ${candidateIndex} outcome binding mismatch`);
      }
      outcomes.push(parsed);
      continue;
    }
    try {
      await s3.source.send(
        new HeadObjectCommand({
          Bucket: args.sourceBucket,
          Key: candidate.sourceKey,
        }),
      );
      const outcome = {
        candidateIndex,
        destinationKeyDigest,
        status: "source-present" as const,
      };
      await storeDeletionOutcome(
        s3.destination,
        args,
        planId,
        outcome,
      );
      outcomes.push(outcome);
      continue;
    } catch (error) {
      if (!isNotFound(error)) throw error;
    }

    const intentKey = `${args.planPrefix}${planId}.candidate-${
      String(candidateIndex).padStart(10, "0")
    }.intent.json`;
    const expectedIntent = {
      schemaVersion: 1 as const,
      planId,
      digest,
      candidateIndex,
      destinationKeyDigest,
    };
    const existingIntent = await loadJsonIfPresent(
      s3.destination,
      args.destinationBucket,
      intentKey,
    );
    const interrupted = existingIntent !== undefined;
    if (interrupted) {
      const intent = DeletionIntentSchema.parse(existingIntent);
      if (stable(intent) !== stable(expectedIntent)) {
        throw new Error(`candidate ${candidateIndex} intent binding mismatch`);
      }
    } else {
      await putCreateOnce(
        s3.destination,
        args.destinationBucket,
        intentKey,
        stable(expectedIntent),
      );
    }

    try {
      await s3.destination.send(
        new HeadObjectCommand({
          Bucket: args.destinationBucket,
          Key: candidate.destinationKey,
        }),
      );
    } catch (error) {
      if (!isNotFound(error)) throw error;
      const outcome = {
        candidateIndex,
        destinationKeyDigest,
        status: interrupted
          ? "uncertain-after-interrupted-attempt" as const
          : "already-absent" as const,
      };
      await storeDeletionOutcome(
        s3.destination,
        args,
        planId,
        outcome,
      );
      outcomes.push(outcome);
      continue;
    }

    await s3.destination.send(
      new DeleteObjectCommand({
        Bucket: args.destinationBucket,
        Key: candidate.destinationKey,
      }),
    );
    const outcome = {
      candidateIndex,
      destinationKeyDigest,
      status: "delete-request-succeeded" as const,
    };
    await storeDeletionOutcome(s3.destination, args, planId, outcome);
    outcomes.push(outcome);
  }
  const receipt = {
    planId,
    digest,
    deleted:
      outcomes.filter((outcome) =>
        outcome.status === "delete-request-succeeded"
      ).length,
    alreadyAbsent:
      outcomes.filter((outcome) => outcome.status === "already-absent").length,
    sourcePresent:
      outcomes.filter((outcome) => outcome.status === "source-present").length,
    uncertain:
      outcomes.filter((outcome) =>
        outcome.status === "uncertain-after-interrupted-attempt"
      ).length,
    outcomes,
    replayed: false,
    appliedAt: now.toISOString(),
  };
  await putCreateOnce(
    s3.destination,
    args.destinationBucket,
    receiptKey,
    stable(receipt),
  );
  return receipt;
}

interface Context {
  globalArgs: GlobalArguments;
  writeResource(spec: string, name: string, data: unknown): Promise<any>;
  logger: {
    info(message: string, attributes?: Record<string, unknown>): void;
  };
}

type RetentionClientsFactory = (
  args: GlobalArguments,
  lifecycle?: boolean,
) => RetentionClients;
type SenderFactory = (
  args: GlobalArguments,
  credentials: [string, string],
) => S3Sender;

/** Normalize replay-only metadata before writing the immutable method resource. */
export function canonicalReceiptResource(
  receipt: z.infer<typeof ReceiptSchema>,
): z.infer<typeof ReceiptSchema> {
  return { ...receipt, replayed: false };
}

/** Build the model with injectable S3 boundaries for contract-level tests. */
export function createModel(
  clientFactory: RetentionClientsFactory = clients,
  senderFactory: SenderFactory = sender,
) {
  return {
    type: "@mgreten/s3-backup-retention",
    version: "2026.08.28.1",
    globalArguments: GlobalArgumentsSchema,
    checks: {
      "valid-retention-policy": {
        description:
          "Validate bucket, identity, prefix, and retention policy relationships",
        labels: ["policy"],
        appliesTo: ["applyRetention"],
        execute: (context: { globalArgs: GlobalArguments }) => {
          try {
            validateConfiguration(context.globalArgs);
            if (
              context.globalArgs.providerMinimumRetentionDays <
                context.globalArgs.absenceDurationDays +
                  Math.ceil(context.globalArgs.maximumPlanAgeHours / 24)
            ) {
              throw new Error(
                "providerMinimumRetentionDays must cover absenceDurationDays plus maximumPlanAgeHours",
              );
            }
            return { pass: true };
          } catch (error) {
            return {
              pass: false,
              errors: [error instanceof Error ? error.message : String(error)],
            };
          }
        },
      },
      "live-retention-authority": {
        description:
          "Verify cross-bucket credential isolation and destination lifecycle policy",
        labels: ["live"],
        appliesTo: ["applyRetention"],
        execute: async (context: { globalArgs: GlobalArguments }) => {
          const s3 = clientFactory(context.globalArgs, true);
          try {
            if (!s3.lifecycle) {
              throw new Error("lifecycle-auditor client is required");
            }
            await verifyCredentialIsolation(s3, context.globalArgs);
            await verifyLifecycle(s3.lifecycle, context.globalArgs);
            return { pass: true };
          } catch (error) {
            return {
              pass: false,
              errors: [error instanceof Error ? error.message : String(error)],
            };
          } finally {
            destroyClients(s3);
          }
        },
      },
    },
    resources: {
      retentionPlan: {
        description: "Immutable deterministic retention preview",
        schema: PlanSchema,
        lifetime: "infinite" as const,
        garbageCollection: 100,
      },
      applyReceipt: {
        description: "Digest-bound retention apply receipt",
        schema: ReceiptSchema,
        lifetime: "infinite" as const,
        garbageCollection: 100,
      },
      lifecycleVerification: {
        description: "Provider lifecycle retention proof",
        schema: LifecycleSchema,
        lifetime: "30d" as const,
        garbageCollection: 100,
      },
      credentialIsolationVerification: {
        description: "Cross-bucket AccessDenied credential-isolation proof",
        schema: IsolationSchema,
        lifetime: "24h" as const,
        garbageCollection: 100,
      },
    },
    methods: {
      verifyCredentialIsolation: {
        description:
          "Verify operational credentials cannot list the other bucket",
        arguments: z.strictObject({}),
        execute: async (_: Record<string, never>, context: Context) => {
          context.logger.info("Credential isolation verification started", {});
          const s3 = clientFactory(context.globalArgs);
          try {
            const output = await verifyCredentialIsolation(
              s3,
              context.globalArgs,
            );
            const result = {
              dataHandles: [
                await context.writeResource(
                  "credentialIsolationVerification",
                  "credential-isolation-current",
                  output,
                ),
              ],
            };
            context.logger.info(
              "Credential isolation verification completed",
              {},
            );
            return result;
          } finally {
            destroyClients(s3);
          }
        },
      },
      verifyLifecycle: {
        description:
          "Verify destination lifecycle with auditor-only credentials",
        arguments: z.strictObject({}),
        execute: async (_: Record<string, never>, context: Context) => {
          context.logger.info("Lifecycle verification started", {});
          validateConfiguration(context.globalArgs);
          const audit = senderFactory(
            context.globalArgs,
            requirePair(
              context.globalArgs,
              "lifecycleAuditorAccessKeyId",
              "lifecycleAuditorSecretAccessKey",
              "lifecycle-auditor",
            ),
          );
          try {
            const output = await verifyLifecycle(audit, context.globalArgs);
            const result = {
              dataHandles: [
                await context.writeResource(
                  "lifecycleVerification",
                  "lifecycle-verification-current",
                  output,
                ),
              ],
            };
            context.logger.info("Lifecycle verification completed", {
              enabledExpirationRulesChecked:
                output.enabledExpirationRulesChecked,
            });
            return result;
          } finally {
            audit.destroy?.();
          }
        },
      },
      previewRetention: {
        description: "Create or verify an immutable no-delete retention plan",
        arguments: z.strictObject({ observedAt: z.iso.datetime() }),
        execute: async (
          input: { observedAt: string },
          context: Context,
        ) => {
          context.logger.info("Retention preview started", {
            observedAt: input.observedAt,
          });
          const s3 = clientFactory(context.globalArgs);
          try {
            const output = await previewRetention(
              s3,
              context.globalArgs,
              input.observedAt,
            );
            const result = {
              dataHandles: [
                await context.writeResource(
                  "retentionPlan",
                  `retention-plan-${output.planId}`,
                  output,
                ),
              ],
            };
            context.logger.info("Retention preview completed", {
              planId: output.planId,
              candidateCount: output.candidates.length,
            });
            return result;
          } finally {
            destroyClients(s3);
          }
        },
      },
      applyRetention: {
        description:
          "Delete destination objects in one exactly confirmed immutable plan",
        arguments: z.strictObject({
          planId: z.string().min(1),
          digest: z.string().regex(/^[a-f0-9]{64}$/),
          confirmation: z.string().default(""),
        }),
        execute: async (
          input: { planId: string; digest: string; confirmation: string },
          context: Context,
        ) => {
          context.logger.info("Retention apply started", {
            planId: input.planId,
          });
          const s3 = clientFactory(context.globalArgs, true);
          try {
            const output = await applyRetention(
              s3,
              context.globalArgs,
              input.planId,
              input.digest,
              input.confirmation,
            );
            const result = {
              dataHandles: [
                await context.writeResource(
                  "applyReceipt",
                  `apply-receipt-${output.planId}`,
                  canonicalReceiptResource(output),
                ),
              ],
            };
            context.logger.info("Retention apply completed", {
              planId: output.planId,
              deleted: output.deleted,
              alreadyAbsent: output.alreadyAbsent,
              sourcePresent: output.sourcePresent,
              uncertain: output.uncertain,
              replayed: output.replayed,
            });
            return result;
          } finally {
            destroyClients(s3);
          }
        },
      },
    },
  };
}

const defaultModel = createModel();

/** Swamp model definition. */
export const model = {
  type: "@mgreten/s3-backup-retention",
  version: "2026.08.28.1",
  globalArguments: GlobalArgumentsSchema,
  checks: defaultModel.checks,
  resources: defaultModel.resources,
  methods: defaultModel.methods,
};
