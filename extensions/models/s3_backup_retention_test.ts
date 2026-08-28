// deno-lint-ignore-file no-explicit-any -- command fakes intentionally accept SDK unions.
import { assertEquals, assertRejects } from "jsr:@std/assert@1.0.19";
import { createModelTestContext } from "jsr:@swamp-club/swamp-testing@0.20260824.32";
import {
  applyRetention,
  canonicalReceiptResource,
  createModel,
  type GlobalArguments,
  previewRetention,
  type RetentionClients,
  type S3Sender,
  verifyCredentialIsolation,
  verifyLifecycle,
} from "./s3_backup_retention.ts";

const awsError = (name: string, status?: number) =>
  Object.assign(new Error(name), {
    name,
    $metadata: { httpStatusCode: status },
  });

class Store {
  source = new Set<string>();
  destination = new Map<string, string>();
  deletes: string[] = [];
  failReceiptOnce = false;
  failCandidateResultOnce = false;
  pageSize?: number;
  omitContinuationToken = false;
  lifecycleRules: any[] = [];
}

class Fake implements S3Sender {
  commands: any[] = [];
  constructor(
    private store: Store,
    private role: "source" | "destination" | "lifecycle",
    private forbidden: "denied" | "allowed" | "missing" = "denied",
  ) {}

  send(command: any): Promise<any> {
    this.commands.push(command);
    const name = command.constructor.name;
    const input = command.input;
    if (name === "GetBucketLifecycleConfigurationCommand") {
      return Promise.resolve({ Rules: this.store.lifecycleRules });
    }
    if (name === "ListObjectsV2Command") {
      const forbidden =
        (this.role === "source" && input.Bucket === "destination") ||
        (this.role === "destination" && input.Bucket === "source");
      if (forbidden && this.forbidden !== "allowed") {
        return Promise.reject(
          this.forbidden === "denied"
            ? awsError("AccessDenied", 403)
            : awsError("NoSuchBucket", 404),
        );
      }
      const keys = input.Bucket === "source"
        ? [...this.store.source]
        : [...this.store.destination.keys()];
      const matching = keys.filter((key) =>
        !input.Prefix || key.startsWith(input.Prefix)
      );
      const start = Number(input.ContinuationToken ?? 0);
      const end = this.store.pageSize
        ? start + this.store.pageSize
        : matching.length;
      const truncated = end < matching.length;
      return Promise.resolve({
        Contents: matching.slice(start, end)
          .map((Key) => ({ Key })),
        IsTruncated: truncated,
        NextContinuationToken: truncated && !this.store.omitContinuationToken
          ? String(end)
          : undefined,
      });
    }
    if (name === "GetObjectCommand") {
      const body = this.store.destination.get(input.Key);
      return body === undefined
        ? Promise.reject(awsError("NoSuchKey", 404))
        : Promise.resolve({ Body: body });
    }
    if (name === "PutObjectCommand") {
      if (this.store.failReceiptOnce && input.Key.endsWith(".applied.json")) {
        this.store.failReceiptOnce = false;
        return Promise.reject(new Error("simulated crash"));
      }
      if (
        this.store.failCandidateResultOnce &&
        input.Key.endsWith(".result.json")
      ) {
        this.store.failCandidateResultOnce = false;
        return Promise.reject(new Error("simulated result-write crash"));
      }
      if (this.store.destination.has(input.Key) && input.IfNoneMatch === "*") {
        return Promise.reject(awsError("PreconditionFailed", 412));
      }
      this.store.destination.set(input.Key, input.Body);
      return Promise.resolve({});
    }
    if (name === "HeadObjectCommand") {
      const exists = input.Bucket === "source"
        ? this.store.source.has(input.Key)
        : this.store.destination.has(input.Key);
      return exists
        ? Promise.resolve({})
        : Promise.reject(awsError("NoSuchKey", 404));
    }
    if (name === "DeleteObjectCommand") {
      this.store.deletes.push(input.Key);
      this.store.destination.delete(input.Key);
      return Promise.resolve({});
    }
    return Promise.reject(new Error(`unexpected ${name}`));
  }
}

const base = (): GlobalArguments => ({
  endpoint: "https://example.invalid",
  region: "auto",
  sourceBucket: "source",
  destinationBucket: "destination",
  sourcePrefix: "media/",
  destinationPrefix: "objects/",
  ledgerPrefix: "retention/ledger/",
  planPrefix: "retention/plans/",
  maximumObjects: 100,
  absenceDurationDays: 1,
  minimumObservationGapHours: 24,
  maximumObservationGapHours: 36,
  maximumPlanAgeHours: 48,
  clockSkewSeconds: 300,
  providerMinimumRetentionDays: 32,
  sourceWriterIdentityId: "source-writer",
  sourceReaderIdentityId: "source-reader",
  destinationWriterIdentityId: "destination-writer",
  destinationPrunerIdentityId: "destination-pruner",
  lifecycleAuditorIdentityId: "lifecycle-auditor",
});

function clients(
  store: Store,
  forbidden: "denied" | "allowed" | "missing" = "denied",
): RetentionClients {
  return {
    source: new Fake(store, "source", forbidden),
    destination: new Fake(store, "destination", forbidden),
    lifecycle: new Fake(store, "lifecycle"),
  };
}

async function mature(store = new Store()) {
  store.destination.set("objects/gone.jpg", "backup");
  await previewRetention(clients(store), base(), "2026-08-27T00:00:00.000Z");
  const plan = await previewRetention(
    clients(store),
    base(),
    "2026-08-28T00:00:00.000Z",
  );
  return { store, plan };
}

Deno.test("isolation accepts AccessDenied only and preview fails before ledger mutation", async () => {
  const good = new Store();
  assertEquals(
    (await verifyCredentialIsolation(clients(good), base()))
      .sourceReaderDeniedDestination,
    true,
  );
  for (const mode of ["allowed", "missing"] as const) {
    const store = new Store();
    store.destination.set("objects/a", "x");
    await assertRejects(
      () =>
        previewRetention(
          clients(store, mode),
          base(),
          "2026-08-28T00:00:00.000Z",
        ),
      Error,
      mode === "allowed" ? "can list" : "did not return AccessDenied",
    );
    assertEquals(
      [...store.destination.keys()].some((key) => key.includes("ledger")),
      false,
    );
  }
});

Deno.test("exact preview replay returns existing plan without advancing ledger", async () => {
  const { store, plan } = await mature();
  const ledgerCount =
    [...store.destination.keys()].filter((key) => key.includes("ledger/"))
      .length;
  const replay = await previewRetention(
    clients(store),
    base(),
    plan.observedAt,
  );
  assertEquals(replay, plan);
  assertEquals(
    [...store.destination.keys()].filter((key) => key.includes("ledger/"))
      .length,
    ledgerCount,
  );
});

Deno.test("conflicting preview replay fails closed", async () => {
  const { store, plan } = await mature();
  const key = `retention/plans/${plan.planId}.json`;
  store.destination.set(
    key,
    store.destination.get(key)!.replace(plan.scopeDigest, "f".repeat(64)),
  );
  await assertRejects(
    () => previewRetention(clients(store), base(), plan.observedAt),
    Error,
    "scope conflict",
  );
});

Deno.test("apply rejects stale plan", async () => {
  const { store, plan } = await mature();
  await assertRejects(
    () =>
      applyRetention(
        clients(store),
        base(),
        plan.planId,
        plan.digest,
        `APPLY ${plan.digest}`,
        new Date("2026-08-31T00:00:01Z"),
      ),
    Error,
    "stale",
  );
  assertEquals(store.deletes, []);
});

Deno.test("apply skips a source object that reappeared", async () => {
  const { store, plan } = await mature();
  store.source.add("media/gone.jpg");
  const result = await applyRetention(
    clients(store),
    base(),
    plan.planId,
    plan.digest,
    `APPLY ${plan.digest}`,
    new Date("2026-08-28T01:00:00Z"),
  );
  assertEquals(result.deleted, 0);
  assertEquals(store.deletes, []);
});

Deno.test("apply rejects changed or wrongly bound ledger", async () => {
  const { store, plan } = await mature();
  store.destination.set(
    plan.ledgerKey,
    store.destination.get(plan.ledgerKey)!.replace('"count":2', '"count":3'),
  );
  await assertRejects(
    () =>
      applyRetention(
        clients(store),
        base(),
        plan.planId,
        plan.digest,
        `APPLY ${plan.digest}`,
        new Date("2026-08-28T01:00:00Z"),
      ),
    Error,
    "ledger digest mismatch",
  );
});

Deno.test("apply rejects a missing bound ledger", async () => {
  const { store, plan } = await mature();
  store.destination.delete(plan.ledgerKey);
  await assertRejects(
    () =>
      applyRetention(
        clients(store),
        base(),
        plan.planId,
        plan.digest,
        `APPLY ${plan.digest}`,
        new Date("2026-08-28T01:00:00Z"),
      ),
    Error,
    "bound ledger is missing",
  );
});

Deno.test("a newer ledger invalidates an older plan", async () => {
  const { store, plan } = await mature();
  await previewRetention(clients(store), base(), "2026-08-29T00:00:00.000Z");
  await assertRejects(
    () =>
      applyRetention(
        clients(store),
        base(),
        plan.planId,
        plan.digest,
        `APPLY ${plan.digest}`,
        new Date("2026-08-29T01:00:00Z"),
      ),
    Error,
    "newer ledger invalidates",
  );
});

Deno.test("apply receipt replay is idempotent and conflicting receipt fails", async () => {
  const { store, plan } = await mature();
  const now = new Date("2026-08-28T01:00:00Z");
  const first = await applyRetention(
    clients(store),
    base(),
    plan.planId,
    plan.digest,
    `APPLY ${plan.digest}`,
    now,
  );
  const replay = await applyRetention(
    clients(store),
    base(),
    plan.planId,
    plan.digest,
    `APPLY ${plan.digest}`,
    now,
  );
  assertEquals({ ...replay, replayed: false }, first);
  assertEquals(canonicalReceiptResource(replay), first);
  assertEquals(store.deletes.length, 1);
  const receiptKey = `retention/plans/${plan.planId}.applied.json`;
  store.destination.set(
    receiptKey,
    store.destination.get(receiptKey)!.replace(plan.digest, "f".repeat(64)),
  );
  await assertRejects(
    () =>
      applyRetention(
        clients(store),
        base(),
        plan.planId,
        plan.digest,
        `APPLY ${plan.digest}`,
        now,
      ),
    Error,
    "receipt identity or digest mismatch",
  );
});

Deno.test("receipt-write crash reuses durable candidate outcome", async () => {
  const { store, plan } = await mature();
  store.failReceiptOnce = true;
  const invoke = () =>
    applyRetention(
      clients(store),
      base(),
      plan.planId,
      plan.digest,
      `APPLY ${plan.digest}`,
      new Date("2026-08-28T01:00:00Z"),
    );
  await assertRejects(invoke, Error, "simulated crash");
  const result = await invoke();
  assertEquals(result.deleted, 1);
  assertEquals(result.uncertain, 0);
  assertEquals(store.deletes, ["objects/gone.jpg"]);
});

Deno.test("delete-result crash records uncertainty without claiming a repeated delete", async () => {
  const { store, plan } = await mature();
  store.failCandidateResultOnce = true;
  const invoke = () =>
    applyRetention(
      clients(store),
      base(),
      plan.planId,
      plan.digest,
      `APPLY ${plan.digest}`,
      new Date("2026-08-28T01:00:00Z"),
    );
  await assertRejects(invoke, Error, "simulated result-write crash");
  const result = await invoke();
  assertEquals(result.deleted, 0);
  assertEquals(result.uncertain, 1);
  assertEquals(
    result.outcomes[0].status,
    "uncertain-after-interrupted-attempt",
  );
  assertEquals(store.deletes, ["objects/gone.jpg"]);
});

Deno.test("fully paginates listings and rejects missing continuation tokens", async () => {
  const store = new Store();
  store.pageSize = 1;
  store.source.add("media/kept-a.jpg");
  store.source.add("media/kept-b.jpg");
  store.destination.set("objects/kept-a.jpg", "a");
  store.destination.set("objects/kept-b.jpg", "b");
  const plan = await previewRetention(
    clients(store),
    base(),
    "2026-08-28T00:00:00.000Z",
  );
  assertEquals(plan.candidates, []);

  const invalid = new Store();
  invalid.pageSize = 1;
  invalid.omitContinuationToken = true;
  invalid.source.add("media/a.jpg");
  invalid.source.add("media/b.jpg");
  await assertRejects(
    () =>
      previewRetention(
        clients(invalid),
        base(),
        "2026-08-28T00:00:00.000Z",
      ),
    Error,
    "truncated listing omitted continuation token",
  );
});

Deno.test("rejects listings beyond maximumObjects", async () => {
  const store = new Store();
  store.destination.set("objects/a.jpg", "a");
  store.destination.set("objects/b.jpg", "b");
  await assertRejects(
    () =>
      previewRetention(
        clients(store),
        { ...base(), maximumObjects: 1 },
        "2026-08-28T00:00:00.000Z",
      ),
    Error,
    "listing exceeded maximumObjects (1)",
  );
});

Deno.test("lifecycle verification accepts safe rules and rejects undercutting expiration", async () => {
  const store = new Store();
  store.lifecycleRules = [{
    ID: "retention",
    Status: "Enabled",
    Expiration: { Days: 32 },
  }];
  const safe = await verifyLifecycle(
    new Fake(store, "lifecycle"),
    base(),
    new Date("2026-08-28T00:00:00Z"),
  );
  assertEquals(safe.enabledExpirationRulesChecked, 1);
  store.lifecycleRules = [{
    ID: "unsafe",
    Status: "Enabled",
    Expiration: { Days: 31 },
  }];
  await assertRejects(
    () => verifyLifecycle(new Fake(store, "lifecycle"), base()),
    Error,
    "must retain objects for at least 32 days",
  );
});

Deno.test("model methods write conforming resources and structured logs", async () => {
  const store = new Store();
  const definition = createModel(
    () => clients(store),
    () => new Fake(store, "lifecycle"),
  );
  const test = createModelTestContext({
    globalArgs: base(),
    methodName: "previewRetention",
  });
  await definition.methods.previewRetention.execute(
    { observedAt: "2026-08-28T00:00:00.000Z" },
    test.context as any,
  );
  const writes = test.getWrittenResources();
  assertEquals(writes.length, 1);
  assertEquals(writes[0].specName, "retentionPlan");
  assertEquals(
    definition.resources.retentionPlan.schema.safeParse(writes[0].data).success,
    true,
  );
  assertEquals(test.getLogsByLevel("info").length, 2);

  assertEquals(
    (await definition.checks["valid-retention-policy"].execute({
      globalArgs: base(),
    })).pass,
    true,
  );
  assertEquals(
    (await definition.checks["live-retention-authority"].execute({
      globalArgs: base(),
    })).pass,
    true,
  );
});
