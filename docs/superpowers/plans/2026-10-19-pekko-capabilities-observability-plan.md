# Pekko Capabilities and Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Integrate Pekko Management, Streams, Persistence TestKit, Spring diagnostics/metrics/logging, and production-grade coordinated shutdown without introducing an external runtime dependency.

**Architecture:** Runtime capabilities stay modular and explicit: Management is required at boot, Streams is available without a useless background stream, Persistence is proven in tests only, and health/metrics read cached local state. Spring exposes the operational control plane while Pekko remains the distributed-runtime owner.

**Tech Stack:** Pekko Management 1.2.1, Pekko Streams/Persistence 1.7.1, Spring Boot Actuator, Micrometer/Prometheus, JUnit 5.

**Spec:** `docs/superpowers/specs/2026-10-19-spring-boot-pekko-cluster-sharding-design.md`

## Global Constraints

- Inherits Plans 1–2 constraints.
- Management port is `8558` in container/Kubernetes baseline and local per-node variants 8551/8552/8553.
- Management startup is mandatory; failure is fatal for baseline readiness.
- Streams runtime is available but no infinite/background demo stream starts at application boot.
- Persistence Typed/Query dependencies are present; no production journal DB is required at boot.
- Health probes read cached local runtime state; no distributed asks per probe.
- Metrics use low-cardinality labels only; entityId/requestId/userId/sessionId are forbidden labels.
- CoordinatedShutdown is the primary shutdown path; direct `ActorSystem.terminate()` is fallback only.

## Review Focus

- Management bind failure: startup must become `FAILED`, not ready-without-management; Task 1 tests it.
- Persistence plugin leakage into normal startup: app must still boot without DB/journal service; Task 3 tests it.
- Metrics cardinality: entity IDs must never become tags; Task 5 tests meter IDs.
- Probe-induced cluster traffic: health reads cached state only; Task 4 tests no gateway/ask invocation.
- SIGTERM ordering: readiness must flip false before cluster leave/ActorSystem termination; Task 6 tests event order.

---

### Task 1: Add Pekko Management lifecycle

**Files:**
- Modify: `pom.xml`
- Modify: `pekko-runtime/pom.xml`
- Create: `pekko-runtime/src/main/java/io/github/k2iot/pekko/runtime/management/PekkoManagementLifecycle.java`
- Modify: `pekko-runtime/src/main/java/io/github/k2iot/pekko/runtime/DefaultPekkoRuntime.java`
- Modify: `pekko-app/src/main/resources/application.conf`
- Test: `pekko-runtime/src/test/java/io/github/k2iot/pekko/runtime/management/PekkoManagementLifecycleTest.java`

**Interfaces:**
- Consumes: ActorSystem.
- Produces: `CompletionStage<Done> start()`/equivalent owned lifecycle result and runtime `managementStarted=true` state only after successful bind.

- [ ] **Step 1: Add managed dependencies**

Add `pekko-management_2.13`, `pekko-management-cluster-http_2.13`, and `pekko-management-cluster-bootstrap_2.13` at version `1.2.1`; keep Management version separate from Core BOM.

- [ ] **Step 2: Write failing success/failure lifecycle tests**

Assert successful start updates runtime state; simulated/bind failure transitions runtime to `FAILED` and prevents readiness.

- [ ] **Step 3: Implement lifecycle and base Management config**

Start Management explicitly from the single runtime lifecycle owner; do not auto-start from unrelated beans.

- [ ] **Step 4: Verify**

Run: `mvn -pl pekko-runtime,pekko-app -am test`

- [ ] **Step 5: Commit**

```bash
git add pom.xml pekko-runtime pekko-app/src/main/resources
git commit -m "feat: manage Pekko Management lifecycle"
```

---

### Task 2: Expose Pekko Streams runtime and smoke test

**Files:**
- Create: `pekko-runtime/src/main/java/io/github/k2iot/pekko/runtime/streams/PekkoStreamsRuntime.java`
- Test: `pekko-test-support/src/test/java/io/github/k2iot/pekko/testsupport/streams/PekkoStreamsTest.java`

**Interfaces:**
- Consumes: ActorSystem/system materializer.
- Produces: owned accessor for the system Materializer/runtime; no background graph.

- [ ] **Step 1: Write failing finite-stream test**

Materialize `Source.range(1,10).map(i -> i * 2).runWith(Sink.seq(), materializer)` and assert `[2,4,...,20]`.

- [ ] **Step 2: Implement runtime accessor**

Use system materializer; do not create multiple custom materializers.

- [ ] **Step 3: Re-run and commit**

```bash
git add pekko-runtime pekko-test-support
git commit -m "feat: expose Pekko Streams runtime"
```

---

### Task 3: Add Persistence Typed/Query and Persistence TestKit proof

**Files:**
- Modify: `pekko-runtime/pom.xml`
- Modify: `pekko-test-support/pom.xml`
- Create: `pekko-test-support/src/test/java/io/github/k2iot/pekko/testsupport/persistence/PersistentDemoBehavior.java`
- Test: `pekko-test-support/src/test/java/io/github/k2iot/pekko/testsupport/persistence/PersistenceRecoveryTest.java`
- Test: `pekko-app/src/test/java/io/github/k2iot/pekko/app/NoExternalJournalStartupTest.java`

**Interfaces:**
- Consumes: Pekko Persistence TestKit only in test scope.
- Produces: test behavior with deterministic `PersistenceId`, command -> event -> recovery proof; no production persistent actor.

- [ ] **Step 1: Add Core-BOM-managed persistence dependencies**

Add `pekko-persistence-typed_2.13`, `pekko-persistence-query_2.13`, and test-scope `pekko-persistence-testkit_2.13`.

- [ ] **Step 2: Write failing persistence recovery test**

Persist an event, stop/recreate the test behavior with the same PersistenceId, and assert recovered state includes the event.

- [ ] **Step 3: Write no-external-journal startup regression test**

Start the normal Spring Boot app with baseline profile and assert readiness can become `UP` without DB/journal service.

- [ ] **Step 4: Implement test-only persistent behavior/config**

Keep it under `pekko-test-support`; do not register it in application sharding/startup.

- [ ] **Step 5: Verify and commit**

```bash
git add pekko-runtime/pom.xml pekko-test-support pekko-app/src/test
git commit -m "test: prove Pekko persistence without runtime database"
```

---

### Task 4: Complete Actuator health and diagnostic APIs

**Files:**
- Modify: `pekko-app/src/main/java/io/github/k2iot/pekko/app/health/PekkoHealthIndicator.java`
- Create: `pekko-app/src/main/java/io/github/k2iot/pekko/app/diagnostics/ClusterDiagnosticController.java`
- Create: `pekko-app/src/main/java/io/github/k2iot/pekko/app/diagnostics/ShardingDiagnosticController.java`
- Create: `pekko-app/src/main/java/io/github/k2iot/pekko/app/diagnostics/SingletonDiagnosticController.java`
- Create: `pekko-app/src/main/java/io/github/k2iot/pekko/app/diagnostics/dto/RuntimeDtos.java`
- Test: `pekko-app/src/test/java/io/github/k2iot/pekko/app/diagnostics/DiagnosticsControllerTest.java`
- Test: `pekko-app/src/test/java/io/github/k2iot/pekko/app/health/PekkoHealthIndicatorTest.java`

**Interfaces:**
- Consumes: cached `ClusterStateTracker`, `ShardingGateway`, demo singleton ref, `PekkoRuntimeState`.
- Produces: `GET /api/v1/diagnostics/runtime`, `/cluster`, `/sharding/{entityId}`, `/singleton`; Actuator health component `pekko`.

- [ ] **Step 1: Write controller contract tests**

Assert safe response fields, 400 for blank entity ID, and no raw config/secret field. Sharding/singleton endpoints may use explicit bounded `ask`; runtime/cluster health endpoints do not.

- [ ] **Step 2: Write health no-network-poll test**

Use mocks/spies to assert a health read touches cached state only and never invokes ShardingGateway/singleton ask.

- [ ] **Step 3: Implement DTOs/controllers/health mapping**

Use Spring HTTP DTO types distinct from actor protocol records.

- [ ] **Step 4: Verify and commit**

```bash
git add pekko-app/src
git commit -m "feat: expose Pekko operational diagnostics"
```

---

### Task 5: Add low-cardinality Micrometer metrics and operational logging

**Files:**
- Create: `pekko-app/src/main/java/io/github/k2iot/pekko/app/metrics/PekkoRuntimeMetrics.java`
- Create: `pekko-runtime/src/main/java/io/github/k2iot/pekko/runtime/cluster/ClusterEventLogger.java`
- Modify: `pekko-app/src/main/resources/application.yml`
- Test: `pekko-app/src/test/java/io/github/k2iot/pekko/app/metrics/PekkoRuntimeMetricsTest.java`

**Interfaces:**
- Consumes: cached runtime/cluster state and Spring `MeterRegistry`.
- Produces: gauges/counters under `pekko.runtime.*`, `pekko.cluster.*`, `pekko.sharding.*` with only low-cardinality tags.

- [ ] **Step 1: Write failing metric registration test**

Assert meter names exist and tag keys are limited to `member.status`, `node.role`, `entity.type`, `runtime.phase`; assert no entity ID/request ID tags appear after sharding diagnostic calls.

- [ ] **Step 2: Implement metrics from cached state/events**

Do not schedule frequent distributed sharding-stat polls.

- [ ] **Step 3: Implement transition logging**

Log membership, sharding registration, singleton placement/failover, bootstrap failure, and shutdown phase changes; do not log every actor message.

- [ ] **Step 4: Verify and commit**

```bash
git add pekko-app pekko-runtime
git commit -m "feat: add Pekko metrics and lifecycle logging"
```

---

### Task 6: Harden coordinated shutdown ordering

**Files:**
- Create: `pekko-runtime/src/main/java/io/github/k2iot/pekko/runtime/lifecycle/PekkoCoordinatedShutdownBridge.java`
- Modify: `pekko-app/src/main/java/io/github/k2iot/pekko/app/lifecycle/SpringPekkoLifecycleBridge.java`
- Test: `pekko-app/src/test/java/io/github/k2iot/pekko/app/lifecycle/CoordinatedShutdownOrderTest.java`
- Test: `pekko-test-support/src/test/java/io/github/k2iot/pekko/testsupport/cluster/GracefulShutdownIT.java`

**Interfaces:**
- Consumes: Spring availability publisher, Cluster self-address, Pekko CoordinatedShutdown.
- Produces: one bounded shutdown future/path that marks readiness false, initiates leave/handoff, then terminates ActorSystem.

- [ ] **Step 1: Write failing shutdown-order test**

Record lifecycle events and assert ordering `REFUSING_TRAFFIC -> cluster leave initiated -> ActorSystem terminated`; direct terminate must not occur first.

- [ ] **Step 2: Write distributed graceful-shutdown integration test**

With three nodes, gracefully stop one host of an entity/singleton; assert remaining cluster keeps entity reachable and singleton eventually relocates.

- [ ] **Step 3: Implement coordinated shutdown bridge**

Use Pekko CoordinatedShutdown phases/hooks rather than raw bean `destroyMethod=terminate` as the primary orchestration.

- [ ] **Step 4: Run full verification**

Run: `mvn clean verify`

Expected: PASS with no external infrastructure.

- [ ] **Step 5: Commit**

```bash
git add pekko-runtime pekko-app pekko-test-support
git commit -m "feat: coordinate Spring and Pekko shutdown"
```
