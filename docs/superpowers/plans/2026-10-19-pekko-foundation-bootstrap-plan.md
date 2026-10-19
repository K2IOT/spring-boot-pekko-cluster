# Pekko Foundation Bootstrap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create a buildable Spring Boot 4.1.1 multi-module foundation that starts exactly one typed Pekko ActorSystem, forms a usable single-node local cluster, exposes local runtime readiness, and shuts down through one lifecycle owner.

**Architecture:** Spring Boot owns process composition and HTTP/Actuator; `pekko-runtime` owns Pekko runtime references and state. The executable `pekko-app` composes `pekko-runtime` and domain modules, while `pekko-runtime` remains independent of demo/business code.

**Tech Stack:** Java 21, Maven, Spring Boot 4.1.1, Apache Pekko Core 1.7.1 (Scala binary 2.13), JUnit 5, Spring Boot Test.

**Spec:** `docs/superpowers/specs/2026-10-19-spring-boot-pekko-cluster-sharding-design.md`

## Global Constraints

- Java version is exactly 21.
- Maven groupId is `io.github.k2iot`; root artifactId is `spring-boot-pekko-cluster`.
- Modules are exactly `pekko-runtime`, `pekko-demo-domain`, `pekko-app`, and `pekko-test-support`.
- Spring Boot version is `4.1.1`; Apache Pekko Core version is `1.7.1`; Scala binary version is `2.13`.
- Import `org.apache.pekko:pekko-bom_2.13:1.7.1`; do not assign independent versions to Pekko Core modules.
- ActorSystem name is exactly `spring-boot-pekko-cluster` and there is exactly one ActorSystem per Spring Boot process.
- Root protocol is typed (`RootCommand`), not `Void` or `Object`.
- Spring configuration stays in YAML; Pekko configuration stays in HOCON.
- No DB, Kafka, Redis, Cassandra, ZooKeeper, or other external runtime service is required by this plan.
- No actor may perform Spring `ApplicationContext` lookup.

## Review Focus

- Duplicate ActorSystem creation: a second lookup/injection path must still return the same Spring singleton ActorSystem; Task 3 tests this.
- Profile/config mismatch: local startup must load `application-local.yml` with `application-local.conf`; Task 5 tests the resolved ports and actor-system name.
- Readiness before cluster `Up`: HTTP may bind, but readiness must remain refusing until the local member reaches `Up`; Task 6 tests the transition.
- Fatal bootstrap failure: runtime phase must become `FAILED` and never report ready; Task 4 tests failed initialization state.
- Shutdown ownership: closing the Spring context must trigger the lifecycle bridge rather than ad-hoc controller/service termination; Task 6 tests ActorSystem termination.

---

### Task 1: Create the Maven reactor and dependency-management baseline

**Files:**
- Create: `pom.xml`
- Create: `.gitignore`
- Create: `pekko-runtime/pom.xml`
- Create: `pekko-demo-domain/pom.xml`
- Create: `pekko-app/pom.xml`
- Create: `pekko-test-support/pom.xml`

**Interfaces:**
- Consumes: approved version and module boundaries from the spec.
- Produces: Maven coordinates `io.github.k2iot:spring-boot-pekko-cluster` and four child modules that later tasks compile against.

- [ ] **Step 1: Write the root POM with exact modules and version properties**

Define properties `java.version=21`, `spring-boot.version=4.1.1`, `pekko.version=1.7.1`, `scala.binary.version=2.13`, and `pekko-management.version=1.2.1`. Import `spring-boot-dependencies` and `pekko-bom_${scala.binary.version}` under dependency management.

- [ ] **Step 2: Write module POMs with dependency direction only**

`pekko-runtime` has Pekko runtime dependencies; `pekko-demo-domain` depends on `pekko-runtime`; `pekko-app` depends on `pekko-runtime` and `pekko-demo-domain`; `pekko-test-support` depends on runtime/domain with test libraries. Do not add Docker/Kubernetes dependencies yet.

- [ ] **Step 3: Add Maven Enforcer rules**

Require Java 21 and dependency convergence/upper-bound consistency without overriding Spring-managed libraries ad hoc.

- [ ] **Step 4: Verify the empty reactor**

Run: `mvn -q -DskipTests validate`

Expected: reactor lists all four modules and exits `0`.

- [ ] **Step 5: Commit**

```bash
git add pom.xml .gitignore pekko-runtime/pom.xml pekko-demo-domain/pom.xml pekko-app/pom.xml pekko-test-support/pom.xml
git commit -m "build: create Spring Boot Pekko Maven reactor"
```

---

### Task 2: Define the typed root protocol and runtime state model

**Files:**
- Create: `pekko-runtime/src/main/java/io/github/k2iot/pekko/runtime/actor/RootCommand.java`
- Create: `pekko-runtime/src/main/java/io/github/k2iot/pekko/runtime/actor/RootGuardian.java`
- Create: `pekko-runtime/src/main/java/io/github/k2iot/pekko/runtime/lifecycle/RuntimePhase.java`
- Create: `pekko-runtime/src/main/java/io/github/k2iot/pekko/runtime/lifecycle/PekkoRuntimeState.java`
- Test: `pekko-runtime/src/test/java/io/github/k2iot/pekko/runtime/lifecycle/PekkoRuntimeStateTest.java`

**Interfaces:**
- Consumes: no prior Java interfaces.
- Produces: `sealed interface RootCommand`, `Behavior<RootCommand> RootGuardian.create()`, `enum RuntimePhase`, and immutable `PekkoRuntimeState` snapshots used by all later lifecycle/health code.

- [ ] **Step 1: Write failing state-model tests**

Assert the initial phase is `NEW`; phase transition snapshots are immutable; `FAILED` and `TERMINATED` are representable terminal states.

- [ ] **Step 2: Run the targeted test**

Run: `mvn -pl pekko-runtime -Dtest=PekkoRuntimeStateTest test`

Expected: FAIL because the types do not exist.

- [ ] **Step 3: Implement exact types**

`RuntimePhase` must contain `NEW`, `ACTOR_SYSTEM_CREATED`, `CORE_INITIALIZED`, `SHARDING_INITIALIZED`, `SINGLETON_INITIALIZED`, `MANAGEMENT_STARTED`, `CLUSTER_JOINING`, `CLUSTER_UP`, `READY`, `DRAINING`, `LEAVING`, `TERMINATING`, `TERMINATED`, `FAILED`.

`PekkoRuntimeState` is a record with at least `RuntimePhase phase`, `String clusterStatus`, `boolean shardingInitialized`, `boolean singletonInitialized`, `boolean managementStarted`, and `Instant since`.

- [ ] **Step 4: Implement `RootGuardian.create()`**

Return a minimal typed behavior that owns no business actor and does not spawn sharded entities.

- [ ] **Step 5: Re-run tests**

Run: `mvn -pl pekko-runtime test`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add pekko-runtime/src
git commit -m "feat: add typed Pekko root and runtime state"
```

---

### Task 3: Create the single ActorSystem and runtime facade

**Files:**
- Create: `pekko-runtime/src/main/java/io/github/k2iot/pekko/runtime/PekkoRuntime.java`
- Create: `pekko-runtime/src/main/java/io/github/k2iot/pekko/runtime/DefaultPekkoRuntime.java`
- Create: `pekko-runtime/src/main/java/io/github/k2iot/pekko/runtime/actor/PekkoActorSystemFactory.java`
- Test: `pekko-runtime/src/test/java/io/github/k2iot/pekko/runtime/actor/PekkoActorSystemFactoryTest.java`

**Interfaces:**
- Consumes: `RootGuardian.create()`, `PekkoRuntimeState`.
- Produces: `ActorSystem<RootCommand> create(Config config)`, `PekkoRuntime.actorSystem()`, `cluster()`, `runtimeState()`; later plans extend the facade with sharding/singleton references without changing ActorSystem ownership.

- [ ] **Step 1: Write the failing ActorSystem factory test**

Create test HOCON with cluster provider and ephemeral Artery port; assert factory output has name `spring-boot-pekko-cluster` and typed root protocol.

- [ ] **Step 2: Run the test and verify failure**

Run: `mvn -pl pekko-runtime -Dtest=PekkoActorSystemFactoryTest test`

Expected: FAIL because factory/facade are absent.

- [ ] **Step 3: Implement `PekkoActorSystemFactory.create(Config)`**

Signature: `ActorSystem<RootCommand> create(com.typesafe.config.Config config)`; call `ActorSystem.create(RootGuardian.create(), "spring-boot-pekko-cluster", config)` exactly once per factory invocation.

- [ ] **Step 4: Implement `PekkoRuntime` and `DefaultPekkoRuntime`**

Expose owned references; do not hide or duplicate native Pekko types. Use an `AtomicReference<PekkoRuntimeState>` or equally explicit local state holder, not Spring static globals.

- [ ] **Step 5: Re-run unit tests**

Run: `mvn -pl pekko-runtime test`

Expected: PASS and ActorSystem terminates in test cleanup.

- [ ] **Step 6: Commit**

```bash
git add pekko-runtime/src
git commit -m "feat: own a single typed Pekko ActorSystem"
```

---

### Task 4: Add Spring Boot composition and lifecycle ownership

**Files:**
- Create: `pekko-app/src/main/java/io/github/k2iot/pekko/app/PekkoClusterApplication.java`
- Create: `pekko-app/src/main/java/io/github/k2iot/pekko/app/config/PekkoSpringConfiguration.java`
- Create: `pekko-app/src/main/java/io/github/k2iot/pekko/app/lifecycle/SpringPekkoLifecycleBridge.java`
- Create: `pekko-app/src/main/java/io/github/k2iot/pekko/app/config/PekkoRuntimeProperties.java`
- Test: `pekko-app/src/test/java/io/github/k2iot/pekko/app/PekkoApplicationContextTest.java`
- Test: `pekko-app/src/test/java/io/github/k2iot/pekko/app/lifecycle/SpringPekkoLifecycleBridgeTest.java`

**Interfaces:**
- Consumes: `PekkoActorSystemFactory`, `DefaultPekkoRuntime`.
- Produces: Spring singleton beans `ActorSystem<RootCommand>` and `PekkoRuntime`; `SpringPekkoLifecycleBridge implements SmartLifecycle` as the only normal startup/shutdown owner.

- [ ] **Step 1: Write failing Spring context tests**

Assert exactly one `ActorSystem` bean exists, repeated injection returns the same instance, exactly one `PekkoRuntime` bean exists, and no controller/service creates another ActorSystem.

- [ ] **Step 2: Write failing lifecycle failure-state test**

Inject a bootstrap collaborator that throws and assert runtime phase becomes `FAILED` and `isRunning()` is false.

- [ ] **Step 3: Run targeted tests**

Run: `mvn -pl pekko-app -am test`

Expected: FAIL because application/config/lifecycle beans are absent.

- [ ] **Step 4: Implement Spring configuration and `SmartLifecycle` bridge**

`start()` initializes owned runtime collaborators in explicit order; `stop()` delegates to Pekko coordinated shutdown/termination boundary. Do not use scattered `@PostConstruct` startup methods.

- [ ] **Step 5: Re-run tests**

Run: `mvn -pl pekko-app -am test`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add pekko-app/src
git commit -m "feat: integrate Pekko lifecycle with Spring Boot"
```

---

### Task 5: Add base and local configuration for a one-node cluster

**Files:**
- Create: `pekko-app/src/main/resources/application.yml`
- Create: `pekko-app/src/main/resources/application-local.yml`
- Create: `pekko-app/src/main/resources/application.conf`
- Create: `pekko-app/src/main/resources/application-local.conf`
- Create: `pekko-runtime/src/main/java/io/github/k2iot/pekko/runtime/config/PekkoConfigLoader.java`
- Test: `pekko-runtime/src/test/java/io/github/k2iot/pekko/runtime/config/PekkoConfigLoaderTest.java`

**Interfaces:**
- Consumes: ActorSystem factory expects a resolved Typesafe `Config`.
- Produces: `Config load(String profile)` mapping `local -> application-local.conf` with fallback to `application.conf` and Pekko reference config.

- [ ] **Step 1: Write failing config tests**

Assert local config resolves actor provider `cluster`, system name usage remains external/fixed, role contains `sharding`, local node1 defaults to Spring `8081`, Artery `2551`, Management `8551`, and seed list contains the local cluster seed address.

- [ ] **Step 2: Run tests**

Run: `mvn -pl pekko-runtime -Dtest=PekkoConfigLoaderTest test`

Expected: FAIL.

- [ ] **Step 3: Implement HOCON common defaults**

Set cluster provider, `sharding` role, SBR provider with `keep-majority`, explicit `pekko.cluster.sharding.number-of-shards = 4096`, `state-store-mode = ddata`, and `remember-entities = off`. Do not add Kubernetes bootstrap settings here.

- [ ] **Step 4: Implement local override and Spring YAML**

Use static local seed nodes and profile-paired ports; Spring YAML owns only server/actuator/application runtime settings.

- [ ] **Step 5: Implement `PekkoConfigLoader.load(String profile)` and profile mismatch validation**

Reject unsupported profile names instead of silently loading unrelated config.

- [ ] **Step 6: Re-run tests**

Run: `mvn -pl pekko-runtime -am test`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add pekko-app/src/main/resources pekko-runtime/src
git commit -m "feat: add local Pekko cluster configuration"
```

---

### Task 6: Track local membership and bridge readiness/liveness

**Files:**
- Create: `pekko-runtime/src/main/java/io/github/k2iot/pekko/runtime/cluster/ClusterRuntimeState.java`
- Create: `pekko-runtime/src/main/java/io/github/k2iot/pekko/runtime/cluster/ClusterStateTracker.java`
- Create: `pekko-app/src/main/java/io/github/k2iot/pekko/app/health/PekkoHealthIndicator.java`
- Create: `pekko-app/src/main/java/io/github/k2iot/pekko/app/health/PekkoAvailabilityManager.java`
- Create: `pekko-app/src/main/java/io/github/k2iot/pekko/app/diagnostics/RuntimeDiagnosticController.java`
- Test: `pekko-app/src/test/java/io/github/k2iot/pekko/app/health/PekkoAvailabilityManagerTest.java`
- Test: `pekko-app/src/test/java/io/github/k2iot/pekko/app/SingleNodeStartupTest.java`

**Interfaces:**
- Consumes: `PekkoRuntime`, Spring `ApplicationEventPublisher`, Pekko Cluster events.
- Produces: immutable cached cluster state; Spring liveness/readiness state; `GET /api/v1/diagnostics/runtime`.

- [ ] **Step 1: Write failing readiness transition tests**

Assert readiness begins `REFUSING_TRAFFIC`, remains refusing before member `Up`, switches to `ACCEPTING_TRAFFIC` only when required local runtime state is usable, and returns to refusing during draining/failed phases.

- [ ] **Step 2: Write failing single-node startup test**

Start the app with test/local config on ephemeral HTTP port; await local cluster member `Up`; assert one ActorSystem, runtime not `FAILED`, and readiness `UP`.

- [ ] **Step 3: Run tests**

Run: `mvn -pl pekko-app -am test`

Expected: FAIL before tracker/health implementation.

- [ ] **Step 4: Implement event-driven `ClusterStateTracker`**

Subscribe to membership/reachability events once; maintain cached immutable state for health/diagnostics rather than distributed asks on probes.

- [ ] **Step 5: Implement availability bridge and health indicator**

Liveness fails only for unrecoverable runtime failure/termination; remote member churn does not automatically fail local liveness.

- [ ] **Step 6: Implement runtime diagnostic response**

Return only safe runtime fields: actor-system name, local address, phase, cluster status, readiness flags; never dump raw config or secrets.

- [ ] **Step 7: Run verification**

Run: `mvn clean verify`

Expected: PASS without any external service.

- [ ] **Step 8: Manual smoke**

Run: `mvn -pl pekko-app spring-boot:run -Dspring-boot.run.profiles=local`

Expected: process starts, local member reaches `Up`, `/actuator/health/readiness` is `UP`, `/api/v1/diagnostics/runtime` reports system `spring-boot-pekko-cluster`.

- [ ] **Step 9: Commit**

```bash
git add pekko-runtime pekko-app
git commit -m "feat: expose Pekko single-node readiness"
```
