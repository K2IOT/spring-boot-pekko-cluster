# Pekko Cluster Sharding Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the runnable foundation into a three-node Pekko cluster with Artery, DData-backed Typed Cluster Sharding, a relocatable demo entity, Cluster Singleton failover, CBOR wire serialization, and deterministic integration tests for failure/rejoin behavior.

**Architecture:** The runtime owns Cluster/DData/Sharding/Singleton extensions and exposes narrow definition/gateway interfaces. Domain modules contribute typed entity/singleton definitions; placement and recovery remain Pekko responsibilities, not Spring concerns.

**Tech Stack:** Java 21, Apache Pekko Core 1.7.1, Pekko Cluster Typed, Cluster Sharding Typed, Distributed Data, Cluster Singleton, Jackson CBOR, Actor TestKit/JUnit 5.

**Spec:** `docs/superpowers/specs/2026-10-19-spring-boot-pekko-cluster-sharding-design.md`

## Global Constraints

- Inherits all constraints from Plan 1.
- All baseline nodes carry role `sharding`.
- Artery is the remote transport; local node ports are 2551/2552/2553.
- Split Brain Resolver strategy is `keep-majority`.
- Cluster Sharding uses Typed API only, exactly `4096` shards, default hashing/allocation, `state-store-mode=ddata`, and `remember-entities=off`.
- Entity IDs are logical IDs and must never include host/pod/node address.
- Actor protocols use immutable Java records/sealed interfaces and `tell` by default.
- Blocking operations on the normal actor dispatcher are forbidden.
- Baseline application wire serialization is Pekko Jackson CBOR behind `CborSerializable`.
- Cluster Singleton is not used for partitionable hot-path workloads.

## Review Focus

- Same entity ID after relocation: host may change but logical ID must remain stable; Task 6 tests it.
- Unknown/invalid entity IDs: gateway must reject blank IDs without creating actors; Task 4 tests it.
- Shard configuration drift: test config must assert exactly 4096 shards and remember-entities off; Task 4 tests it.
- Singleton failover gap: callers may temporarily time out but surviving nodes must converge on one new singleton host; Task 7 tests eventual convergence.
- Remote serialization compatibility: a real cross-node Ping/GetLocation must serialize through Artery, not only local round-trip; Task 8 tests it.

---

### Task 1: Complete cluster membership tracking and local multi-node configuration

**Files:**
- Modify: `pekko-runtime/src/main/java/io/github/k2iot/pekko/runtime/cluster/ClusterStateTracker.java`
- Create: `pekko-app/src/main/resources/application-node1.conf`
- Create: `pekko-app/src/main/resources/application-node2.conf`
- Create: `pekko-app/src/main/resources/application-node3.conf`
- Create: `pekko-test-support/src/test/java/io/github/k2iot/pekko/testsupport/cluster/ThreeNodeClusterFixture.java`
- Test: `pekko-test-support/src/test/java/io/github/k2iot/pekko/testsupport/cluster/ClusterMembershipIT.java`

**Interfaces:**
- Consumes: runtime ActorSystem and cached state from Plan 1.
- Produces: reusable `ThreeNodeClusterFixture` and member/reachability snapshots used by all distributed tests.

- [ ] **Step 1: Write a failing 3-node membership integration test**

Start three ActorSystems on ephemeral ports, join via node1/node2 seed addresses, and eventually assert member count 3, all `Up`, unreachable count 0.

- [ ] **Step 2: Run the test**

Run: `mvn -pl pekko-test-support -Dtest=ClusterMembershipIT test`

Expected: FAIL before fixture/config completion.

- [ ] **Step 3: Implement `ThreeNodeClusterFixture`**

Provide `start()`, `systems()`, `awaitAllUp(Duration)`, `shutdownNode(int)`, and `close()`; all waits are event/eventually based, never fixed long sleeps.

- [ ] **Step 4: Extend membership tracker for joined/up/leaving/exiting/removed and unreachable/reachable events**

Keep state local and immutable.

- [ ] **Step 5: Verify**

Run: `mvn -pl pekko-test-support -am test`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add pekko-runtime pekko-app/src/main/resources pekko-test-support
git commit -m "test: prove three-node Pekko membership"
```

---

### Task 2: Initialize Distributed Data explicitly

**Files:**
- Create: `pekko-runtime/src/main/java/io/github/k2iot/pekko/runtime/ddata/DistributedDataBootstrap.java`
- Modify: `pekko-runtime/src/main/java/io/github/k2iot/pekko/runtime/DefaultPekkoRuntime.java`
- Test: `pekko-test-support/src/test/java/io/github/k2iot/pekko/testsupport/ddata/DistributedDataIT.java`

**Interfaces:**
- Consumes: `ActorSystem<RootCommand>`.
- Produces: `DistributedDataBootstrap.initialize(ActorSystem<?>)` and owned access to the DData replicator/extension.

- [ ] **Step 1: Write failing CRDT replication test**

Update a uniquely named ORSet/ORMap key on node A and eventually assert the value is observable on nodes B and C using the test's explicit consistency semantics.

- [ ] **Step 2: Run test and confirm failure**

Run: `mvn -pl pekko-test-support -Dtest=DistributedDataIT test`

- [ ] **Step 3: Implement explicit DData bootstrap**

Initialize extension during `CORE_INITIALIZED`; do not add application-global majority consistency policy.

- [ ] **Step 4: Re-run test**

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pekko-runtime pekko-test-support
git commit -m "feat: initialize Pekko Distributed Data"
```

---

### Task 3: Add typed sharding definition and gateway interfaces

**Files:**
- Create: `pekko-runtime/src/main/java/io/github/k2iot/pekko/runtime/sharding/ShardedEntityDefinition.java`
- Create: `pekko-runtime/src/main/java/io/github/k2iot/pekko/runtime/sharding/ShardingGateway.java`
- Create: `pekko-runtime/src/main/java/io/github/k2iot/pekko/runtime/sharding/DefaultShardingGateway.java`
- Test: `pekko-runtime/src/test/java/io/github/k2iot/pekko/runtime/sharding/DefaultShardingGatewayTest.java`

**Interfaces:**
- Consumes: native `ClusterSharding`.
- Produces: `EntityTypeKey<M> typeKey()`, `Entity<M, ?> entity()`, and `<M> EntityRef<M> entityRef(EntityTypeKey<M> typeKey, String entityId)`.

- [ ] **Step 1: Write failing gateway contract tests**

Assert a valid logical ID delegates to `ClusterSharding.entityRefFor`; blank/null logical IDs are rejected before lookup; gateway holds no entity-ref map/cache.

- [ ] **Step 2: Run test**

Run: `mvn -pl pekko-runtime -Dtest=DefaultShardingGatewayTest test`

- [ ] **Step 3: Implement exact interfaces**

Keep native Pekko generic types visible; do not invent reflection scanning or a generic framework around them.

- [ ] **Step 4: Re-run test and commit**

```bash
git add pekko-runtime/src
git commit -m "feat: add typed sharding extension points"
```

---

### Task 4: Implement the demo sharded entity and explicit sharding bootstrap

**Files:**
- Create: `pekko-demo-domain/src/main/java/io/github/k2iot/pekko/demo/sharding/DemoEntity.java`
- Create: `pekko-demo-domain/src/main/java/io/github/k2iot/pekko/demo/sharding/DemoEntityDefinition.java`
- Create: `pekko-runtime/src/main/java/io/github/k2iot/pekko/runtime/sharding/ShardingBootstrap.java`
- Test: `pekko-demo-domain/src/test/java/io/github/k2iot/pekko/demo/sharding/DemoEntityTest.java`
- Test: `pekko-runtime/src/test/java/io/github/k2iot/pekko/runtime/sharding/ShardingBootstrapTest.java`

**Interfaces:**
- Consumes: `ShardedEntityDefinition`, ActorSystem cluster address.
- Produces: `DemoEntity.TYPE_KEY`, sealed `DemoEntity.Command`, records `Ping`, `GetLocation`, `Pong`, `Location`, enum/record `Stop`, and `ShardingBootstrap.initialize(Collection<ShardedEntityDefinition<?>>)`.

- [ ] **Step 1: Write actor protocol tests**

Using Typed ActorTestKit, assert `Ping(replyTo)` returns `Pong(entityId)`, `GetLocation(replyTo)` returns entity ID plus current member address, and `Stop` terminates the behavior.

- [ ] **Step 2: Write sharding config/registration tests**

Assert exactly 4096 shards are resolved, DData state store is selected, remember-entities is off, and `DemoEntityDefinition` hosts on role `sharding`.

- [ ] **Step 3: Run tests and verify failure**

Run: `mvn -pl pekko-demo-domain,pekko-runtime -am test`

- [ ] **Step 4: Implement `DemoEntity` with immutable typed protocol**

No Spring dependency and no blocking calls.

- [ ] **Step 5: Implement explicit bootstrap iteration over contributed definitions**

Register each definition once; registration failure propagates as fatal bootstrap failure.

- [ ] **Step 6: Re-run tests**

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add pekko-demo-domain pekko-runtime
git commit -m "feat: register DData-backed demo sharding"
```

---

### Task 5: Add Pekko Jackson CBOR wire serialization boundary

**Files:**
- Create: `pekko-runtime/src/main/java/io/github/k2iot/pekko/runtime/serialization/CborSerializable.java`
- Modify: `pekko-demo-domain/src/main/java/io/github/k2iot/pekko/demo/sharding/DemoEntity.java`
- Modify: `pekko-app/src/main/resources/application.conf`
- Test: `pekko-test-support/src/test/java/io/github/k2iot/pekko/testsupport/serialization/CborSerializationTest.java`

**Interfaces:**
- Consumes: demo actor protocol.
- Produces: marker interface bound to Pekko `jackson-cbor` serializer for remote application protocol messages.

- [ ] **Step 1: Write failing serialization round-trip tests**

Serialize/deserialize representative `Pong`/`Location` messages through Pekko Serialization and assert semantic equality; verify Spring JSON mapper is not injected into Pekko serialization code.

- [ ] **Step 2: Run test**

Run: `mvn -pl pekko-test-support -Dtest=CborSerializationTest test`

- [ ] **Step 3: Add marker binding and protocol markers**

Keep HTTP DTO and actor wire types separate.

- [ ] **Step 4: Re-run and commit**

```bash
git add pekko-runtime pekko-demo-domain pekko-app/src/main/resources pekko-test-support
git commit -m "feat: serialize Pekko messages with CBOR"
```

---

### Task 6: Prove sharding distribution and abrupt-node relocation

**Files:**
- Test: `pekko-test-support/src/test/java/io/github/k2iot/pekko/testsupport/sharding/ClusterShardingIT.java`

**Interfaces:**
- Consumes: `ThreeNodeClusterFixture`, `ShardingGateway`, `DemoEntity.TYPE_KEY`.
- Produces: regression proof for distribution and relocation semantics.

- [ ] **Step 1: Write distribution test**

Activate at least 100 distinct logical IDs, query locations, and assert entities are hosted on more than one node; do not assert exact 33/33/34 distribution.

- [ ] **Step 2: Write relocation test**

Choose an entity hosted on node B, abruptly terminate B, await member removal, then query the same logical ID and assert it is reachable on A or C with unchanged entity ID and changed physical host.

- [ ] **Step 3: Run integration test**

Run: `mvn -pl pekko-test-support -Dtest=ClusterShardingIT test`

Expected: PASS without fixed long sleeps.

- [ ] **Step 4: Commit**

```bash
git add pekko-test-support/src/test/java/io/github/k2iot/pekko/testsupport/sharding
git commit -m "test: verify shard distribution and relocation"
```

---

### Task 7: Implement and test Cluster Singleton

**Files:**
- Create: `pekko-runtime/src/main/java/io/github/k2iot/pekko/runtime/singleton/SingletonDefinition.java`
- Create: `pekko-runtime/src/main/java/io/github/k2iot/pekko/runtime/singleton/ClusterSingletonBootstrap.java`
- Create: `pekko-demo-domain/src/main/java/io/github/k2iot/pekko/demo/singleton/DemoSingleton.java`
- Create: `pekko-demo-domain/src/main/java/io/github/k2iot/pekko/demo/singleton/DemoSingletonDefinition.java`
- Test: `pekko-test-support/src/test/java/io/github/k2iot/pekko/testsupport/singleton/ClusterSingletonIT.java`

**Interfaces:**
- Consumes: `ClusterSingleton`, role `sharding`.
- Produces: `SingletonDefinition<M>`, owned typed singleton proxy/ref, demo `Ping`/`GetLocation` protocol.

- [ ] **Step 1: Write failing steady-state and failover tests**

From all three nodes ask `GetLocation`; assert one common host. Kill the host; allow transient timeout; eventually assert all survivors agree on one new host.

- [ ] **Step 2: Run test**

Run: `mvn -pl pekko-test-support -Dtest=ClusterSingletonIT test`

- [ ] **Step 3: Implement minimal singleton definition/bootstrap and demo behavior**

Do not route sharded hot-path messages through it.

- [ ] **Step 4: Re-run and commit**

```bash
git add pekko-runtime pekko-demo-domain pekko-test-support
git commit -m "feat: add demo Cluster Singleton failover"
```

---

### Task 8: Prove remote serialization, graceful leave, and rejoin

**Files:**
- Test: `pekko-test-support/src/test/java/io/github/k2iot/pekko/testsupport/cluster/RemoteProtocolIT.java`
- Test: `pekko-test-support/src/test/java/io/github/k2iot/pekko/testsupport/cluster/GracefulLeaveAndRejoinIT.java`

**Interfaces:**
- Consumes: all Plan 2 runtime pieces.
- Produces: end-to-end proof that Artery transports real demo protocol messages and member lifecycle supports leave/new-incarnation rejoin.

- [ ] **Step 1: Write remote protocol test**

Force a demo entity onto/observe it on a remote node and send `Ping`/`GetLocation` from another node; assert valid replies cross Artery.

- [ ] **Step 2: Write graceful leave/rejoin test**

Ask node B to leave, await `Removed`, assert selected entities remain reachable, then start a new ActorSystem incarnation for B and await three `Up` members again.

- [ ] **Step 3: Run full Plan 2 verification**

Run: `mvn clean verify`

Expected: all unit and in-JVM distributed tests PASS.

- [ ] **Step 4: Commit**

```bash
git add pekko-test-support
git commit -m "test: verify Pekko remote lifecycle semantics"
```
