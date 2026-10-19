# Spring Boot Based Apache Pekko Cluster & Sharding Foundation — Architecture Design

**Date:** 2026-10-19  
**Status:** Design approved in conversation; written spec pending user review  
**Project name:** `spring-boot-pekko-cluster`  
**Target:** Java 21, Maven, Spring Boot 4.1.1, Apache Pekko Core 1.7.1, Apache Pekko Management 1.2.1

---

## 1. Purpose

Build a production-oriented reference foundation that runs Apache Pekko inside a Spring Boot process and proves the major Pekko distributed-runtime capabilities before any business domain is added.

The project must be runnable from IntelliJ or `java -jar` as a normal Spring Boot application. Starting Spring Boot must automatically create and bootstrap the Pekko runtime in the same JVM. A developer must not run a separate Pekko server.

The foundation must prove, through executable diagnostics and tests, that the following capabilities are integrated and working:

- Pekko Typed Actor runtime
- Pekko Cluster membership
- Artery remoting
- Cluster Sharding Typed
- Distributed Data
- Cluster Singleton
- Pekko Streams runtime
- Pekko Persistence Typed and Persistence Query dependencies, with Persistence TestKit coverage
- Pekko Jackson serialization
- Pekko Management
- Pekko Discovery
- Cluster Bootstrap for Kubernetes
- Spring Boot Actuator and Micrometer integration
- Graceful coordinated shutdown
- Local multi-node, Docker Compose, and Kubernetes-ready deployment models

The foundation contains only minimal diagnostic/demo actors. It does not contain application business logic.

---

## 2. Goals

### 2.1 Functional goals

1. One Spring Boot process represents one Pekko cluster member.
2. Starting the Spring Boot application automatically starts exactly one Pekko `ActorSystem`.
3. A single node can form a usable local cluster for smoke testing.
4. Three local processes can form a cluster using static seed nodes.
5. A three-container Docker Compose deployment can form a cluster using static seed nodes.
6. A Kubernetes deployment can form a cluster using Pekko Discovery + Cluster Bootstrap without static seed nodes.
7. Cluster Sharding can distribute logical entities across cluster nodes.
8. When a node fails, its shards can be relocated and entities can be reactivated on surviving nodes.
9. Cluster Singleton has one active logical instance and can relocate after its host node is lost.
10. DData can replicate test CRDT state between members.
11. Pekko Streams can materialize and execute a smoke-test stream.
12. Persistence APIs are integrated and testable without requiring a production database at application startup.
13. Spring Boot readiness must reflect whether the local Pekko member is usable.
14. Shutdown must use Pekko CoordinatedShutdown and cluster leave/handoff semantics.

### 2.2 Engineering goals

- Strong separation between the Spring application shell and the Pekko distributed runtime.
- Pekko actors remain independent of Spring `ApplicationContext` lookups.
- Typed protocols and immutable messages only for application actors.
- Blocking work never executes on the Pekko default actor dispatcher.
- No external database, Redis, Kafka, Cassandra, ZooKeeper, or other coordination service is required to build and run the baseline.
- Configuration remains native to each runtime: Spring YAML for Spring concerns, HOCON for Pekko concerns.
- Runtime behavior is observable through health, metrics, logs, Pekko Management, and minimal diagnostic APIs.

---

## 3. Non-goals

The first version explicitly does **not** provide:

- Production business actors or domain workflows
- A production persistence journal/snapshot database
- Kafka, Redis, Postgres, Cassandra, ScyllaDB, or external cache integration
- Pekko HTTP as the application's HTTP server
- A generic custom Spring Boot starter for Pekko
- Custom shard allocation strategies
- Custom entity-to-shard hashing
- Reliable/durable delivery guarantees on top of Cluster Sharding
- Global actor mailbox durability
- Full network-partition chaos testing in the default build
- Native-image support
- Automatic tuning of dispatcher, heap, or shard count
- A general-purpose framework that hides the native Pekko APIs

---

## 4. Baseline technology stack

### 4.1 Platform

- Java 21
- Maven multi-module build
- Spring Boot 4.1.1
- Spring MVC
- Spring Boot Actuator
- Micrometer + Prometheus registry

### 4.2 Pekko

- Apache Pekko Core 1.7.1
- Actor Typed
- Cluster Typed
- Cluster Sharding Typed
- Pekko Remote / Artery
- Cluster Tools
- Cluster Metrics where useful
- Distributed Data
- Streams
- Persistence Typed
- Persistence Query
- Serialization Jackson
- Discovery
- Actor TestKit Typed
- Persistence TestKit

### 4.3 Pekko Management

- Apache Pekko Management 1.2.1
- Management core
- Cluster HTTP Management
- Cluster Bootstrap

### 4.4 Dependency version policy

- All Pekko Core modules use the same Pekko Core version.
- Milestone versions are excluded from the baseline.
- Spring Boot dependency management remains the source of truth for Spring-managed dependencies.
- Pekko and Spring serialization stacks are treated as separate boundaries; no shared `ObjectMapper`/`JsonMapper` is part of the architecture contract.
- Dependency convergence is verified in the build.

---

## 5. High-level architecture

```text
┌─────────────────────────────────────────────────────────────┐
│                     Spring Boot JVM                         │
│                                                             │
│  Spring MVC     Actuator/Micrometer     Spring Config/DI    │
│       │                  │                      │            │
│       └──────────────────┴──────────────────────┘            │
│                          │                                  │
│                     PekkoRuntime                            │
│                          │                                  │
│                   ActorSystem<RootCommand>                  │
│                          │                                  │
│       ┌──────────────────┼───────────────────────┐          │
│       │                  │                       │          │
│    Cluster            Sharding              Management     │
│       │                  │                       │          │
│    Artery             DData                  Discovery     │
│       │                  │                       │          │
│    SBR              Entities               Bootstrap(K8s)  │
│       │                                          │          │
│    Singleton        Streams Runtime                         │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

Spring Boot is the process owner. Pekko is a runtime component living in the same process.

One process has exactly:

- one Spring `ApplicationContext`
- one typed `ActorSystem`
- one Pekko Cluster member identity

---

## 6. Module architecture

The Maven build is multi-module.

```text
spring-boot-pekko-cluster/
│
├── pom.xml
├── pekko-runtime/
├── pekko-demo-domain/
├── pekko-app/
└── pekko-test-support/
```

### 6.1 `pekko-runtime`

Reusable runtime integration layer. It owns no demo or business domain.

Responsibilities:

- ActorSystem creation support
- Cluster extension initialization
- Cluster event tracking
- Sharding bootstrap abstractions
- Cluster Singleton bootstrap abstractions
- DData initialization
- Streams runtime access
- Persistence configuration surface
- Serialization markers/configuration
- Pekko Management lifecycle
- Discovery/bootstrap integration
- Runtime lifecycle state
- Spring/Pekko shutdown coordination interfaces
- Health/diagnostic state models used by the application shell

Representative package boundaries:

```text
runtime.actor
runtime.cluster
runtime.sharding
runtime.singleton
runtime.ddata
runtime.streams
runtime.persistence
runtime.serialization
runtime.management
runtime.lifecycle
runtime.interaction
```

### 6.2 `pekko-demo-domain`

Contains only minimal actors/protocols that prove distributed runtime behavior.

- `DemoEntity`
- `DemoEntityDefinition`
- `DemoSingleton`
- `DemoSingletonDefinition`
- immutable typed protocols

It may depend on public extension points exposed by `pekko-runtime`, such as `ShardedEntityDefinition` and `SingletonDefinition`.

### 6.3 `pekko-app`

The only executable Spring Boot application.

Responsibilities:

- `@SpringBootApplication`
- composition root
- Spring configuration
- Spring lifecycle bridge
- Actuator health/readiness
- Micrometer metrics
- diagnostic REST endpoints
- application packaging

Dependencies:

```text
pekko-app
  ├── pekko-runtime
  └── pekko-demo-domain
```

### 6.4 `pekko-test-support`

Reusable test fixtures and distributed test helpers.

Responsibilities:

- typed actor test helpers
- multi-ActorSystem cluster fixture
- wait/eventually helpers
- serialization tests
- cluster/sharding/singleton/DData integration helpers
- Persistence TestKit helpers
- Docker/Kubernetes test orchestration support where useful

Production modules must not depend on `pekko-test-support` at runtime.

### 6.5 Dependency-direction invariant

`pekko-runtime` must never depend on `pekko-demo-domain` or `pekko-app`.

The dependency graph is acyclic:

```text
pekko-runtime
     ▲
     │
pekko-demo-domain
     ▲
     │
  pekko-app
```

`pekko-app` also depends directly on `pekko-runtime` because it is the composition root.

---

## 7. Spring/Pekko ownership boundary

### 7.1 Spring owns

- process lifecycle
- dependency composition
- HTTP/control plane
- Actuator
- Micrometer
- Spring application configuration
- blocking integration infrastructure added later

### 7.2 Pekko owns

- actor lifecycle
- actor scheduling/dispatching
- cluster membership
- remoting
- sharding
- entity lifecycle
- distributed data
- singleton lifecycle
- Pekko streams runtime
- actor supervision
- coordinated shutdown phases

### 7.3 Forbidden coupling

Application actors must not use:

```java
applicationContext.getBean(...)
```

or other runtime service locator patterns.

Spring may construct actor/entity definitions and capture explicit immutable dependencies in factories during bootstrap, after which Pekko owns actor lifecycle.

---

## 8. ActorSystem and root behavior

There is exactly one typed ActorSystem per Spring Boot process.

System name is fixed across all cluster members:

```text
spring-boot-pekko-cluster
```

The root type is a typed protocol rather than `Void`:

```text
RootCommand
```

The root guardian is intentionally minimal. Cluster Sharding entities are not manually spawned under the root guardian; their lifecycle is owned by Cluster Sharding.

The ActorSystem must not be created by controllers, services, or individual domain modules.

---

## 9. Runtime facade

`PekkoRuntime` is the primary application-facing runtime facade.

Conceptual contract:

```text
PekkoRuntime
├── actorSystem
├── cluster
├── sharding
├── singleton
└── runtimeState
```

The facade centralizes extension initialization and lifecycle ownership. It does not attempt to wrap every Pekko API.

Spring components must not repeatedly call static extension lookups throughout the codebase when a runtime-owned reference exists.

---

## 10. Startup lifecycle

### 10.1 Lifecycle orchestration

Startup is explicit and ordered. The project does not scatter initialization across unrelated `@PostConstruct` methods.

A single Spring lifecycle component, `SpringPekkoLifecycleBridge`, coordinates startup and shutdown using Spring lifecycle semantics (for example, `SmartLifecycle`).

### 10.2 Startup phases

```text
NEW
 │
 ▼
ACTOR_SYSTEM_CREATED
 │
 ▼
CORE_INITIALIZED
 │
 ▼
SHARDING_INITIALIZED
 │
 ▼
SINGLETON_INITIALIZED
 │
 ▼
MANAGEMENT_STARTED
 │
 ▼
CLUSTER_JOINING
 │
 ▼
CLUSTER_UP
 │
 ▼
READY
```

`CORE_INITIALIZED` includes required extensions such as Cluster and Distributed Data.

For Kubernetes, Management and discovery/bootstrap infrastructure must be available before Cluster Bootstrap needs to discover/contact peers.

### 10.3 Failure behavior

A mandatory bootstrap failure transitions runtime state to `FAILED` and triggers shutdown rather than leaving an HTTP process alive as a zombie node.

Fatal baseline startup failures include:

- ActorSystem creation failure
- invalid Pekko configuration
- required sharded entity registration failure
- required singleton initialization failure
- required Management startup failure
- cluster join failure beyond configured startup policy

---

## 11. Cluster membership

### 11.1 Cluster provider

The ActorSystem uses Pekko cluster actor provider.

### 11.2 Cluster roles

All baseline nodes use:

```text
role = sharding
```

The design intentionally supports future node specialization such as `api`, `worker`, and `sharding` roles without redesigning entity protocols.

### 11.3 Membership tracking

A `ClusterStateTracker` consumes cluster events and maintains an immutable local snapshot for:

- health
- readiness
- metrics
- diagnostics
- logging

Representative state:

```text
member status
self reachability / runtime status
member count
reachable member count
sharding readiness
runtime phase
```

Health endpoints must read this cached state instead of performing distributed asks on every probe.

---

## 12. Artery remoting

Artery is the cluster transport.

Production ports inside a pod/container:

- Spring HTTP: `8080`
- Pekko Artery: `25520`
- Pekko Management: `8558`

Artery is private cluster traffic and must not be exposed to the public Internet.

Local multi-process development may use distinct Artery ports (`2551`, `2552`, `2553`) because all nodes share one host network namespace.

---

## 13. Cluster joining strategy

Exactly one joining mechanism is active for an environment.

### 13.1 Local development

Static seed nodes.

Typical seeds:

```text
pekko://spring-boot-pekko-cluster@127.0.0.1:2551
pekko://spring-boot-pekko-cluster@127.0.0.1:2552
```

Node 1 can start alone and form the initial development cluster. Additional nodes join through configured seeds.

### 13.2 Docker Compose

Static seed nodes using Docker DNS names, for example:

```text
node1:25520
node2:25520
```

### 13.3 Kubernetes

No static seed nodes.

Use:

```text
Pekko Discovery
    +
Kubernetes API discovery
    +
Pekko Management
    +
Cluster Bootstrap
```

Static seeds and Cluster Bootstrap must not be enabled together.

---

## 14. Split Brain Resolver

Production baseline uses Pekko Split Brain Resolver with:

```text
keep-majority
```

Rationale:

- cluster size may change dynamically
- supports a simple first production topology
- avoids running two independent sharded/singleton worlds after a network partition

Baseline production topology is three members, with odd-sized clusters preferred for HA.

An external lease strategy is outside the first version and can be evaluated later if stronger fencing requirements emerge.

---

## 15. Failure and recovery semantics

### 15.1 Abrupt node failure

```text
node lost
  ↓
failure detector marks unreachable
  ↓
SBR decision
  ↓
member down/removed
  ↓
shards become eligible for relocation
  ↓
entities reactivate on survivors when needed
  ↓
singleton relocates
```

### 15.2 Graceful leave

```text
readiness false
  ↓
cluster leave
  ↓
shard handoff
  ↓
singleton handover/relocation
  ↓
member exiting/removed
  ↓
ActorSystem termination
```

### 15.3 Rejoining

A restarted process is a new member incarnation. The design does not rely on the old process identity being preserved.

---

## 16. Cluster Sharding

### 16.1 API

Application sharding uses Pekko Typed Cluster Sharding only.

Core abstractions:

- `EntityTypeKey<Command>`
- `Entity`
- `EntityRef<Command>`
- `ClusterSharding`

### 16.2 Entity identity

Entity IDs are stable domain/logical identities independent of physical deployment location.

Forbidden entity-ID components include:

- pod name
- hostname
- node ID
- current cluster address

### 16.3 Entity types

Each independent domain actor category has its own `EntityTypeKey`.

The baseline demo contains one `DemoEntity` type.

### 16.4 Number of shards

Baseline explicitly configures:

```text
4096 shards
```

The value is treated as an operational compatibility decision. It must be identical across members and must not be changed casually through rolling deployment.

`4096` is a reference baseline, not an automatically optimal production value. Production sizing must be validated against projected entity counts, node counts, rebalance behavior, and benchmarks before the first durable production rollout.

### 16.5 Entity-to-shard mapping

Use Pekko's default hash-based mapping.

No custom extractor is introduced in the baseline.

### 16.6 Allocation strategy

Use the standard/default Pekko shard allocation strategy.

No custom placement policy is implemented until measurements prove a need.

### 16.7 Coordinator state store

Use Distributed Data (`ddata`) for sharding coordinator state.

Legacy persistence-based coordinator state is not used.

### 16.8 Remember entities

Baseline:

```text
remember-entities = off
```

Entities activate lazily on demand.

This avoids resurrecting a very large historical logical entity population after shard/node restart and keeps automatic/passivation-based lifecycle practical.

### 16.9 Passivation

Passivation is enabled and configurable per workload.

The demo/reference entity uses an idle-oriented passivation policy with a reference timeout of approximately five minutes. Production domains may use different semantics, including explicit lifecycle-driven stop messages.

### 16.10 Sharding roles

Entity definitions declare the `sharding` role as the hosting role. The design supports future proxy-only nodes.

### 16.11 `ShardingGateway`

Spring/application code accesses entities through a small `ShardingGateway` boundary rather than exposing cluster placement logic.

It resolves `EntityRef`s by `EntityTypeKey` and logical entity ID.

The gateway must not maintain a giant cache of all entity refs.

---

## 17. Sharded entity definition model

A domain module contributes a definition equivalent to:

```text
ShardedEntityDefinition<M>
├── typeKey
└── entity factory/definition
```

`ShardingBootstrap` receives the set/list of contributed definitions at application composition time and registers them explicitly.

The architecture must not use reflection-based "start everything" magic.

---

## 18. Demo sharded entity

A minimal `DemoEntity` proves that sharding is actually functional.

Protocol includes only diagnostic commands such as:

- `Ping`
- `GetLocation`
- `Stop`

`GetLocation` returns at least:

- entity ID
- physical Pekko member address hosting the current incarnation

This allows black-box tests to demonstrate initial placement and relocation after node loss.

---

## 19. Actor protocol design

### 19.1 Typed only

Application actors do not use `Behavior<Object>` or `ActorRef<Object>`.

### 19.2 Immutable messages

Prefer Java 21 constructs:

- records
- sealed interfaces
- enums

Messages are immutable.

### 19.3 Public vs internal protocol

Public commands are distinct from internal async-completion/timer/retry messages when practical.

### 19.4 Interaction defaults

- `tell` is the default interaction style.
- `ask` is used only when the caller genuinely requires a response.
- request/response messages carry explicit typed `replyTo` references.
- ask timeout is always explicit at an application boundary.

An ask timeout does not prove that the target operation was not processed; automatic retry of non-idempotent commands is forbidden without an explicit protocol.

---

## 20. Threading model

Three concurrency domains are distinct:

| Domain | Execution model | Intended work |
|---|---|---|
| Spring MVC | Virtual Threads where enabled | HTTP/control plane and imperative blocking boundaries |
| Pekko actors | Pekko dispatcher | short non-blocking actor message handling |
| Blocking integrations | explicit VT/custom executor + resource bulkhead | JDBC/JPA/legacy blocking APIs added later |

### 20.1 Pekko dispatcher rule

Actor handlers on the normal dispatcher must not execute blocking operations such as:

- JDBC calls
- blocking HTTP clients
- `Future.get()` / `CompletionStage.join()`
- `Thread.sleep()`
- waiting on locks/semaphores for long external operations

### 20.2 Async completion

Actor-initiated asynchronous work returns to the actor through mailbox-safe mechanisms such as `pipeToSelf` rather than mutating actor state inside external completion callbacks.

### 20.3 Virtual Thread rule

Virtual Threads do not replace the Pekko actor dispatcher. They are used at blocking application integration boundaries.

Resource concurrency is still bounded by downstream capacity such as DB connection pools or explicit bulkheads.

---

## 21. Mailboxes and backpressure

The baseline keeps the standard actor mailbox behavior and does not globally force bounded mailboxes.

The architecture does not treat actor mailboxes as durable queues.

For high-volume flows where producer rate must be controlled, Pekko Streams or an external durable messaging system should be used instead of allowing unbounded mailbox accumulation.

Dead letters and actor processing latency are observable.

---

## 22. Supervision

Expected domain outcomes are modeled as protocol values, not exceptions.

Unexpected failures use actor supervision policies defined close to each entity's behavior/definition.

No global "restart everything forever" policy is part of the runtime framework.

A restart may reconstruct behavior but does not guarantee restoration of volatile actor state. State that must survive failure requires persistence or reconstruction from an authoritative source.

---

## 23. Serialization

### 23.1 Pekko wire protocol

Baseline application actor messages use Pekko Jackson CBOR serialization through an explicit marker interface such as:

```text
CborSerializable
```

Remote protocol data contains serializable data only; it does not contain Spring infrastructure objects, repositories, executors, sockets, or mutable service objects.

### 23.2 HTTP JSON boundary

Spring MVC HTTP DTO serialization remains independent from Pekko message serialization.

HTTP DTOs and actor commands are allowed to be different types, with explicit mapping at the application boundary.

### 23.3 Mapper isolation

Do not share Spring's JSON mapper instance with Pekko serialization.

### 23.4 Evolution

Actor wire protocols are rolling-upgrade contracts. Prefer additive/backward-compatible schema evolution.

For future very-high-volume or cross-language hot paths, Protobuf may replace CBOR for selected protocols without changing the actor/sharding architecture.

---

## 24. Distributed Data

DData is an active runtime dependency because Cluster Sharding uses it for coordinator state.

The runtime explicitly initializes/accesses the DData extension so configuration problems are detected early and diagnostics are available.

Application use of DData is limited to small/medium replicated coordination state and CRDT-suitable data.

DData is not a replacement for a large cache, operational database, or analytics store.

Application consistency levels are use-case decisions; the runtime does not force majority reads/writes globally.

---

## 25. Cluster Singleton

The runtime initializes a minimal `DemoSingleton` on all eligible nodes using Pekko Cluster Singleton.

Protocol is minimal:

- `Ping`
- `GetLocation`
- optional stop/testing command where appropriate

The singleton is for proving lifecycle and failover semantics, not for processing sharded hot-path traffic.

The runtime must not encourage using singleton as a default global service for partitionable workloads.

The application tolerates a failover gap while singleton ownership moves after member failure.

---

## 26. Pekko Streams

Pekko Streams dependency and runtime are available through the same ActorSystem.

The baseline does not start a useless infinite background stream at application startup.

Streams are verified by tests that materialize a finite graph and demonstrate data processing/backpressure-compatible execution.

Future stream use cases may include:

- high-volume ingress pipelines
- throttling/buffering before actors
- persistence query/projection flows
- WebSocket data pipelines

---

## 27. Persistence

### 27.1 Baseline behavior

Dependencies are included for:

- Persistence Typed
- Persistence Query
- Persistence TestKit (test scope)

The production/demo startup does not create a persistent actor and therefore does not require a production journal database.

### 27.2 Test proof

A test-scope persistent behavior demonstrates:

```text
command → event → test journal → restart/recovery → reconstructed state
```

### 27.3 Future production actors

A persistent sharded entity uses a deterministic `PersistenceId` derived from stable entity identity.

Sharding and persistence remain separate concerns:

- Sharding answers where the logical entity runs.
- Persistence answers how durable state is recovered.

---

## 28. Spring configuration

Spring owns YAML configuration for:

- server
- management/actuator
- Micrometer
- Spring profiles
- application-specific runtime options such as diagnostic enablement and ask timeouts

Spring YAML must not mirror the entire native Pekko configuration tree.

---

## 29. Pekko configuration

Pekko owns HOCON configuration for:

- actor provider
- remoting/Artery
- cluster
- SBR
- sharding
- serialization
- dispatchers
- DData
- persistence plugin namespaces
- Management
- Discovery/Cluster Bootstrap

### 29.1 HOCON layers

```text
application.conf
├── common runtime defaults
│
├── application-local.conf
│   └── localhost ports + static seeds
│
├── application-docker.conf
│   └── Docker DNS + static seeds
│
├── application-test.conf
│   └── dynamic/ephemeral test configuration
│
└── application-kubernetes.conf
    └── no static seeds + discovery/bootstrap
```

Environment launch scripts/configuration must pair the Spring profile with the corresponding Pekko HOCON resource to prevent profile mismatch.

---

## 30. Local development topology

Recommended three-node local ports:

| Node | Spring | Artery | Management |
|---|---:|---:|---:|
| node1 | 8081 | 2551 | 8551 |
| node2 | 8082 | 2552 | 8552 |
| node3 | 8083 | 2553 | 8553 |

All nodes run the same executable JAR.

IntelliJ documentation provides three run configurations/recipes corresponding to these settings.

---

## 31. Spring Boot readiness and liveness

### 31.1 Readiness

Spring may bind the HTTP port before Pekko is ready, but readiness remains `REFUSING_TRAFFIC` until mandatory local runtime conditions are satisfied.

Baseline readiness requires:

- ActorSystem running
- local cluster member status `Up`
- required Sharding entity types registered/initialized
- DData extension initialized
- Singleton proxy/runtime initialized
- Pekko Management started
- runtime not draining/failed

Readiness does not require a production persistence database because no baseline runtime actor depends on one.

A transient unreachable member elsewhere does not automatically kill readiness on a healthy majority-side node. A node transitioning to a losing/down/leaving/removed state becomes not ready.

### 31.2 Liveness

Liveness represents whether the process/runtime can continue, not whether every distributed dependency is perfect.

Fatal examples:

- unexpected ActorSystem termination
- unrecoverable runtime bootstrap failure
- runtime state `FAILED`

Normal cluster convergence, rebalance, or a remote member loss must not cause a restart loop through an overly aggressive liveness probe.

---

## 32. Management planes

### 32.1 Spring Actuator

Runs on the Spring HTTP management/application configuration and provides:

- health
- liveness/readiness
- metrics
- Prometheus endpoint
- application observability

### 32.2 Pekko Management

Runs on the Pekko Management port and provides:

- cluster-management capabilities
- discovery/bootstrap contact point functionality
- Pekko runtime readiness/liveness endpoints where configured

Pekko Management is internal-only in production network policy.

---

## 33. Diagnostic REST API

Diagnostics are intentionally minimal and non-business.

Prefix:

```text
/api/v1/diagnostics
```

Required endpoints:

- `GET /runtime`
- `GET /cluster`
- `GET /sharding/{entityId}`
- `GET /singleton`
- optional `POST /sharding/{entityId}/ping` for explicit demo interaction

### 33.1 Runtime response

Includes safe fields such as:

- actor system name
- local Pekko address
- runtime phase
- cluster status
- sharding readiness
- singleton initialization state
- management state

It must not expose secrets or dump raw configuration.

### 33.2 Cluster response

Includes:

- self member
- members and status
- roles
- unreachable members

### 33.3 Sharding response

Queries `DemoEntity(entityId)` for its current logical ID and physical hosting member address.

### 33.4 Singleton response

Returns the physical member currently hosting the demo singleton.

---

## 34. Metrics

Micrometer exposes low-cardinality runtime metrics under a Pekko namespace, for example:

```text
pekko.runtime.*
pekko.cluster.*
pekko.sharding.*
```

Useful dimensions include:

- member status
- node role
- entity type
- runtime phase

Forbidden metric labels include:

- user ID
- session ID
- room ID
- arbitrary entity ID
- request ID
- actor paths containing high-cardinality logical identities

Cluster/sharding statistics collection must not become a frequent distributed polling hot path.

---

## 35. Logging

Logs include structured contextual information when available:

- actor system
- cluster address
- member status
- node roles
- entity type
- runtime phase

The baseline does not log every actor message.

Cluster membership transitions, sharding registration, singleton placement changes, bootstrap failures, and shutdown phases are logged at appropriate operational levels.

---

## 36. Graceful shutdown

Spring Boot is the process owner but Pekko CoordinatedShutdown owns distributed-runtime teardown ordering.

Required semantic sequence:

```text
SIGTERM / application close
        ↓
Spring readiness = REFUSING_TRAFFIC
        ↓
stop accepting new application work
        ↓
Pekko CoordinatedShutdown
        ↓
Cluster leave
        ↓
Shard handoff
        ↓
Singleton relocation/handover
        ↓
member Exiting/Removed
        ↓
ActorSystem termination
        ↓
Spring context/JVM exit
```

A simple Spring bean `destroyMethod = terminate` is not the primary shutdown orchestration mechanism.

Shutdown has a bounded operational timeout. Kubernetes termination grace period must be sized to permit the normal handoff path.

---

## 37. Docker Compose deployment

### 37.1 Image

One image:

```text
spring-boot-pekko-cluster:<version>
```

### 37.2 Containers

Three nodes:

```text
node1
node2
node3
```

Inside each container:

- Spring `8080`
- Artery `25520`
- Management `8558`

Host maps Spring/Management ports uniquely for diagnostics. Artery does not need host exposure for normal Compose networking.

### 37.3 Membership

Docker profile uses static seeds with Docker service DNS names.

### 37.4 Health

Compose health checks Spring readiness rather than only testing that TCP port 8080 is open.

---

## 38. Kubernetes deployment

### 38.1 Baseline topology

- Deployment with 3 replicas
- all baseline replicas have role `sharding`
- no static seed nodes
- Kubernetes API Discovery + Cluster Bootstrap

### 38.2 Resources

Baseline manifests include:

```text
namespace
service-account
role
role-binding
application service
management/discovery service as required
configmap
deployment
pod-disruption-budget
```

RBAC follows least privilege and only grants discovery reads required by the selected integration.

### 38.3 Probes

- startup probe protects slow initial JVM/bootstrap time
- liveness probe uses Spring Actuator liveness
- readiness probe uses Spring Actuator readiness

### 38.4 Rolling update

Reference deployment:

```text
replicas = 3
maxUnavailable = 1
maxSurge = 1
```

New pods are considered available only after the local Pekko member is `Up` and required sharding runtime is ready.

### 38.5 PodDisruptionBudget

Reference:

```text
minAvailable = 2
```

for a three-member cluster.

### 38.6 Distribution

Use topology spread constraints or preferred anti-affinity so replicas do not all land on the same Kubernetes worker when the infrastructure allows spreading.

### 38.7 Networking

- application HTTP can be exposed according to the consuming service design
- Management is internal only
- Artery is pod-to-pod internal only

---

## 39. Resource/JVM policy

The foundation does not claim universal CPU/memory values.

Production sizing is benchmark-driven.

Container memory limit must leave headroom beyond Java heap for:

- metaspace
- direct buffers
- Artery/network buffers
- thread stacks
- native runtime allocations

`-Xmx` must not simply equal the container memory limit.

CPU throttling, GC pauses, mailbox latency, and remoting latency must be observed during sizing tests.

---

## 40. Test strategy

Testing is layered.

```text
Actor unit tests
       ↓
In-JVM multi-ActorSystem integration
       ↓
Docker multi-process black-box tests
       ↓
Kubernetes smoke/E2E tests
```

### 40.1 Actor unit tests

Use Pekko Typed Actor TestKit for:

- protocol behavior
- replies
- state transitions observable through messages
- timers
- supervision
- stop/passivation behavior where testable

Prefer black-box message-based assertions.

### 40.2 In-JVM cluster tests

Create multiple ActorSystems with dynamic/ephemeral ports and verify:

- cluster membership
- DData replication
- sharding distribution
- sharding relocation
- singleton identity/failover
- remote serialization
- graceful leave/rejoin

Tests use eventually/event-driven assertions rather than fixed long sleeps.

### 40.3 Spring integration tests

A small number of `@SpringBootTest` cases verify:

- context startup
- singleton ActorSystem/runtime beans
- Actuator integration
- diagnostics mapping

Full 3-node distributed behavior is not repeated in every Spring integration test.

### 40.4 Streams test

Materialize and validate a finite stream pipeline.

### 40.5 Persistence test

Use Persistence TestKit to validate event persistence and recovery without requiring a production journal service.

### 40.6 Docker black-box tests

Start three real JVM/container nodes and verify:

1. all nodes become ready
2. cluster reports three `Up` members
3. entities activate across multiple nodes
4. abrupt host loss causes selected entity relocation after member removal
5. singleton relocates after host failure
6. stopped/restarted node can rejoin
7. cluster returns to three healthy members

Both graceful `docker stop` and abrupt `docker kill` scenarios are required because they test different paths.

### 40.7 Kubernetes tests

Dedicated profile/job using kind or k3d verifies:

- no static seed nodes
- discovery finds peers
- Cluster Bootstrap forms a three-member cluster
- all pods become ready
- rolling update reaches the new version with cluster availability preserved according to the deployment policy

Network-partition chaos testing is deferred to a future optional profile.

---

## 41. Maven/CI lifecycle

### 41.1 Default developer path

```text
mvn test
```

runs fast unit/testkit-level tests.

```text
mvn clean verify
```

runs unit tests plus in-JVM integration verification.

### 41.2 Environment profiles

Separate profiles/jobs cover heavier tests, for example:

```text
-Pdocker-e2e
-Pkubernetes-e2e
```

A normal local build does not require Kubernetes.

### 41.3 CI stages

```text
compile
  ↓
unit + ActorTestKit
  ↓
serialization + persistence testkit
  ↓
in-JVM cluster integration
  ↓
package Spring Boot jar
  ↓
build Docker image
  ↓
Docker Compose black-box
  ↓
release/nightly: Kubernetes smoke
```

---

## 42. Acceptance criteria

### 42.1 Build

From a clean checkout:

```text
mvn clean verify
```

succeeds without external database/message broker/cache infrastructure.

### 42.2 Single node

Starting one Spring Boot node results in:

- ActorSystem running
- cluster member `Up`
- DData initialized
- Sharding initialized
- DemoEntity registered
- DemoSingleton initialized
- Management started
- Spring readiness `UP`

### 42.3 Three local nodes

Three processes converge to:

```text
member count = 3
all members = Up
unreachable = 0
```

### 42.4 Sharding

- many logical entities can be activated
- entity hosting spans multiple nodes
- a selected entity hosted on a failed member becomes reachable on a surviving member after cluster removal/relocation

### 42.5 Singleton

- all nodes observe the same active singleton host in steady state
- after host loss, surviving nodes eventually observe the same new singleton host

### 42.6 DData

A CRDT update issued on one member becomes observable from other members according to the test consistency model.

### 42.7 Graceful shutdown

A gracefully stopped node:

- becomes not ready before exit
- leaves cluster
- hands off shards
- permits singleton relocation
- terminates its ActorSystem
- exits without indefinite hang

### 42.8 Rejoin

A restarted process can join as a new member, reach `Up`, participate in sharding, and restore the expected three-node topology.

### 42.9 Docker

A single documented Compose command forms a healthy three-node cluster without manual join or actor-placement steps.

### 42.10 Kubernetes

A three-replica deployment with empty static seed configuration automatically discovers peers and forms a cluster through Pekko Discovery + Cluster Bootstrap.

---

## 43. Operational invariants

The following are architecture-level invariants:

1. Exactly one ActorSystem per Spring Boot process.
2. All members of one cluster use the same ActorSystem name.
3. Static seeds and Cluster Bootstrap are never active together.
4. `number-of-shards` is explicitly configured and identical on all members.
5. `remember-entities` is off in the baseline.
6. Sharding coordinator state uses DData.
7. Normal actor handlers do not block.
8. Actors do not use Spring service lookup.
9. HTTP DTO serialization and Pekko wire serialization are separate boundaries.
10. Actor mailbox is not treated as durable delivery infrastructure.
11. Cluster Singleton is not used for partitionable high-throughput work.
12. Readiness is false before the local member is usable for required Pekko runtime functions.
13. Health probes use cached local runtime state rather than distributed polling.
14. Shutdown is coordinated; direct ActorSystem termination is only a fallback safety mechanism.
15. Diagnostic/metrics labels do not contain high-cardinality entity identities.

---

## 44. Key architectural rationale

The architecture intentionally follows the Actor model's core strengths:

- isolate mutable state inside actor boundaries
- communicate asynchronously with immutable messages
- decouple logical actor identity from physical node/thread location
- multiplex many actors over dispatchers rather than mapping actors one-to-one to threads
- use one programming/runtime model across local concurrency and distributed execution

Spring Boot complements this model rather than replacing it. Spring owns application composition, HTTP/control-plane behavior, operational endpoints, and future blocking integration boundaries. Pekko owns the distributed actor/data-plane runtime.

---

## 45. Future extension points

The design deliberately leaves clean paths for later additions without requiring a foundation rewrite:

- production persistence plugin and persistent sharded entities
- Protobuf serialization for selected hot protocols
- projection/read-model pipeline via Persistence Query + Streams
- specialized cluster roles (`api`, `worker`, `sharding`)
- proxy-only API nodes
- workload-specific passivation policies
- external lease-based split-brain strategy if required
- Netty/WebSocket signaling ingress
- Spring-managed blocking gateways using Virtual Threads and bulkheads
- Kafka/other durable delivery for workflows that require durable messaging
- chaos/network-partition test profile

These are extensions, not requirements of the initial foundation.

---

## 46. Version and source notes

Version baseline verified on 2026-10-19:

- Spring Boot 4.1.1
- Apache Pekko Core 1.7.1
- Apache Pekko Management 1.2.1

The conceptual actor/runtime boundaries are consistent with the principles covered in *Akka in Action*: ActorSystem as the container/nexus for actor runtime capabilities, ActorRef/mailbox isolation, dispatcher-based execution, asynchronous messaging, clustering, sharding, persistence, and distributed-data concepts. The implementation target is Apache Pekko and current Pekko APIs, not the historical Akka API shown in the book.

---

## 47. Design completion state

The architectural design is complete enough to move to implementation planning after user approval of this written spec.

The next step is **not implementation yet**. The next step is to create a task-by-task implementation plan that maps this specification to concrete Maven modules, files, dependencies, tests, and execution order.
